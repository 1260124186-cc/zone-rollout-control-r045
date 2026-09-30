package workflowprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"reflect"
	"sort"
	"time"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/assessment"
	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/httpapi"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

// runLeaseSummaryFreeze verifies that an allocation group returns a frozen
// summary of its outcome, stable across idempotent replays, later lease state
// changes, and process restarts. Groups persisted before summaries existed are
// reconstructed deterministically from their lease facts and then frozen as
// well.
func runLeaseSummaryFreeze() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	active := harness
	defer func() { active.close() }()

	if _, err := enrollNode(harness, "resolver-freeze-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-freeze-02", "east"); err != nil {
		return err
	}
	planAggregate, _, err := createActivePlan(harness, "freeze.example", 2002, 2, 0.5, 100)
	if err != nil {
		return err
	}

	created, err := allocate(harness, planAggregate.ID, "freeze-request-001", "east", 2)
	if err != nil {
		return err
	}
	if err := expectFrozenCreation(created); err != nil {
		return err
	}

	replay, err := allocate(harness, planAggregate.ID, "freeze-request-001", "east", 2)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(replay, created) {
		return ensure(false, "same-key replay must return the identical frozen group")
	}

	// A later lease release must not rewrite the frozen summary.
	sortedIDs := append([]string(nil), created.LeaseIDs...)
	sort.Strings(sortedIDs)
	var releasedFirst domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+sortedIDs[0]+"/release", nil, &releasedFirst); err != nil {
		return err
	}
	afterRelease, err := allocate(harness, planAggregate.ID, "freeze-request-001", "east", 2)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(afterRelease.Summary, created.Summary) {
		return ensure(false, "lease release must not rewrite the frozen summary")
	}
	if afterRelease.Summary.Items[0].State != domain.LeaseClaimed {
		return ensure(false, "frozen summary must still record the original claimed state")
	}

	// A restart reading the snapshot must hand back the same frozen content.
	harness, err = harness.restart()
	if err != nil {
		return err
	}
	active = harness
	afterRestart, err := allocate(harness, planAggregate.ID, "freeze-request-001", "east", 2)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(afterRestart.Summary, created.Summary) {
		return ensure(false, "restart read must return the same frozen summary")
	}

	// Simulate a snapshot produced before summaries existed: strip the summary,
	// restart from disk, and let the first replay derive it from lease facts.
	if err := stripLeaseSummaries(harness.statePath()); err != nil {
		return err
	}
	harness, err = harness.restart()
	if err != nil {
		return err
	}
	active = harness
	derived, err := allocate(harness, planAggregate.ID, "freeze-request-001", "east", 2)
	if err != nil {
		return err
	}
	if derived.Summary == nil || derived.Summary.Source != domain.LeaseSummaryDerived {
		return ensure(false, "legacy group summary must be derived from lease facts")
	}
	if err := expectDerivedItems(derived, sortedIDs); err != nil {
		return err
	}

	// Once derived and frozen, further lease changes must not recompute it.
	var releasedSecond domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+sortedIDs[1]+"/release", nil, &releasedSecond); err != nil {
		return err
	}
	afterSecondRelease, err := allocate(harness, planAggregate.ID, "freeze-request-001", "east", 2)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(afterSecondRelease.Summary, derived.Summary) {
		return ensure(false, "derived summary must freeze and ignore later lease changes")
	}
	if afterSecondRelease.Summary.Items[1].State != domain.LeaseClaimed {
		return ensure(false, "derived summary must keep the state frozen at reconstruction")
	}
	return nil
}

func expectFrozenCreation(group domain.LeaseGroup) error {
	if group.Summary == nil {
		return ensure(false, "allocation group must carry a frozen summary")
	}
	if group.Summary.Source != domain.LeaseSummaryCreated {
		return ensure(false, "summary created in the allocation transaction must use the created source")
	}
	if len(group.Summary.Items) != len(group.LeaseIDs) {
		return ensure(false, "summary must cover every lease of the group")
	}
	expectedIDs := append([]string(nil), group.LeaseIDs...)
	sort.Strings(expectedIDs)
	expiresAt := group.Summary.Items[0].ExpiresAt
	if expiresAt.IsZero() || !expiresAt.Equal(group.Summary.Items[1].ExpiresAt) {
		return ensure(false, "summary must expose the shared lease deadline")
	}
	if !expiresAt.After(group.CreatedAt) {
		return ensure(false, "summary deadline must follow the creation time")
	}
	for index, item := range group.Summary.Items {
		if item.LeaseID != expectedIDs[index] {
			return ensure(false, "summary items must be stably sorted by lease id")
		}
		if item.NodeID == "" {
			return ensure(false, "summary item must expose the assigned node")
		}
		if item.State != domain.LeaseClaimed {
			return ensure(false, "created summary must freeze the initial claimed state")
		}
	}
	return nil
}

func expectDerivedItems(group domain.LeaseGroup, sortedIDs []string) error {
	if len(group.Summary.Items) != 2 {
		return ensure(false, "derived summary must cover both leases")
	}
	expectedStates := []domain.LeaseState{domain.LeaseReleased, domain.LeaseClaimed}
	for index, item := range group.Summary.Items {
		if item.LeaseID != sortedIDs[index] {
			return ensure(false, "derived summary must be sorted by lease id")
		}
		if item.NodeID == "" || item.ExpiresAt.IsZero() {
			return ensure(false, "derived summary must keep node and deadline facts")
		}
		if item.State != expectedStates[index] {
			return ensure(false, fmt.Sprintf(
				"derived summary must freeze the provable state at reconstruction, item %d got %q",
				index, item.State,
			))
		}
	}
	return nil
}

// stripLeaseSummaries rewrites the on-disk snapshot as it would have looked
// before allocation summaries existed.
func stripLeaseSummaries(path string) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read snapshot for legacy rewrite: %w", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return fmt.Errorf("decode snapshot for legacy rewrite: %w", err)
	}
	groups, ok := snapshot["lease_groups"].(map[string]any)
	if !ok {
		return ensure(false, "snapshot must contain lease groups")
	}
	for _, raw := range groups {
		entry, ok := raw.(map[string]any)
		if !ok {
			return ensure(false, "lease group snapshot entry has an unexpected shape")
		}
		delete(entry, "summary")
	}
	encoded, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode legacy snapshot: %w", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return fmt.Errorf("write legacy snapshot: %w", err)
	}
	return nil
}

func (h *harness) statePath() string {
	return h.tempDir + "/state.json"
}

// restart shuts the running server down and reopens the same snapshot,
// simulating a process restart without changing the working directory.
func (h *harness) restart() (*harness, error) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.server.Shutdown(shutdownCtx); err != nil {
		return nil, fmt.Errorf("shutdown server before restart: %w", err)
	}
	_ = h.listener.Close()

	stateStore, err := store.Open(h.statePath())
	if err != nil {
		return nil, fmt.Errorf("reopen store on restart: %w", err)
	}
	clock := platform.NewManualClock(h.clock.Now())
	ids := platform.CryptoIDGenerator{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	plans := plan.New(stateStore, clock, ids)
	nodes := fleet.New(stateStore, clock, ids)
	allocations := allocation.New(stateStore, clock, ids)
	probesService := probe.New(stateStore, clock, ids, allocations)
	assessments := assessment.New(stateStore, clock, ids)
	api := httpapi.New(plans, nodes, allocations, probesService, assessments, ids, logger)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen on restart: %w", err)
	}
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = server.Serve(listener)
	}()
	return &harness{
		baseURL:  "http://" + listener.Addr().String(),
		clock:    clock,
		client:   &http.Client{Timeout: 10 * time.Second},
		server:   server,
		listener: listener,
		tempDir:  h.tempDir,
	}, nil
}
