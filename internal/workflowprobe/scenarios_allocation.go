package workflowprobe

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeLeaseAllocation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-east-02", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "lease.example", 1001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	first, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(first.LeaseIDs) != 2 {
		return ensure(false, "first lease group must contain two leases")
	}
	if err := expectFrozenGrants(first); err != nil {
		return err
	}
	expectedExpiry := harness.clock.Now().Add(600 * time.Second)
	for _, grant := range first.LeaseGrants {
		if grant.State != domain.LeaseClaimed {
			return ensure(false, "frozen grant must record the initial claimed state")
		}
		if !grant.ExpiresAt.Equal(expectedExpiry) {
			return ensure(false, "frozen grant must record the allocation deadline")
		}
		if grant.NodeID == "" {
			return ensure(false, "frozen grant must record the node id")
		}
	}

	// Same-key replay must return byte-identical frozen content.
	replay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "lease request replay returned another group")
	}
	if err := expectSameGrants(first, replay); err != nil {
		return err
	}

	// Restart reads from the persisted snapshot and must return the same
	// frozen content without rebuilding it.
	if err := harness.restart(); err != nil {
		return err
	}
	afterRestart, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if err := expectSameGrants(first, afterRestart); err != nil {
		return err
	}

	// A fresh allocation is impossible while the two nodes stay leased.
	_, err = allocate(harness, plan.ID, "lease-request-0002", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}

	// Subsequent lease/node lifecycle changes must not rewrite the summary.
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	afterRelease, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if err := expectSameGrants(first, afterRelease); err != nil {
		return err
	}

	// Expiration also must leave the frozen summary untouched. Advancing the
	// clock past the deadline and issuing another request triggers expiry
	// cleanup at the start of the allocation transaction; the released and
	// expired nodes become eligible for a fresh allocation.
	harness.clock.Advance(601 * time.Second)
	if _, err := allocate(harness, plan.ID, "lease-request-0003", "east", 2); err != nil {
		return err
	}
	afterExpiry, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if err := expectSameGrants(first, afterExpiry); err != nil {
		return err
	}

	// Legacy groups persisted without a summary must be deterministically
	// explained from provable lease facts on replay, survive restart with the
	// same frozen content, and never track later lifecycle state.
	if err := harness.restart(); err != nil {
		return err
	}
	if err := stripLeaseGrants(harness.snapshotPath()); err != nil {
		return err
	}
	legacy, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if legacy.ID != first.ID {
		return ensure(false, "legacy replay returned another group")
	}
	if err := expectFrozenGrants(legacy); err != nil {
		return err
	}
	for _, grant := range legacy.LeaseGrants {
		if grant.State != domain.LeaseClaimed {
			return ensure(false, "legacy explanation must record the initial claimed state")
		}
		if grant.ExpiresAt.Equal(time.Time{}) {
			return ensure(false, "legacy explanation must record the original deadline")
		}
	}
	if err := harness.restart(); err != nil {
		return err
	}
	legacyReplay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if err := expectSameGrants(legacy, legacyReplay); err != nil {
		return err
	}
	return nil
}

func expectFrozenGrants(group domain.LeaseGroup) error {
	if len(group.LeaseGrants) != len(group.LeaseIDs) {
		return ensure(false, "lease group summary must cover every lease id")
	}
	ids := append([]string(nil), group.LeaseIDs...)
	sort.Strings(ids)
	for index, grant := range group.LeaseGrants {
		if grant.LeaseID != ids[index] {
			return ensure(false, "lease group summary must be stably sorted by lease id")
		}
		if grant.NodeID == "" {
			return ensure(false, "lease group summary must record the node id")
		}
	}
	return nil
}

func expectSameGrants(want, got domain.LeaseGroup) error {
	if len(want.LeaseGrants) != len(got.LeaseGrants) {
		return ensure(false, "frozen grant count changed on replay/restart")
	}
	for index := range want.LeaseGrants {
		expected := want.LeaseGrants[index]
		actual := got.LeaseGrants[index]
		if expected != actual {
			return ensure(false, "frozen grant content changed after replay or lifecycle change")
		}
	}
	return nil
}

// stripLeaseGrants rewrites a snapshot as older software would have stored it:
// lease groups carry only lease ids, with no frozen summary.
func stripLeaseGrants(path string) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return err
	}
	rawGroups, ok := snapshot["lease_groups"]
	if !ok {
		return nil
	}
	var groups map[string]map[string]json.RawMessage
	if err := json.Unmarshal(rawGroups, &groups); err != nil {
		return err
	}
	for _, group := range groups {
		delete(group, "lease_grants")
	}
	strippedGroups, err := json.Marshal(groups)
	if err != nil {
		return err
	}
	snapshot["lease_groups"] = strippedGroups
	encoded, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o600)
}
