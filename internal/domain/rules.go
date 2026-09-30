package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// FreezeLeaseGrants builds the frozen allocation summary for freshly created
// leases. Every grant is captured in its initial claimed state, and the result
// is stably sorted by lease id so the same allocation always serializes to the
// same frozen content.
func FreezeLeaseGrants(leases []NodeLease) []LeaseGrant {
	grants := make([]LeaseGrant, 0, len(leases))
	for _, lease := range leases {
		grants = append(grants, LeaseGrant{
			LeaseID:   lease.ID,
			NodeID:    lease.NodeID,
			State:     LeaseClaimed,
			ExpiresAt: lease.ExpiresAt.UTC(),
		})
	}
	sort.Slice(grants, func(i, j int) bool {
		return grants[i].LeaseID < grants[j].LeaseID
	})
	return grants
}

// ExplainLeaseGrants deterministically reconstructs the frozen summary for an
// allocation group created before summaries were persisted. Only provable
// lease facts are used: the referenced leases must still exist and belong to
// the group, and the captured state is always the initial claimed state. The
// current lease or node lifecycle state is intentionally never consulted, so
// the explanation stays stable after releases, reports, and expirations.
func ExplainLeaseGrants(groupID string, leaseIDs []string, leases map[string]NodeLease) ([]LeaseGrant, error) {
	ordered := make([]string, len(leaseIDs))
	copy(ordered, leaseIDs)
	sort.Strings(ordered)
	grants := make([]LeaseGrant, 0, len(ordered))
	for _, leaseID := range ordered {
		lease, ok := leases[leaseID]
		if !ok {
			return nil, NewError(CodeStorageFailure, "lease group references a missing lease", map[string]any{
				"lease_group_id": groupID,
				"lease_id":       leaseID,
			})
		}
		if lease.AllocationID != groupID {
			return nil, NewError(CodeStorageFailure, "lease is linked to a different lease group", map[string]any{
				"lease_group_id": groupID,
				"lease_id":       leaseID,
				"allocation_id":  lease.AllocationID,
			})
		}
		grants = append(grants, LeaseGrant{
			LeaseID:   lease.ID,
			NodeID:    lease.NodeID,
			State:     LeaseClaimed,
			ExpiresAt: lease.ExpiresAt.UTC(),
		})
	}
	return grants, nil
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
