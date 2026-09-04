package admission

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Test scope note: every test here runs purely in-memory against MemoryStore with no
// JUMI, Kueue, or Kubernetes dependency (RA-I0 §9 / mandatory test 10). The whole
// package imports only the standard library.

const (
	ownerReadiness BlockerOwner = "readiness"
	ownerQuota     BlockerOwner = "quota"
)

func newService() *Service { return New(NewMemoryStore()) }

func mustRegister(t *testing.T, s *Service, req RegisterRequest) Candidate {
	t.Helper()
	cand, err := s.RegisterCandidate(context.Background(), req)
	if err != nil {
		t.Fatalf("RegisterCandidate(%q): unexpected error: %v", req.CandidateID, err)
	}
	return cand
}

func allowDecision() AuthorizationDecision {
	return AuthorizationDecision{
		Availability: AvailabilityAvailable,
		Outcome:      OutcomeAllow,
		Provenance:   AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123", ObservedAt: time.Unix(1_700_000_000, 0)},
	}
}

func mustEligible(t *testing.T, s *Service, id CandidateID) bool {
	t.Helper()
	ok, err := s.IsEligible(context.Background(), id)
	if err != nil {
		t.Fatalf("IsEligible(%q): unexpected error: %v", id, err)
	}
	return ok
}

// Mandatory test 1: idempotent decision-operation retry converges (RA-I0 §6).
func TestIdempotentDecisionRetryConverges(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})

	op := AuthorizationDecisionOp{OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision()}
	first, err := s.ApplyAuthorizationDecision(ctx, op)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	// Retry the SAME operation, even with a different (honest) observation timestamp:
	// the immutable semantics are identical, so it must reconcile, not conflict.
	retryOp := op
	retryOp.Decision.Provenance.ObservedAt = time.Unix(1_700_009_999, 0)
	second, err := s.ApplyAuthorizationDecision(ctx, retryOp)
	if err != nil {
		t.Fatalf("idempotent retry returned error: %v", err)
	}
	if first.Blockers[OwnerAuthorization].State != BlockerCleared ||
		second.Blockers[OwnerAuthorization].State != BlockerCleared {
		t.Fatalf("retry did not converge to a cleared authorization blocker: %+v / %+v",
			first.Blockers[OwnerAuthorization], second.Blockers[OwnerAuthorization])
	}
}

// Mandatory test 2: same operation id + changed immutable semantics conflicts /
// fails closed and never silently rewrites (RA-I0 §6).
func TestChangedSemanticsConflict(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})

	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-1", CandidateID: "c1", Decision: allowDecision(),
	}); err != nil {
		t.Fatalf("apply allow: %v", err)
	}
	// Reuse op-1 with a DENY (changed immutable semantics).
	deny := AuthorizationDecision{
		Availability: AvailabilityAvailable, Outcome: OutcomeDeny,
		Provenance: AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123"},
	}
	_, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-1", CandidateID: "c1", Decision: deny,
	})
	if !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("expected ErrOperationConflict, got %v", err)
	}
	// The prior ALLOW must remain untouched (no silent rewrite).
	cand, _, _ := s.GetCandidate(ctx, "c1")
	if cand.Blockers[OwnerAuthorization].State != BlockerCleared {
		t.Fatalf("conflicting op rewrote state: %+v", cand.Blockers[OwnerAuthorization])
	}
}

// Mandatory test 3: an available DENY keeps the Authorization blocker active (RA-I0 §5).
func TestDenyKeepsBlockerActive(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})

	cand, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-deny", CandidateID: "c1",
		Decision: AuthorizationDecision{
			Availability: AvailabilityAvailable, Outcome: OutcomeDeny,
			Provenance: AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123"},
		},
	})
	if err != nil {
		t.Fatalf("apply deny: %v", err)
	}
	if cand.Blockers[OwnerAuthorization].State != BlockerActive {
		t.Fatalf("DENY did not keep blocker active: %+v", cand.Blockers[OwnerAuthorization])
	}
	if mustEligible(t, s, "c1") {
		t.Fatal("candidate eligible under DENY")
	}
}

// Mandatory test 4: required UNKNOWN / UNAVAILABLE keeps the Authorization blocker
// active as an availability state, and is NEVER stored or reported as a DENY verdict
// (RA-I0 §3).
func TestUnknownIsNotDeny(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		av   DecisionAvailability
	}{
		{"unknown", AvailabilityUnknown},
		{"unavailable", AvailabilityUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newService()
			mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
			cand, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
				OperationID: "op-x", CandidateID: "c1",
				Decision: AuthorizationDecision{
					Availability: tc.av, Outcome: OutcomeUnspecified,
					Provenance: AuthorizationProvenance{Source: "authz-svc"},
				},
			})
			if err != nil {
				t.Fatalf("apply %s: %v", tc.name, err)
			}
			b := cand.Blockers[OwnerAuthorization]
			if b.State != BlockerActive {
				t.Fatalf("%s did not keep blocker active: %+v", tc.name, b)
			}
			// The projection must record the availability state, not a DENY outcome.
			if cand.AuthProjection == nil {
				t.Fatal("no authorization projection recorded")
			}
			if cand.AuthProjection.Decision.Outcome == OutcomeDeny {
				t.Fatal("UNKNOWN/UNAVAILABLE was stored as a DENY outcome")
			}
			if cand.AuthProjection.Decision.Availability != tc.av {
				t.Fatalf("projection lost availability: %v", cand.AuthProjection.Decision.Availability)
			}
		})
	}
}

// TestUnavailableDecisionWithOutcomeRejected proves a DENY cannot be smuggled in under
// an UNKNOWN/UNAVAILABLE availability (defends RA-I0 §3 at the validation boundary).
func TestUnavailableDecisionWithOutcomeRejected(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	_, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-x", CandidateID: "c1",
		Decision: AuthorizationDecision{
			Availability: AvailabilityUnknown, Outcome: OutcomeDeny,
			Provenance: AuthorizationProvenance{Source: "authz-svc"},
		},
	})
	if !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("expected ErrInvalidDecision for outcome under UNKNOWN, got %v", err)
	}
}

// Mandatory test 5: a semantically-current ALLOW clears ONLY the Authorization blocker;
// unrelated owner blockers are untouched (RA-I0 §5).
func TestAllowClearsOnlyAuthorizationBlocker(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization, ownerReadiness}})

	cand, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision(),
	})
	if err != nil {
		t.Fatalf("apply allow: %v", err)
	}
	if cand.Blockers[OwnerAuthorization].State != BlockerCleared {
		t.Fatal("ALLOW did not clear authorization blocker")
	}
	if cand.Blockers[ownerReadiness].State != BlockerActive {
		t.Fatal("ALLOW wrongly cleared an unrelated owner's blocker")
	}
}

// Mandatory test 6: an unrelated active blocker keeps eligibility false even when the
// Authorization blocker is cleared (RA-I0 §7).
func TestUnrelatedBlockerKeepsIneligible(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization, ownerReadiness}})

	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision(),
	}); err != nil {
		t.Fatalf("apply allow: %v", err)
	}
	if mustEligible(t, s, "c1") {
		t.Fatal("eligible while readiness blocker still active (authorization clearance alone must not admit)")
	}
	// Positive control: clearing the remaining owner makes it eligible.
	if _, err := s.ApplyOwnerBlocker(ctx, OwnerBlockerOp{
		OperationID: "op-ready", CandidateID: "c1", Owner: ownerReadiness, State: BlockerCleared,
	}); err != nil {
		t.Fatalf("clear readiness: %v", err)
	}
	if !mustEligible(t, s, "c1") {
		t.Fatal("not eligible after all owner blockers cleared")
	}
}

// Mandatory test 7: after a restart, the Authorization blocker projection and its
// provenance are recovered and produce the same eligibility (RA-I0 §4).
func TestRestartRecoversProjectionAndEligibility(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	s1 := New(store)
	mustRegister(t, s1, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	if _, err := s1.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision(),
	}); err != nil {
		t.Fatalf("apply allow: %v", err)
	}

	// "Restart": a brand-new Service instance over the SAME durable store.
	s2 := New(store)
	cand, ok, err := s2.GetCandidate(ctx, "c1")
	if err != nil || !ok {
		t.Fatalf("recover candidate: ok=%v err=%v", ok, err)
	}
	if cand.AuthProjection == nil {
		t.Fatal("authorization projection not recovered after restart")
	}
	p := cand.AuthProjection.Decision.Provenance
	if p.Source != "authz-svc" || p.BasisRef != "grant-123" {
		t.Fatalf("provenance not recovered: %+v", p)
	}
	if !mustEligible(t, s2, "c1") {
		t.Fatal("eligibility differs after restart")
	}
}

// Mandatory test 8: cross-owner clearing is forbidden — an owner op only affects its
// own blocker, and the Authorization blocker cannot be cleared through the generic
// owner-blocker path (RA-I0 §2, §4).
func TestCrossOwnerClearForbidden(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization, ownerReadiness, ownerQuota}})

	// Clearing readiness must not affect quota (owner isolation).
	if _, err := s.ApplyOwnerBlocker(ctx, OwnerBlockerOp{
		OperationID: "op-ready", CandidateID: "c1", Owner: ownerReadiness, State: BlockerCleared,
	}); err != nil {
		t.Fatalf("clear readiness: %v", err)
	}
	cand, _, _ := s.GetCandidate(ctx, "c1")
	if cand.Blockers[ownerReadiness].State != BlockerCleared {
		t.Fatal("readiness not cleared")
	}
	if cand.Blockers[ownerQuota].State != BlockerActive {
		t.Fatal("clearing readiness leaked into quota (cross-owner clear)")
	}
	// The Authorization blocker cannot be cleared via ApplyOwnerBlocker at all.
	_, err := s.ApplyOwnerBlocker(ctx, OwnerBlockerOp{
		OperationID: "op-auth", CandidateID: "c1", Owner: OwnerAuthorization, State: BlockerCleared,
	})
	if !errors.Is(err, ErrInvalidBlockerOp) {
		t.Fatalf("expected ErrInvalidBlockerOp clearing authorization via owner path, got %v", err)
	}
	cand, _, _ = s.GetCandidate(ctx, "c1")
	if cand.Blockers[OwnerAuthorization].State != BlockerActive {
		t.Fatal("authorization blocker was cleared through the generic owner path")
	}
}

// Mandatory test 9: clinical urgency, requested priority, and the effective admission
// decision remain independent concepts (RA-I0 §8).
func TestPriorityAxesIndependent(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{
		CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization},
		ClinicalUrgency: "critical", RequestedPriority: "high",
	})

	eff, err := s.EffectiveAdmissionFor(ctx, "c1")
	if err != nil {
		t.Fatalf("effective admission: %v", err)
	}
	if eff != AdmissionBlocked {
		t.Fatalf("expected BLOCKED before clearance, got %v", eff)
	}
	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision(),
	}); err != nil {
		t.Fatalf("apply allow: %v", err)
	}
	cand, _, _ := s.GetCandidate(ctx, "c1")
	eff = cand.effectiveAdmission()
	// The effective admission axis changed, but the other two axes are untouched.
	if eff != AdmissionEligible {
		t.Fatalf("expected ELIGIBLE after clearance, got %v", eff)
	}
	if cand.ClinicalUrgency != "critical" {
		t.Fatalf("clinical urgency mutated by admission change: %q", cand.ClinicalUrgency)
	}
	if cand.RequestedPriority != "high" {
		t.Fatalf("requested priority mutated by admission change: %q", cand.RequestedPriority)
	}
}

// Mandatory test 10 (explicit end-to-end): a full admission lifecycle runs purely
// in-memory with no external system (RA-I0 §9).
func TestFullLifecycleNoExternalDeps(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "run-intent-1", Owners: []BlockerOwner{OwnerAuthorization, ownerReadiness}})
	if mustEligible(t, s, "run-intent-1") {
		t.Fatal("fresh candidate must not be eligible (fail-closed)")
	}
	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "run-intent-1", Decision: allowDecision(),
	}); err != nil {
		t.Fatalf("allow: %v", err)
	}
	if _, err := s.ApplyOwnerBlocker(ctx, OwnerBlockerOp{
		OperationID: "op-ready", CandidateID: "run-intent-1", Owner: ownerReadiness, State: BlockerCleared,
	}); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if !mustEligible(t, s, "run-intent-1") {
		t.Fatal("candidate not eligible after all owners cleared")
	}
}

// TestAllowThenDenyReactivates proves revocation-by-superseding-decision: a later DENY
// re-activates the Authorization blocker cleared by an earlier ALLOW (RA-I0 §5).
func TestAllowThenDenyReactivates(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision(),
	}); err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !mustEligible(t, s, "c1") {
		t.Fatal("not eligible after allow")
	}
	cand, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-deny", CandidateID: "c1",
		Decision: AuthorizationDecision{
			Availability: AvailabilityAvailable, Outcome: OutcomeDeny,
			Provenance: AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123"},
		},
	})
	if err != nil {
		t.Fatalf("deny: %v", err)
	}
	if cand.Blockers[OwnerAuthorization].State != BlockerActive {
		t.Fatal("later DENY did not re-activate the authorization blocker")
	}
	if mustEligible(t, s, "c1") {
		t.Fatal("still eligible after revoking ALLOW with DENY")
	}
}

// TestConstrainedDoesNotClear proves CONSTRAINED is not a clean clearance (RA-I0 §5:
// only ALLOW clears).
func TestConstrainedDoesNotClear(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	cand, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-con", CandidateID: "c1",
		Decision: AuthorizationDecision{
			Availability: AvailabilityAvailable, Outcome: OutcomeConstrained,
			Provenance: AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123"},
		},
	})
	if err != nil {
		t.Fatalf("constrained: %v", err)
	}
	if cand.Blockers[OwnerAuthorization].State != BlockerActive {
		t.Fatal("CONSTRAINED wrongly cleared the authorization blocker")
	}
}

// TestAuthorizationOpRequiresApplicableOwner proves an Authorization decision cannot be
// applied to a candidate that has no Authorization blocker (fail-closed).
func TestAuthorizationOpRequiresApplicableOwner(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{ownerReadiness}})
	_, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision(),
	})
	if !errors.Is(err, ErrOwnerNotApplicable) {
		t.Fatalf("expected ErrOwnerNotApplicable, got %v", err)
	}
}

// TestCrashBeforeCommitLeavesConsistentState proves the operation ledger and the
// candidate commit atomically: a simulated crash before commit records neither, so a
// post-restart retry re-applies cleanly (RA-I0 §6 durability).
func TestCrashBeforeCommitLeavesConsistentState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	s := New(store)
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})

	boom := errors.New("simulated crash")
	store.BeforeCommit = func() error { return boom }
	op := AuthorizationDecisionOp{OperationID: "op-allow", CandidateID: "c1", Decision: allowDecision()}
	if _, err := s.ApplyAuthorizationDecision(ctx, op); !errors.Is(err, boom) {
		t.Fatalf("expected simulated crash error, got %v", err)
	}
	// Nothing committed: blocker still active, no projection.
	cand, _, _ := s.GetCandidate(ctx, "c1")
	if cand.Blockers[OwnerAuthorization].State != BlockerActive || cand.AuthProjection != nil {
		t.Fatalf("crash left partial state: %+v proj=%v", cand.Blockers[OwnerAuthorization], cand.AuthProjection)
	}
	// Restart recovery: the same operation id retries cleanly (it was never recorded).
	store.BeforeCommit = nil
	if _, err := s.ApplyAuthorizationDecision(ctx, op); err != nil {
		t.Fatalf("retry after crash: %v", err)
	}
	if !mustEligible(t, s, "c1") {
		t.Fatal("not eligible after successful retry")
	}
}

// TestReturnedCandidateIsDeepCopied proves callers cannot mutate durable state through
// a returned candidate's maps/pointers.
func TestReturnedCandidateIsDeepCopied(t *testing.T) {
	ctx := context.Background()
	s := newService()
	cand := mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	// Mutate the returned copy aggressively.
	cand.Blockers[OwnerAuthorization] = Blocker{Owner: OwnerAuthorization, State: BlockerCleared}
	cand.Blockers["injected"] = Blocker{Owner: "injected", State: BlockerCleared}

	stored, _, _ := s.GetCandidate(ctx, "c1")
	if stored.Blockers[OwnerAuthorization].State != BlockerActive {
		t.Fatal("caller mutated durable blocker state via returned map")
	}
	if _, ok := stored.Blockers["injected"]; ok {
		t.Fatal("caller injected a blocker into durable state via returned map")
	}
}

// TestRegistrationIdempotencyAndConflict proves re-registration under the same id is
// idempotent for the same owner set and fails closed for a changed owner set.
func TestRegistrationIdempotencyAndConflict(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization, ownerReadiness}})

	// Same id + same owner set → idempotent.
	if _, err := s.RegisterCandidate(ctx, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{ownerReadiness, OwnerAuthorization}}); err != nil {
		t.Fatalf("idempotent re-register: %v", err)
	}
	// Same id + different owner set → conflict.
	_, err := s.RegisterCandidate(ctx, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	if !errors.Is(err, ErrCandidateConflict) {
		t.Fatalf("expected ErrCandidateConflict, got %v", err)
	}
}

// TestInvalidRequestsRejected covers structural validation fail-closed paths.
func TestInvalidRequestsRejected(t *testing.T) {
	ctx := context.Background()
	s := newService()
	if _, err := s.RegisterCandidate(ctx, RegisterRequest{CandidateID: "", Owners: []BlockerOwner{OwnerAuthorization}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty candidate id: got %v", err)
	}
	if _, err := s.RegisterCandidate(ctx, RegisterRequest{CandidateID: "c1"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("no owners: got %v", err)
	}
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})
	// Available decision missing a concrete outcome.
	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op", CandidateID: "c1",
		Decision: AuthorizationDecision{Availability: AvailabilityAvailable, Provenance: AuthorizationProvenance{Source: "s", BasisRef: "b"}},
	}); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("available without outcome: got %v", err)
	}
	// Available decision missing basis reference.
	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op2", CandidateID: "c1",
		Decision: AuthorizationDecision{Availability: AvailabilityAvailable, Outcome: OutcomeAllow, Provenance: AuthorizationProvenance{Source: "s"}},
	}); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("available without basis: got %v", err)
	}
}

// TestUndefinedBlockerStateRejected proves an out-of-range BlockerState supplied by an
// embedding caller (e.g. from decoded input) is rejected fail-closed and never
// persisted outside the declared ACTIVE/CLEARED domain.
func TestUndefinedBlockerStateRejected(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization, ownerReadiness}})
	_, err := s.ApplyOwnerBlocker(ctx, OwnerBlockerOp{
		OperationID: "op-bad", CandidateID: "c1", Owner: ownerReadiness, State: BlockerState(99),
	})
	if !errors.Is(err, ErrInvalidBlockerOp) {
		t.Fatalf("undefined blocker state: expected ErrInvalidBlockerOp, got %v", err)
	}
	// The bad operation must not have been recorded, and the blocker is unchanged.
	cand, _, _ := s.GetCandidate(ctx, "c1")
	if got := cand.Blockers[ownerReadiness].State; got != BlockerActive {
		t.Fatalf("undefined state leaked into durable blocker: %v", got)
	}
}

// TestWhitespaceCandidateIDRejected proves the candidate identity (which IS the pre-Run
// intent reference, RA-I0 §1) is trim-validated, not just checked for emptiness — a
// whitespace-only id must fail closed rather than durably create a blank-identity
// candidate.
func TestWhitespaceCandidateIDRejected(t *testing.T) {
	ctx := context.Background()
	s := newService()
	if _, err := s.RegisterCandidate(ctx, RegisterRequest{CandidateID: "   ", Owners: []BlockerOwner{OwnerAuthorization}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("whitespace candidate id: expected ErrInvalidRequest, got %v", err)
	}
}

// TestZeroBlockerCandidateNotEligible proves fail-closed defense-in-depth at the Store
// boundary: a candidate with no applicable owner blockers (injectable directly via the
// exported Store) is never vacuously eligible.
func TestZeroBlockerCandidateNotEligible(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if _, err := store.CreateCandidate(ctx, Candidate{CandidateID: "c1", Blockers: map[BlockerOwner]Blocker{}}); err != nil {
		t.Fatalf("create zero-blocker candidate: %v", err)
	}
	s := New(store)
	if ok, _ := s.IsEligible(ctx, "c1"); ok {
		t.Fatal("zero-blocker candidate reported eligible (must fail closed)")
	}
	eff, _ := s.EffectiveAdmissionFor(ctx, "c1")
	if eff != AdmissionBlocked {
		t.Fatalf("zero-blocker candidate effective admission = %v, want BLOCKED", eff)
	}
}

// TestLastReportedDecisionWins locks the documented projection semantics (RA-I0 §4/§5):
// RA applies the most recently reported decision under a new operation id and does not
// itself adjudicate recency by ObservedAt. A DENY followed by a later-reported ALLOW
// (carrying an OLDER ObservedAt, new op id) clears the blocker — recency ordering is the
// Authorization owner's responsibility, not RA's.
func TestLastReportedDecisionWins(t *testing.T) {
	ctx := context.Background()
	s := newService()
	mustRegister(t, s, RegisterRequest{CandidateID: "c1", Owners: []BlockerOwner{OwnerAuthorization}})

	if _, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-deny", CandidateID: "c1",
		Decision: AuthorizationDecision{
			Availability: AvailabilityAvailable, Outcome: OutcomeDeny,
			Provenance: AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123", ObservedAt: time.Unix(2_000_000_000, 0)},
		},
	}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	// A newly reported ALLOW (distinct op id) with an OLDER ObservedAt still wins.
	cand, err := s.ApplyAuthorizationDecision(ctx, AuthorizationDecisionOp{
		OperationID: "op-allow", CandidateID: "c1",
		Decision: AuthorizationDecision{
			Availability: AvailabilityAvailable, Outcome: OutcomeAllow,
			Provenance: AuthorizationProvenance{Source: "authz-svc", BasisRef: "grant-123", ObservedAt: time.Unix(1_000_000_000, 0)},
		},
	})
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if cand.Blockers[OwnerAuthorization].State != BlockerCleared {
		t.Fatal("last-reported ALLOW did not clear the blocker")
	}
}
