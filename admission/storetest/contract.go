// Package storetest is the reusable admission.Store contract suite (RA-I1 C1–C8).
// Every Store backend runs the same suite through Run with its own Harness, so the
// immutable-registration, operation-ledger, deep-copy and concurrency invariants are
// proven per backend instead of being assumed from MemoryStore.
package storetest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/HeaInSeo/PolicyScheduler/admission"
)

// Harness adapts one Store backend to the contract suite.
type Harness struct {
	// NewStore returns an empty Store. Required.
	NewStore func(t *testing.T) admission.Store

	// Reopen returns a Store over the same durable state as s, as seen by a new
	// process after a restart. It must NOT return s itself or share its in-process
	// state: an identity reopen would prove nothing about durability. A backend with
	// no durable state leaves Reopen nil, and the reopen axis is reported as skipped
	// (NOT IMPLEMENTED) rather than passed.
	Reopen func(t *testing.T, s admission.Store) admission.Store
}

const (
	ownerAuthorization = admission.OwnerAuthorization
	ownerReadiness     = admission.BlockerOwner("readiness")
	ownerQuota         = admission.BlockerOwner("quota")
)

// Run executes the full Store contract against h.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.NewStore == nil {
		t.Fatal("storetest: Harness.NewStore is required")
	}
	t.Run("GetMissing", func(t *testing.T) { testGetMissing(t, h) })
	t.Run("CreateIdempotent", func(t *testing.T) { testCreateIdempotent(t, h) })
	t.Run("CreateConflict", func(t *testing.T) { testCreateConflict(t, h) })
	t.Run("CreateIgnoresMutableState", func(t *testing.T) { testCreateIgnoresMutableState(t, h) })
	t.Run("DeepCopy", func(t *testing.T) { testDeepCopy(t, h) })
	t.Run("ConcurrentCreate", func(t *testing.T) { testConcurrentCreate(t, h) })
	t.Run("OperationLedger", func(t *testing.T) { testOperationLedger(t, h) })
	t.Run("ConcurrentOperationReplay", func(t *testing.T) { testConcurrentOperationReplay(t, h) })
	t.Run("Reopen", func(t *testing.T) { testReopen(t, h) })
}

func candidate(id admission.CandidateID, urgency, priority string, owners ...admission.BlockerOwner) admission.Candidate {
	blockers := make(map[admission.BlockerOwner]admission.Blocker, len(owners))
	for _, o := range owners {
		blockers[o] = admission.Blocker{Owner: o, State: admission.BlockerActive, Reason: "awaiting owner clearance"}
	}
	return admission.Candidate{
		CandidateID:       id,
		Blockers:          blockers,
		ClinicalUrgency:   urgency,
		RequestedPriority: priority,
	}
}

func baseCandidate() admission.Candidate {
	return candidate("c1", "routine", "normal", ownerAuthorization, ownerReadiness)
}

func mustCreate(t *testing.T, s admission.Store, c admission.Candidate) admission.Candidate {
	t.Helper()
	got, err := s.CreateCandidate(context.Background(), c)
	if err != nil {
		t.Fatalf("CreateCandidate(%q): %v", c.CandidateID, err)
	}
	return got
}

func mustGet(t *testing.T, s admission.Store, id admission.CandidateID) admission.Candidate {
	t.Helper()
	got, ok, err := s.GetCandidate(context.Background(), id)
	if err != nil || !ok {
		t.Fatalf("GetCandidate(%q): ok=%v err=%v", id, ok, err)
	}
	return got
}

func clearOwner(owner admission.BlockerOwner) func(*admission.Candidate) error {
	return func(c *admission.Candidate) error {
		c.Blockers[owner] = admission.Blocker{Owner: owner, State: admission.BlockerCleared}
		return nil
	}
}

func testGetMissing(t *testing.T, h Harness) {
	s := h.NewStore(t)
	if _, ok, err := s.GetCandidate(context.Background(), "absent"); ok || err != nil {
		t.Fatalf("GetCandidate(absent): ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	_, err := s.ApplyOperation(context.Background(),
		admission.OperationRecord{OperationID: "op", CandidateID: "absent", Fingerprint: "f"}, clearOwner(ownerReadiness))
	if !errors.Is(err, admission.ErrCandidateNotFound) {
		t.Fatalf("ApplyOperation(absent): err=%v, want ErrCandidateNotFound", err)
	}
}

// C1/C5: identical re-registration (owner order irrelevant) returns the stored candidate.
func testCreateIdempotent(t *testing.T, h Harness) {
	s := h.NewStore(t)
	first := mustCreate(t, s, baseCandidate())
	again := mustCreate(t, s, candidate("c1", "routine", "normal", ownerReadiness, ownerAuthorization))
	if !reflect.DeepEqual(first, again) {
		t.Fatalf("identical re-registration changed the candidate\nfirst=%+v\nagain=%+v", first, again)
	}
	if got := mustGet(t, s, "c1"); !reflect.DeepEqual(got, first) {
		t.Fatalf("stored candidate differs from returned\nstored=%+v\nreturned=%+v", got, first)
	}
}

// C2–C4/C9: any difference in an immutable registration field conflicts, compared as
// exact bytes with empty meaning unset, and the stored candidate is untouched.
func testCreateConflict(t *testing.T, h Harness) {
	cases := []struct {
		name string
		cand admission.Candidate
	}{
		{"urgency-drift", candidate("c1", "urgent", "normal", ownerAuthorization, ownerReadiness)},
		{"priority-drift", candidate("c1", "routine", "high", ownerAuthorization, ownerReadiness)},
		{"both-drift", candidate("c1", "urgent", "high", ownerAuthorization, ownerReadiness)},
		{"urgency-case", candidate("c1", "Routine", "normal", ownerAuthorization, ownerReadiness)},
		{"priority-whitespace", candidate("c1", "routine", " normal", ownerAuthorization, ownerReadiness)},
		{"urgency-empty", candidate("c1", "", "normal", ownerAuthorization, ownerReadiness)},
		{"owner-added", candidate("c1", "routine", "normal", ownerAuthorization, ownerReadiness, ownerQuota)},
		{"owner-removed", candidate("c1", "routine", "normal", ownerAuthorization)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := h.NewStore(t)
			stored := mustCreate(t, s, baseCandidate())
			if _, err := s.CreateCandidate(context.Background(), tc.cand); !errors.Is(err, admission.ErrCandidateConflict) {
				t.Fatalf("CreateCandidate(%s): err=%v, want ErrCandidateConflict", tc.name, err)
			}
			if got := mustGet(t, s, "c1"); !reflect.DeepEqual(got, stored) {
				t.Fatalf("conflict mutated the stored candidate\nwant=%+v\ngot=%+v", stored, got)
			}
		})
	}
	t.Run("empty-is-a-value", func(t *testing.T) {
		s := h.NewStore(t)
		stored := mustCreate(t, s, candidate("c1", "", "", ownerAuthorization))
		mustCreate(t, s, candidate("c1", "", "", ownerAuthorization))
		if _, err := s.CreateCandidate(context.Background(), candidate("c1", "routine", "", ownerAuthorization)); !errors.Is(err, admission.ErrCandidateConflict) {
			t.Fatalf("setting a previously empty urgency: err=%v, want ErrCandidateConflict", err)
		}
		if got := mustGet(t, s, "c1"); !reflect.DeepEqual(got, stored) {
			t.Fatalf("conflict mutated the stored candidate\nwant=%+v\ngot=%+v", stored, got)
		}
	})
}

// C6: after mutable state advanced (blocker cleared), an identical retry still
// reconciles and returns the advanced state instead of conflicting or resetting it.
func testCreateIgnoresMutableState(t *testing.T, h Harness) {
	s := h.NewStore(t)
	mustCreate(t, s, baseCandidate())
	advanced, err := s.ApplyOperation(context.Background(),
		admission.OperationRecord{OperationID: "op-1", CandidateID: "c1", Fingerprint: "clear-readiness"}, clearOwner(ownerReadiness))
	if err != nil {
		t.Fatalf("ApplyOperation: %v", err)
	}
	retry := mustCreate(t, s, baseCandidate())
	if !reflect.DeepEqual(retry, advanced) {
		t.Fatalf("identical retry did not return the advanced candidate\nwant=%+v\ngot=%+v", advanced, retry)
	}
	if got := mustGet(t, s, "c1").Blockers[ownerReadiness].State; got != admission.BlockerCleared {
		t.Fatalf("identical retry reset blocker state to %v", got)
	}
}

// Stored state never aliases caller-owned maps or pointers, in either direction.
func testDeepCopy(t *testing.T, h Harness) {
	s := h.NewStore(t)
	in := baseCandidate()
	in.AuthProjection = &admission.AuthorizationProjection{}
	created := mustCreate(t, s, in)
	want := mustGet(t, s, "c1")

	in.Blockers[ownerReadiness] = admission.Blocker{Owner: ownerReadiness, State: admission.BlockerCleared}
	in.Blockers["injected"] = admission.Blocker{Owner: "injected"}
	in.AuthProjection.Decision.Provenance.Source = "mutated-input"
	created.Blockers[ownerAuthorization] = admission.Blocker{Owner: ownerAuthorization, State: admission.BlockerCleared}
	created.AuthProjection.Decision.Provenance.Source = "mutated-return"
	got := mustGet(t, s, "c1")
	got.Blockers["injected-get"] = admission.Blocker{Owner: "injected-get"}
	got.AuthProjection.Decision.Provenance.Source = "mutated-get"

	if final := mustGet(t, s, "c1"); !reflect.DeepEqual(final, want) {
		t.Fatalf("caller mutation leaked into the store\nwant=%+v\ngot=%+v", want, final)
	}

	applied, err := s.ApplyOperation(context.Background(),
		admission.OperationRecord{OperationID: "op-1", CandidateID: "c1", Fingerprint: "clear-readiness"}, clearOwner(ownerReadiness))
	if err != nil {
		t.Fatalf("ApplyOperation: %v", err)
	}
	wantApplied := mustGet(t, s, "c1")
	applied.Blockers[ownerReadiness] = admission.Blocker{Owner: ownerReadiness, State: admission.BlockerActive}
	if final := mustGet(t, s, "c1"); !reflect.DeepEqual(final, wantApplied) {
		t.Fatalf("ApplyOperation result aliases stored state\nwant=%+v\ngot=%+v", wantApplied, final)
	}
}

// C7: concurrent registrations of one id with two different urgencies. Exactly one
// registration wins; every caller matching the winner succeeds, every other caller
// conflicts, and the stored candidate equals the winner.
func testConcurrentCreate(t *testing.T, h Harness) {
	s := h.NewStore(t)
	const n = 16
	urgencies := []string{"routine", "urgent"}
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = s.CreateCandidate(context.Background(), candidate("c1", urgencies[i%2], "normal", ownerAuthorization))
		}(i)
	}
	close(start)
	wg.Wait()

	winner := mustGet(t, s, "c1").ClinicalUrgency
	for i, err := range errs {
		if urgencies[i%2] == winner {
			if err != nil {
				t.Errorf("caller %d matches winner %q but got %v", i, winner, err)
			}
		} else if !errors.Is(err, admission.ErrCandidateConflict) {
			t.Errorf("caller %d drifts from winner %q but got %v, want ErrCandidateConflict", i, winner, err)
		}
	}
}

// Operation ledger: replay is idempotent, same id + different fingerprint conflicts,
// and a failed mutate records nothing so a retry applies cleanly.
func testOperationLedger(t *testing.T, h Harness) {
	ctx := context.Background()
	s := h.NewStore(t)
	mustCreate(t, s, baseCandidate())
	op := admission.OperationRecord{OperationID: "op-1", CandidateID: "c1", Fingerprint: "clear-readiness"}

	calls := 0
	countingClear := func(c *admission.Candidate) error {
		calls++
		return clearOwner(ownerReadiness)(c)
	}
	first, err := s.ApplyOperation(ctx, op, countingClear)
	if err != nil {
		t.Fatalf("ApplyOperation: %v", err)
	}
	replay, err := s.ApplyOperation(ctx, op, countingClear)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("replay re-ran mutate: calls=%d, want 1", calls)
	}
	if !reflect.DeepEqual(first, replay) {
		t.Fatalf("replay returned a different candidate\nfirst=%+v\nreplay=%+v", first, replay)
	}

	conflicting := op
	conflicting.Fingerprint = "different-semantics"
	if _, err := s.ApplyOperation(ctx, conflicting, clearOwner(ownerAuthorization)); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("same op id, different fingerprint: err=%v, want ErrOperationConflict", err)
	}
	if got := mustGet(t, s, "c1"); !reflect.DeepEqual(got, first) {
		t.Fatalf("operation conflict mutated the candidate\nwant=%+v\ngot=%+v", first, got)
	}

	failing := admission.OperationRecord{OperationID: "op-2", CandidateID: "c1", Fingerprint: "clear-authorization"}
	boom := errors.New("mutate failed")
	if _, err := s.ApplyOperation(ctx, failing, func(*admission.Candidate) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("failing mutate: err=%v, want %v", err, boom)
	}
	if got := mustGet(t, s, "c1"); !reflect.DeepEqual(got, first) {
		t.Fatalf("failed mutate changed the candidate\nwant=%+v\ngot=%+v", first, got)
	}
	retried, err := s.ApplyOperation(ctx, failing, clearOwner(ownerAuthorization))
	if err != nil {
		t.Fatalf("retry after failed mutate: %v", err)
	}
	if retried.Blockers[ownerAuthorization].State != admission.BlockerCleared {
		t.Fatalf("retry after failed mutate was not applied: %+v", retried)
	}
}

// Concurrent replays of one operation all succeed and converge on one committed state.
func testConcurrentOperationReplay(t *testing.T, h Harness) {
	s := h.NewStore(t)
	mustCreate(t, s, baseCandidate())
	op := admission.OperationRecord{OperationID: "op-1", CandidateID: "c1", Fingerprint: "clear-readiness"}
	const n = 16
	results := make([]admission.Candidate, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = s.ApplyOperation(context.Background(), op, clearOwner(ownerReadiness))
		}(i)
	}
	close(start)
	wg.Wait()
	final := mustGet(t, s, "c1")
	for i := range results {
		if errs[i] != nil {
			t.Errorf("replay %d: %v", i, errs[i])
			continue
		}
		if !reflect.DeepEqual(results[i], final) {
			t.Errorf("replay %d diverged\nwant=%+v\ngot=%+v", i, final, results[i])
		}
	}
}

// C8: registration and operation truth survive a reopen, and the idempotency /
// conflict verdicts are the same across it.
func testReopen(t *testing.T, h Harness) {
	if h.Reopen == nil {
		t.Skip("NOT IMPLEMENTED: backend has no durable reopen (RA-I1 C8); not faked with an identity reopen")
	}
	ctx := context.Background()
	s := h.NewStore(t)
	mustCreate(t, s, baseCandidate())
	op := admission.OperationRecord{OperationID: "op-1", CandidateID: "c1", Fingerprint: "clear-readiness"}
	if _, err := s.ApplyOperation(ctx, op, clearOwner(ownerReadiness)); err != nil {
		t.Fatalf("ApplyOperation: %v", err)
	}
	want := mustGet(t, s, "c1")

	r := h.Reopen(t, s)
	if r == s {
		t.Fatal("Reopen returned the same Store instance; that is not a reopen")
	}
	if got := mustGet(t, r, "c1"); !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate lost or changed across reopen\nwant=%+v\ngot=%+v", want, got)
	}
	if got := mustCreate(t, r, baseCandidate()); !reflect.DeepEqual(got, want) {
		t.Fatalf("identical retry after reopen\nwant=%+v\ngot=%+v", want, got)
	}
	if _, err := r.CreateCandidate(ctx, candidate("c1", "urgent", "normal", ownerAuthorization, ownerReadiness)); !errors.Is(err, admission.ErrCandidateConflict) {
		t.Fatalf("drift after reopen: err=%v, want ErrCandidateConflict", err)
	}
	calls := 0
	if _, err := r.ApplyOperation(ctx, op, func(c *admission.Candidate) error {
		calls++
		return clearOwner(ownerReadiness)(c)
	}); err != nil || calls != 0 {
		t.Fatalf("op replay after reopen: err=%v mutateCalls=%d, want nil/0 (ledger must survive reopen)", err, calls)
	}
	conflicting := op
	conflicting.Fingerprint = "different-semantics"
	if _, err := r.ApplyOperation(ctx, conflicting, clearOwner(ownerAuthorization)); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("op conflict after reopen: err=%v, want ErrOperationConflict", err)
	}
}
