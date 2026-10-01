package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Store is the durable persistence seam for the admission domain. RA-I0 makes NO
// service/DB/topology commitment (the packet forbids it): durability is expressed only
// through this abstraction, and MemoryStore is the reference implementation used by
// tests. A real deployment can back it with any durable store without changing the
// domain logic. Every backend must pass the storetest.Run contract suite; MemoryStore
// passes it without the durable-reopen axis, which it cannot provide.
//
// The Store owns two atomicity guarantees the domain relies on:
//   - CreateCandidate is idempotent by candidate identity (the originating pre-Run
//     intent) and fails closed on a conflicting re-registration.
//   - ApplyOperation reconciles a durable operation ledger and commits the operation
//     record together with the candidate mutation atomically, so a decision-operation
//     retry converges (RA-I0 §6) and a crash never leaves the ledger and the candidate
//     disagreeing.
type Store interface {
	// CreateCandidate durably records a new candidate. Idempotent by CandidateID with
	// the same applicable owner set, clinical urgency and requested priority (returns the
	// existing candidate); a re-registration under the same id that differs in any of
	// them fails closed with ErrCandidateConflict and stores nothing.
	CreateCandidate(ctx context.Context, cand Candidate) (Candidate, error)

	// GetCandidate returns a deep copy of a durable candidate, or ok=false if absent.
	GetCandidate(ctx context.Context, id CandidateID) (Candidate, bool, error)

	// ApplyOperation atomically reconciles the operation ledger and, for a new
	// operation, applies mutate to a copy of the candidate and commits both. Reconcile:
	//   - same OperationID + same Fingerprint  → returns the current candidate (idempotent);
	//   - same OperationID + different Fingerprint → ErrOperationConflict (no rewrite);
	//   - new OperationID → mutate + commit.
	// mutate returning an error aborts the operation without recording it.
	ApplyOperation(ctx context.Context, op OperationRecord, mutate func(*Candidate) error) (Candidate, error)
}

// OperationRecord is the durable identity + immutable-semantics fingerprint of a
// decision/blocker operation. The fingerprint excludes non-identity observation
// details (e.g. observation timestamps) so an honest retry of the same decision
// reconciles instead of conflicting.
type OperationRecord struct {
	OperationID string
	CandidateID CandidateID
	Fingerprint string
}

// Store sentinel errors.
var (
	// ErrCandidateNotFound reports an unknown CandidateID.
	ErrCandidateNotFound = errors.New("admission: candidate not found")
	// ErrCandidateConflict reports re-registration of a candidate id with different
	// immutable registration semantics (applicable owner set, clinical urgency or
	// requested priority).
	ErrCandidateConflict = errors.New("admission: candidate id conflicts with a prior registration")
	// ErrOperationConflict reports the same operation id re-used with different immutable
	// semantics — rejected fail-closed, never silently rewritten (RA-I0 §6).
	ErrOperationConflict = errors.New("admission: operation id conflicts with a prior operation")
)

// MemoryStore is the in-memory reference Store. It is safe for concurrent use and
// exposes a BeforeCommit hook so tests can simulate a crash between the decision and
// the durable commit and prove the operation ledger and candidate stay consistent.
type MemoryStore struct {
	mu         sync.Mutex
	candidates map[CandidateID]Candidate
	ops        map[string]OperationRecord

	// BeforeCommit, if set, runs inside ApplyOperation after mutate succeeds but before
	// the candidate and operation record are committed. Returning an error aborts the
	// commit (simulated crash): neither the candidate nor the ledger is updated.
	BeforeCommit func() error
}

// NewMemoryStore constructs an empty in-memory Store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		candidates: make(map[CandidateID]Candidate),
		ops:        make(map[string]OperationRecord),
	}
}

// CreateCandidate implements Store.
func (s *MemoryStore) CreateCandidate(_ context.Context, cand Candidate) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.candidates[cand.CandidateID]; ok {
		if !SameRegistration(existing, cand) {
			return Candidate{}, fmt.Errorf("%w: %q", ErrCandidateConflict, cand.CandidateID)
		}
		return cloneCandidate(existing), nil
	}
	s.candidates[cand.CandidateID] = cloneCandidate(cand)
	return cloneCandidate(cand), nil
}

// GetCandidate implements Store.
func (s *MemoryStore) GetCandidate(_ context.Context, id CandidateID) (Candidate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cand, ok := s.candidates[id]
	if !ok {
		return Candidate{}, false, nil
	}
	return cloneCandidate(cand), true, nil
}

// ApplyOperation implements Store.
func (s *MemoryStore) ApplyOperation(_ context.Context, op OperationRecord, mutate func(*Candidate) error) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cand, ok := s.candidates[op.CandidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w: %q", ErrCandidateNotFound, op.CandidateID)
	}

	if prior, seen := s.ops[op.OperationID]; seen {
		if prior.Fingerprint != op.Fingerprint || prior.CandidateID != op.CandidateID {
			return Candidate{}, fmt.Errorf("%w: %q", ErrOperationConflict, op.OperationID)
		}
		// Idempotent replay: the operation is already durably applied; return the
		// current candidate unchanged.
		return cloneCandidate(cand), nil
	}

	next := cloneCandidate(cand)
	if err := mutate(&next); err != nil {
		return Candidate{}, err
	}
	if s.BeforeCommit != nil {
		if err := s.BeforeCommit(); err != nil {
			// Simulated crash before commit: leave both the candidate and the ledger
			// untouched so a post-restart retry re-applies cleanly.
			return Candidate{}, err
		}
	}
	s.candidates[op.CandidateID] = cloneCandidate(next)
	s.ops[op.OperationID] = op
	return cloneCandidate(next), nil
}

// SameRegistration reports whether a re-registration carries the same immutable
// registration semantics as the stored candidate: applicable owner set, declared
// clinical urgency and requested priority (RA-C3: same identity + different semantics
// conflicts). Every Store backend uses this one definition for its CreateCandidate
// reconcile. Urgency/priority are never silently dropped or rewritten here; changing
// them is a separate append-only decision operation, not a re-registration (RA-C7).
// Comparison is exact byte equality: no case folding, trimming, enum mapping or other
// normalization. The empty string means "unset" and is itself a registered value, so
// empty vs non-empty conflicts like any other difference.
func SameRegistration(existing, cand Candidate) bool {
	return existing.ClinicalUrgency == cand.ClinicalUrgency &&
		existing.RequestedPriority == cand.RequestedPriority &&
		sameOwnerSet(existing.Blockers, cand.Blockers)
}

// sameOwnerSet reports whether two blocker maps cover the same owner set (identity of a
// candidate's applicable owners), ignoring per-blocker state.
func sameOwnerSet(a, b map[BlockerOwner]Blocker) bool {
	if len(a) != len(b) {
		return false
	}
	for owner := range a {
		if _, ok := b[owner]; !ok {
			return false
		}
	}
	return true
}

// cloneCandidate deep-copies a candidate so stored state never shares backing maps or
// pointers with callers (immutability of durable truth against caller-side mutation).
func cloneCandidate(c Candidate) Candidate {
	if c.Blockers != nil {
		blockers := make(map[BlockerOwner]Blocker, len(c.Blockers))
		for owner, b := range c.Blockers {
			blockers[owner] = b
		}
		c.Blockers = blockers
	}
	if c.AuthProjection != nil {
		proj := *c.AuthProjection
		c.AuthProjection = &proj
	}
	return c
}
