package admission

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Service is the embeddable Run Admission core facade. It performs pure request
// validation and delegates durable, atomic state transitions to a Store. It owns the
// admission-domain truth (candidate, owner-scoped blockers, eligibility) but never the
// Authorization/entitlement truth — for Authorization it holds only a recoverable
// projection driven by decisions reported to it (RA-I0 §4).
type Service struct {
	store Store
}

// New constructs a Service over the given durable Store.
func New(store Store) *Service {
	return &Service{store: store}
}

// Additional sentinel errors for request validation.
var (
	// ErrInvalidRequest reports a structurally invalid registration request.
	ErrInvalidRequest = errors.New("admission: invalid registration request")
	// ErrInvalidDecision reports a structurally invalid or self-contradictory
	// Authorization decision (e.g. an unavailable decision carrying an outcome verdict).
	ErrInvalidDecision = errors.New("admission: invalid authorization decision")
	// ErrInvalidBlockerOp reports a structurally invalid owner-blocker operation.
	ErrInvalidBlockerOp = errors.New("admission: invalid blocker operation")
	// ErrOwnerNotApplicable reports an operation targeting an owner that is not one of
	// the candidate's applicable blocker owners.
	ErrOwnerNotApplicable = errors.New("admission: owner is not applicable to this candidate")
)

// RegisterRequest registers an admission candidate keyed by the originating pre-Run
// intent (RA-I0 §1). Owners are the applicable owner-scoped blockers, each created
// ACTIVE (fail-closed) until its owner clears it. Clinical urgency and requested
// priority are independent descriptive axes (RA-I0 §8).
type RegisterRequest struct {
	CandidateID       CandidateID
	Owners            []BlockerOwner
	ClinicalUrgency   string
	RequestedPriority string
}

// RegisterCandidate validates and durably records a candidate. It is idempotent by
// CandidateID (same id + same owner set reconciles to the existing candidate); a
// re-registration under the same id with a different owner set fails closed.
func (s *Service) RegisterCandidate(ctx context.Context, req RegisterRequest) (Candidate, error) {
	if strings.TrimSpace(string(req.CandidateID)) == "" {
		return Candidate{}, fmt.Errorf("%w: empty candidate id", ErrInvalidRequest)
	}
	if len(req.Owners) == 0 {
		return Candidate{}, fmt.Errorf("%w: at least one blocker owner is required", ErrInvalidRequest)
	}
	blockers := make(map[BlockerOwner]Blocker, len(req.Owners))
	for _, owner := range req.Owners {
		if strings.TrimSpace(string(owner)) == "" {
			return Candidate{}, fmt.Errorf("%w: empty blocker owner", ErrInvalidRequest)
		}
		if _, dup := blockers[owner]; dup {
			return Candidate{}, fmt.Errorf("%w: duplicate blocker owner %q", ErrInvalidRequest, owner)
		}
		blockers[owner] = Blocker{Owner: owner, State: BlockerActive, Reason: "awaiting owner clearance"}
	}
	cand := Candidate{
		CandidateID:       req.CandidateID,
		Blockers:          blockers,
		ClinicalUrgency:   req.ClinicalUrgency,
		RequestedPriority: req.RequestedPriority,
	}
	return s.store.CreateCandidate(ctx, cand)
}

// AuthorizationDecisionOp projects an Authorization owner's decision onto the
// candidate's Authorization blocker. The operation is idempotent by OperationID; its
// immutable semantics are (candidate, availability, outcome, provenance source+basis)
// — the observation timestamp is deliberately excluded so an honest retry converges.
type AuthorizationDecisionOp struct {
	OperationID string
	CandidateID CandidateID
	Decision    AuthorizationDecision
}

// ApplyAuthorizationDecision records the reported Authorization decision as the
// candidate's Authorization blocker projection and derives the blocker state
// (RA-I0 §3–§5): a semantically-current available ALLOW clears the Authorization
// blocker; DENY, CONSTRAINED, and required UNKNOWN/UNAVAILABLE keep it active
// (fail-closed). UNKNOWN/UNAVAILABLE is never stored or treated as DENY. Only the
// Authorization-owned blocker is ever touched; unrelated owners' blockers are untouched.
func (s *Service) ApplyAuthorizationDecision(ctx context.Context, op AuthorizationDecisionOp) (Candidate, error) {
	if err := validateAuthorizationOp(op); err != nil {
		return Candidate{}, err
	}
	rec := OperationRecord{
		OperationID: op.OperationID,
		CandidateID: op.CandidateID,
		Fingerprint: authorizationFingerprint(op),
	}
	return s.store.ApplyOperation(ctx, rec, func(c *Candidate) error {
		if _, ok := c.Blockers[OwnerAuthorization]; !ok {
			return fmt.Errorf("%w: %q", ErrOwnerNotApplicable, OwnerAuthorization)
		}
		// Project the observed decision (RA does not own the truth; it records what the
		// Authorization owner reported, with provenance, for restart reconciliation).
		c.AuthProjection = &AuthorizationProjection{Decision: op.Decision}
		if op.Decision.clearsAuthorizationBlocker() {
			c.Blockers[OwnerAuthorization] = Blocker{Owner: OwnerAuthorization, State: BlockerCleared}
			return nil
		}
		c.Blockers[OwnerAuthorization] = Blocker{
			Owner:  OwnerAuthorization,
			State:  BlockerActive,
			Reason: authorizationBlockReason(op.Decision),
		}
		return nil
	})
}

// OwnerBlockerOp sets or clears a NON-Authorization owner's blocker. It is owner-scoped
// by construction: it only ever touches op.Owner's blocker, so one owner can never
// clear another's (RA-I0 §2). The Authorization blocker is excluded here — it is driven
// solely by ApplyAuthorizationDecision so admission operations cannot invent or clear
// entitlement state (RA-I0 §4).
type OwnerBlockerOp struct {
	OperationID string
	CandidateID CandidateID
	Owner       BlockerOwner
	State       BlockerState
	Reason      string
}

// ApplyOwnerBlocker applies an owner-scoped blocker state change, idempotent by
// OperationID with fail-closed conflict on changed semantics.
func (s *Service) ApplyOwnerBlocker(ctx context.Context, op OwnerBlockerOp) (Candidate, error) {
	if strings.TrimSpace(op.OperationID) == "" || strings.TrimSpace(string(op.CandidateID)) == "" {
		return Candidate{}, fmt.Errorf("%w: empty operation/candidate id", ErrInvalidBlockerOp)
	}
	if strings.TrimSpace(string(op.Owner)) == "" {
		return Candidate{}, fmt.Errorf("%w: empty owner", ErrInvalidBlockerOp)
	}
	if op.Owner == OwnerAuthorization {
		return Candidate{}, fmt.Errorf("%w: the authorization blocker is driven only by ApplyAuthorizationDecision", ErrInvalidBlockerOp)
	}
	rec := OperationRecord{
		OperationID: op.OperationID,
		CandidateID: op.CandidateID,
		Fingerprint: ownerBlockerFingerprint(op),
	}
	return s.store.ApplyOperation(ctx, rec, func(c *Candidate) error {
		if _, ok := c.Blockers[op.Owner]; !ok {
			return fmt.Errorf("%w: %q", ErrOwnerNotApplicable, op.Owner)
		}
		c.Blockers[op.Owner] = Blocker{Owner: op.Owner, State: op.State, Reason: op.Reason}
		return nil
	})
}

// GetCandidate returns a durable candidate by id.
func (s *Service) GetCandidate(ctx context.Context, id CandidateID) (Candidate, bool, error) {
	return s.store.GetCandidate(ctx, id)
}

// IsEligible reports whether all applicable owner-scoped blockers are cleared
// (RA-I0 §7). Authorization clearance alone is never sufficient — every owner's blocker
// must be clear.
func (s *Service) IsEligible(ctx context.Context, id CandidateID) (bool, error) {
	cand, ok, err := s.store.GetCandidate(ctx, id)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("%w: %q", ErrCandidateNotFound, id)
	}
	return cand.allBlockersCleared(), nil
}

// EffectiveAdmissionFor returns the effective admission decision axis for a candidate,
// derived purely from blocker state and independent of clinical urgency and requested
// priority (RA-I0 §8).
func (s *Service) EffectiveAdmissionFor(ctx context.Context, id CandidateID) (EffectiveAdmission, error) {
	cand, ok, err := s.store.GetCandidate(ctx, id)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrCandidateNotFound, id)
	}
	return cand.effectiveAdmission(), nil
}

// allBlockersCleared reports whether every applicable owner-scoped blocker is cleared.
// A candidate with no applicable owner blockers is treated as NOT cleared (fail-closed):
// eligibility must be an explicit consequence of every owner clearing, never a vacuous
// default. RegisterCandidate enforces ≥1 owner; this guard also fails closed for a
// candidate injected directly at the Store boundary.
func (c Candidate) allBlockersCleared() bool {
	if len(c.Blockers) == 0 {
		return false
	}
	for _, b := range c.Blockers {
		if b.State != BlockerCleared {
			return false
		}
	}
	return true
}

// effectiveAdmission derives the effective admission decision from blocker state only.
func (c Candidate) effectiveAdmission() EffectiveAdmission {
	if c.allBlockersCleared() {
		return AdmissionEligible
	}
	return AdmissionBlocked
}

// validateAuthorizationOp enforces structural validity of an Authorization decision
// operation, including the outcome/availability separation (RA-I0 §3): an unavailable
// decision (UNKNOWN/UNAVAILABLE) must not carry an outcome verdict, so a DENY can never
// be smuggled in under UNKNOWN/UNAVAILABLE.
func validateAuthorizationOp(op AuthorizationDecisionOp) error {
	if strings.TrimSpace(op.OperationID) == "" || strings.TrimSpace(string(op.CandidateID)) == "" {
		return fmt.Errorf("%w: empty operation/candidate id", ErrInvalidDecision)
	}
	d := op.Decision
	switch d.Availability {
	case AvailabilityAvailable:
		switch d.Outcome {
		case OutcomeAllow, OutcomeDeny, OutcomeConstrained:
		default:
			return fmt.Errorf("%w: available decision requires a concrete outcome", ErrInvalidDecision)
		}
		if strings.TrimSpace(d.Provenance.BasisRef) == "" {
			return fmt.Errorf("%w: available decision requires a basis reference for reconciliation", ErrInvalidDecision)
		}
	case AvailabilityUnknown, AvailabilityUnavailable:
		if d.Outcome != OutcomeUnspecified {
			return fmt.Errorf("%w: unavailable decision must not carry an outcome verdict", ErrInvalidDecision)
		}
	default:
		return fmt.Errorf("%w: decision availability is unspecified", ErrInvalidDecision)
	}
	if strings.TrimSpace(d.Provenance.Source) == "" {
		return fmt.Errorf("%w: decision requires a provenance source", ErrInvalidDecision)
	}
	return nil
}

// authorizationBlockReason explains why a non-clearing decision keeps the Authorization
// blocker active, preserving the availability-vs-outcome distinction (never "DENY" for
// an UNKNOWN/UNAVAILABLE decision).
func authorizationBlockReason(d AuthorizationDecision) string {
	if d.Availability != AvailabilityAvailable {
		return fmt.Sprintf("authorization decision %s (no verdict)", d.Availability)
	}
	return fmt.Sprintf("authorization outcome %s", d.Outcome)
}

// authorizationFingerprint canonicalizes an Authorization op's immutable semantics.
// ObservedAt is intentionally excluded so an honest retry of the same decision
// reconciles instead of conflicting.
func authorizationFingerprint(op AuthorizationDecisionOp) string {
	payload := struct {
		Candidate    CandidateID `json:"candidate"`
		Owner        string      `json:"owner"`
		Availability int         `json:"availability"`
		Outcome      int         `json:"outcome"`
		Source       string      `json:"source"`
		BasisRef     string      `json:"basis_ref"`
	}{
		Candidate:    op.CandidateID,
		Owner:        string(OwnerAuthorization),
		Availability: int(op.Decision.Availability),
		Outcome:      int(op.Decision.Outcome),
		Source:       op.Decision.Provenance.Source,
		BasisRef:     op.Decision.Provenance.BasisRef,
	}
	return hashPayload(payload)
}

// ownerBlockerFingerprint canonicalizes a non-Authorization owner-blocker op's
// immutable semantics: (candidate, owner, state). Reason is descriptive prose, not
// identity, and is deliberately excluded — consistent with the authorization path,
// where the block reason is derived and excluded — so an honest retry of the same
// (owner, state) with a reworded reason reconciles rather than conflicting.
func ownerBlockerFingerprint(op OwnerBlockerOp) string {
	payload := struct {
		Candidate CandidateID `json:"candidate"`
		Owner     string      `json:"owner"`
		State     int         `json:"state"`
	}{
		Candidate: op.CandidateID,
		Owner:     string(op.Owner),
		State:     int(op.State),
	}
	return hashPayload(payload)
}

func hashPayload(payload any) string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		// The fingerprint payloads are fixed all-scalar structs that cannot fail to
		// marshal; a failure here is a programming error, not a runtime condition, and
		// must never silently collapse distinct operations to one fingerprint.
		panic(fmt.Sprintf("admission: fingerprint payload must be marshalable: %v", err))
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
