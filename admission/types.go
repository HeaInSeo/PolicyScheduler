// Package admission implements the RA-I0 Run Admission core spine: the smallest
// embeddable admission-domain authority that tracks a pre-Run candidate, its
// independent owner-scoped blockers (including a recoverable Authorization blocker
// PROJECTION), idempotent decision operations, and eligibility — without owning
// Authorization/entitlement truth and without any JUMI/Kueue/Kubernetes side effects.
//
// Scope boundary (RA-I0): this package does NOT implement Authorization/IAM, does not
// own the Run-preparation/Pipeline materialization Authorization recheck, performs no
// JUMI SubmitRun / Kueue / Kubernetes mutation, and selects no fairness/aging/quota
// algorithm. Identities and enums are internal; no public API/wire schema, ID/hash
// algorithm, or DB/service topology is committed.
package admission

import "time"

// CandidateID is the durable admission candidate identity. It IS the originating
// pre-Run intent reference (RA-I0 §1): Run Admission never mints a second
// queue/candidate identity distinct from the pre-Run intent.
type CandidateID string

// BlockerOwner identifies the independent owner of an admission blocker (RA-I0 §2).
// Blockers are owner-scoped: an operation for one owner can only affect that owner's
// blocker, never another owner's.
type BlockerOwner string

const (
	// OwnerAuthorization owns the Authorization/entitlement blocker. Run Admission
	// holds only a recoverable PROJECTION of this blocker; it never owns, mints, or
	// mutates entitlement truth (RA-I0 §4).
	OwnerAuthorization BlockerOwner = "authorization"
)

// AuthorizationOutcome is the entitlement decision outcome (RA-I0 §3). It is distinct
// from decision availability and is meaningful only when the decision is AVAILABLE.
type AuthorizationOutcome int

const (
	// OutcomeUnspecified is the zero value; never a valid available outcome.
	OutcomeUnspecified AuthorizationOutcome = iota
	OutcomeAllow
	OutcomeDeny
	OutcomeConstrained
)

func (o AuthorizationOutcome) String() string {
	switch o {
	case OutcomeAllow:
		return "ALLOW"
	case OutcomeDeny:
		return "DENY"
	case OutcomeConstrained:
		return "CONSTRAINED"
	default:
		return "UNSPECIFIED"
	}
}

// DecisionAvailability is whether an Authorization decision is available at all
// (RA-I0 §3). UNKNOWN / UNAVAILABLE is a decision-availability state, NEVER a DENY
// verdict, and must never be stored or reused as DENY.
type DecisionAvailability int

const (
	// AvailabilityUnspecified is the zero value; treated as not-available (fail-closed).
	AvailabilityUnspecified DecisionAvailability = iota
	AvailabilityAvailable
	AvailabilityUnknown
	AvailabilityUnavailable
)

func (a DecisionAvailability) String() string {
	switch a {
	case AvailabilityAvailable:
		return "AVAILABLE"
	case AvailabilityUnknown:
		return "UNKNOWN"
	case AvailabilityUnavailable:
		return "UNAVAILABLE"
	default:
		return "UNSPECIFIED"
	}
}

// AuthorizationProvenance carries enough source/basis to reconcile (re-verify) the
// current Authorization basis after a crash/restart (RA-I0 §4). Run Admission stores
// this as part of a projection; it is not a second entitlement truth.
type AuthorizationProvenance struct {
	// Source identifies the Authorization authority that produced the decision.
	Source string
	// BasisRef references the entitlement basis (e.g. a grant/decision identifier)
	// sufficient to re-verify the current decision against the Authorization owner.
	BasisRef string
	// ObservedAt is when Run Admission observed this decision.
	ObservedAt time.Time
}

// AuthorizationDecision is a decision availability plus, when available, an outcome,
// with the provenance needed to reconcile. Outcome is meaningful only when
// Availability == AvailabilityAvailable.
type AuthorizationDecision struct {
	Availability DecisionAvailability
	Outcome      AuthorizationOutcome
	Provenance   AuthorizationProvenance
}

// clearsAuthorizationBlocker reports whether this decision may clear the
// Authorization-owned blocker (RA-I0 §5): only an AVAILABLE ALLOW clears; DENY,
// CONSTRAINED, and UNKNOWN/UNAVAILABLE all keep the blocker active (fail-closed).
//
// Run Admission does NOT independently adjudicate currency/revocation. It applies the
// decision most recently reported to it (last report wins); enforcing recency ordering
// across separate reports is the Authorization owner's responsibility, not RA's. The
// projection retains provenance (source/basis) so the current basis can be reconciled
// after a restart. ObservedAt is descriptive provenance only and is deliberately not
// used to order decisions here.
func (d AuthorizationDecision) clearsAuthorizationBlocker() bool {
	return d.Availability == AvailabilityAvailable && d.Outcome == OutcomeAllow
}

// BlockerState is the state of an owner-scoped blocker.
type BlockerState int

const (
	// BlockerActive means the blocker is holding admission (fail-closed default).
	BlockerActive BlockerState = iota
	// BlockerCleared means the owner has cleared its blocker.
	BlockerCleared
)

func (s BlockerState) String() string {
	if s == BlockerCleared {
		return "CLEARED"
	}
	return "ACTIVE"
}

// Blocker is one owner-scoped admission blocker.
type Blocker struct {
	Owner  BlockerOwner
	State  BlockerState
	Reason string
}

// AuthorizationProjection is Run Admission's crash/restart-recoverable projection of
// the Authorization blocker (RA-I0 §4): the last observed decision plus provenance to
// reconcile the current basis. It is NOT the entitlement truth.
type AuthorizationProjection struct {
	Decision AuthorizationDecision
}

// Candidate is the admission-domain aggregate keyed by the originating pre-Run intent
// (CandidateID). It holds independent owner-scoped blockers, the Authorization blocker
// projection, and the three separate priority axes (RA-I0 §8): clinical urgency,
// requested priority, and the effective admission decision — none combined by any
// scheduling/fairness algorithm in I0.
type Candidate struct {
	CandidateID       CandidateID
	Blockers          map[BlockerOwner]Blocker
	AuthProjection    *AuthorizationProjection
	ClinicalUrgency   string
	RequestedPriority string
}

// EffectiveAdmission is the effective admission decision axis (RA-I0 §8), derived from
// blocker state and deliberately independent of clinical urgency and requested
// priority.
type EffectiveAdmission string

const (
	// AdmissionBlocked: at least one applicable owner-scoped blocker is active.
	AdmissionBlocked EffectiveAdmission = "BLOCKED"
	// AdmissionEligible: all applicable owner-scoped blockers are cleared.
	AdmissionEligible EffectiveAdmission = "ELIGIBLE"
)
