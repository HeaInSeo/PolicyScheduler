package sqlitestore_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/HeaInSeo/PolicyScheduler/admission"
	"github.com/HeaInSeo/PolicyScheduler/admission/sqlitestore"
	"github.com/HeaInSeo/PolicyScheduler/admission/storetest"
)

func dbPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "admission.db")
}

func mustOpen(t *testing.T, path string) *sqlitestore.Store {
	t.Helper()
	s, err := sqlitestore.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// reopen closes s and opens a NEW Store over the same database file, as a restarted
// writer process would.
func reopen(t *testing.T, s *sqlitestore.Store, path string) *sqlitestore.Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return mustOpen(t, path)
}

// The J1 SQLite store passes the full contract, including the C8 reopen axis with a
// genuine close + reopen of the database file.
func TestSQLiteStoreContract(t *testing.T) {
	var mu sync.Mutex
	paths := map[admission.Store]string{}
	storetest.Run(t, storetest.Harness{
		NewStore: func(t *testing.T) admission.Store {
			path := dbPath(t)
			s := mustOpen(t, path)
			mu.Lock()
			paths[s] = path
			mu.Unlock()
			return s
		},
		Reopen: func(t *testing.T, s admission.Store) admission.Store {
			mu.Lock()
			path := paths[s]
			mu.Unlock()
			return reopen(t, s.(*sqlitestore.Store), path)
		},
	})
}

// A second writer on the same file is fenced while the first is open, and can take
// over once the first closes.
func TestSingleWriterFence(t *testing.T) {
	path := dbPath(t)
	first := mustOpen(t, path)
	if second, err := sqlitestore.Open(context.Background(), path); !errors.Is(err, sqlitestore.ErrWriterFenced) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second Open while first is live: err=%v, want ErrWriterFenced", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mustOpen(t, path)
}

// The authorization projection (including provenance) and the nil-vs-empty blocker
// distinction survive a reopen. ObservedAt keeps its instant to the nanosecond and is
// read back in UTC.
func TestProjectionAndBlockersSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := dbPath(t)
	s := mustOpen(t, path)
	observed := time.Date(2026, 10, 1, 17, 4, 5, 123456789, time.FixedZone("KST", 9*3600))
	withProjection := admission.Candidate{
		CandidateID: "c-proj",
		Blockers: map[admission.BlockerOwner]admission.Blocker{
			admission.OwnerAuthorization: {Owner: admission.OwnerAuthorization, State: admission.BlockerActive, Reason: "awaiting decision"},
		},
		AuthProjection: &admission.AuthorizationProjection{Decision: admission.AuthorizationDecision{
			Availability: admission.AvailabilityAvailable,
			Outcome:      admission.OutcomeConstrained,
			Provenance:   admission.AuthorizationProvenance{Source: "iam", BasisRef: "grant-7", ObservedAt: observed},
		}},
		ClinicalUrgency:   "urgent",
		RequestedPriority: "high",
	}
	nilBlockers := admission.Candidate{CandidateID: "c-nil", ClinicalUrgency: "routine"}
	emptyBlockers := admission.Candidate{CandidateID: "c-empty", Blockers: map[admission.BlockerOwner]admission.Blocker{}}
	for _, c := range []admission.Candidate{withProjection, nilBlockers, emptyBlockers} {
		if _, err := s.CreateCandidate(ctx, c); err != nil {
			t.Fatalf("CreateCandidate(%q): %v", c.CandidateID, err)
		}
	}

	r := reopen(t, s, path)
	got, ok, err := r.GetCandidate(ctx, "c-proj")
	if err != nil || !ok {
		t.Fatalf("GetCandidate after reopen: ok=%v err=%v", ok, err)
	}
	gotAt := got.AuthProjection.Decision.Provenance.ObservedAt
	if !gotAt.Equal(observed) || gotAt.Location() != time.UTC {
		t.Fatalf("ObservedAt = %v (%v), want instant %v in UTC", gotAt, gotAt.Location(), observed)
	}
	want := withProjection
	wantProj := *withProjection.AuthProjection
	wantProj.Decision.Provenance.ObservedAt = observed.UTC()
	want.AuthProjection = &wantProj
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate changed across reopen\nwant=%+v\ngot=%+v", want, got)
	}
	if c, _, _ := r.GetCandidate(ctx, "c-nil"); c.Blockers != nil {
		t.Fatalf("nil blockers became %#v", c.Blockers)
	}
	if c, _, _ := r.GetCandidate(ctx, "c-empty"); c.Blockers == nil || len(c.Blockers) != 0 {
		t.Fatalf("empty blockers became %#v", c.Blockers)
	}
}

// Registration identity is immutable through ApplyOperation: a mutate that rewrites
// urgency/priority/id changes neither the stored nor the returned candidate.
func TestApplyOperationKeepsRegistrationIdentity(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, dbPath(t))
	if _, err := s.CreateCandidate(ctx, admission.Candidate{CandidateID: "c1", ClinicalUrgency: "routine", RequestedPriority: "normal"}); err != nil {
		t.Fatalf("CreateCandidate: %v", err)
	}
	got, err := s.ApplyOperation(ctx, admission.OperationRecord{OperationID: "op", CandidateID: "c1", Fingerprint: "f"},
		func(c *admission.Candidate) error {
			c.CandidateID, c.ClinicalUrgency, c.RequestedPriority = "other", "urgent", "high"
			return nil
		})
	if err != nil {
		t.Fatalf("ApplyOperation: %v", err)
	}
	stored, _, err := s.GetCandidate(ctx, "c1")
	if err != nil {
		t.Fatalf("GetCandidate: %v", err)
	}
	want := admission.Candidate{CandidateID: "c1", ClinicalUrgency: "routine", RequestedPriority: "normal"}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(stored, want) {
		t.Fatalf("registration identity rewritten\nreturned=%+v\nstored=%+v", got, stored)
	}
}

// A database written by a newer schema is refused, not reinterpreted.
func TestRejectsNewerSchema(t *testing.T) {
	path := dbPath(t)
	s := mustOpen(t, path)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	if _, err := raw.Exec("PRAGMA user_version = 99"); err != nil {
		t.Fatalf("set user_version: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	if s, err := sqlitestore.Open(context.Background(), path); !errors.Is(err, sqlitestore.ErrUnsupportedSchema) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("Open newer schema: err=%v, want ErrUnsupportedSchema", err)
	}
}

func TestOpenRejectsInvalidPath(t *testing.T) {
	for _, p := range []string{"", "a.db?mode=ro"} {
		if s, err := sqlitestore.Open(context.Background(), p); err == nil {
			_ = s.Close()
			t.Fatalf("Open(%q) succeeded, want error", p)
		}
	}
}
