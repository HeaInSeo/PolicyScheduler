package sqlitestore_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
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

// A file this package did not create is refused and left byte-identical: it is never
// stamped, migrated, switched to WAL or adopted.
func TestOpenRefusesForeignDatabase(t *testing.T) {
	cases := []struct {
		name  string
		stmts []string
	}{
		{"unmarked-other-table", []string{"CREATE TABLE other (x TEXT)"}},
		{"unmarked-lookalike-tables", []string{
			"CREATE TABLE candidates (candidate_id TEXT PRIMARY KEY, note TEXT)",
			"INSERT INTO candidates VALUES ('c1', 'foreign')",
		}},
		{"unmarked-user-version", []string{"PRAGMA user_version = 1"}},
		{"other-application-id", []string{"PRAGMA application_id = 1234"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := dbPath(t)
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("raw open: %v", err)
			}
			for _, stmt := range tc.stmts {
				if _, err := raw.Exec(stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			if err := raw.Close(); err != nil {
				t.Fatalf("raw close: %v", err)
			}
			assertOpenLeavesFileUntouched(t, path, sqlitestore.ErrForeignDatabase)
		})
	}
	t.Run("not-sqlite", func(t *testing.T) {
		path := dbPath(t)
		if err := os.WriteFile(path, []byte("not a sqlite database, just some bytes\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		assertOpenLeavesFileUntouched(t, path, nil)
	})
}

// assertOpenLeavesFileUntouched requires Open(path) to fail (with want, if non-nil)
// and the file to be byte-identical afterwards with no journal/WAL left behind.
func assertOpenLeavesFileUntouched(t *testing.T, path string, want error) {
	t.Helper()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read before: %v", err)
	}
	s, err := sqlitestore.Open(context.Background(), path)
	if err == nil {
		_ = s.Close()
		t.Fatal("Open adopted a foreign file")
	}
	if want != nil && !errors.Is(err, want) {
		t.Fatalf("Open: err=%v, want %v", err, want)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("refused Open modified the file (%d → %d bytes)", len(before), len(after))
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(path + suffix); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("refused Open left %s behind: %v", suffix, err)
		}
	}
}

// A foreign database left after a crash with committed frames still in its -wal is
// refused without checkpointing: the main file and the -wal stay byte-identical and
// no -shm or -journal appears. The foreign table exists only in the WAL, so the
// refusal also proves ownership is judged with the WAL applied.
func TestOpenRefusesForeignCrashStateWAL(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "crashed.db")
	raw, err := sql.Open("sqlite", src)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	raw.SetMaxOpenConns(1)
	for _, stmt := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA wal_autocheckpoint = 0",
		"CREATE TABLE other (x TEXT)",
		"INSERT INTO other VALUES ('foreign')",
	} {
		if _, err := raw.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Copy the files while the writer is still open: that is the on-disk state a crash
	// leaves behind (the normal close below would checkpoint the WAL away).
	path := filepath.Join(dir, "admission.db")
	for _, suffix := range []string{"", "-wal"} {
		b, err := os.ReadFile(src + suffix)
		if err != nil {
			t.Fatalf("read %s: %v", src+suffix, err)
		}
		if suffix == "-wal" && len(b) == 0 {
			t.Fatal("precondition: the crash-state WAL holds no frames")
		}
		if err := os.WriteFile(path+suffix, b, 0o600); err != nil {
			t.Fatalf("write %s: %v", path+suffix, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	assertCrashStateRefused(t, path, "-wal")
}

// A foreign rollback-journal database left after a crash mid-transaction, with pages
// already spilled into the main file and its -journal still hot, is refused without
// rolling the journal back: the main file and the -journal stay byte-identical and no
// -wal or -shm appears.
func TestOpenRefusesForeignHotJournal(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "crashed.db")
	raw, err := sql.Open("sqlite", src)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	raw.SetMaxOpenConns(1)
	for _, stmt := range []string{
		"PRAGMA journal_mode = DELETE",
		"PRAGMA cache_size = 1",
		"CREATE TABLE other (x TEXT)",
		"INSERT INTO other VALUES ('foreign')",
		"BEGIN",
		"WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500) " +
			"INSERT INTO other SELECT hex(randomblob(500)) FROM n",
	} {
		if _, err := raw.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Copy the files while the transaction is still open: that is the on-disk state a
	// crash leaves behind (the rollback below would delete the journal).
	path := filepath.Join(dir, "admission.db")
	for _, suffix := range []string{"", "-journal"} {
		b, err := os.ReadFile(src + suffix)
		if err != nil {
			t.Fatalf("read %s: %v", src+suffix, err)
		}
		if suffix == "-journal" && len(b) == 0 {
			t.Fatal("precondition: the crash-state journal is empty")
		}
		if err := os.WriteFile(path+suffix, b, 0o600); err != nil {
			t.Fatalf("write %s: %v", path+suffix, err)
		}
	}
	if _, err := raw.Exec("ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	assertCrashStateRefused(t, path, "-journal")
}

// assertCrashStateRefused requires Open(path) to fail with ErrForeignDatabase while
// path and path+sidecar stay byte-identical and no other journal/WAL file appears.
func assertCrashStateRefused(t *testing.T, path, sidecar string) {
	t.Helper()
	kept := []string{"", sidecar}
	before := map[string][]byte{}
	for _, suffix := range kept {
		b, err := os.ReadFile(path + suffix)
		if err != nil {
			t.Fatalf("read before: %v", err)
		}
		before[suffix] = b
	}
	if s, err := sqlitestore.Open(context.Background(), path); !errors.Is(err, sqlitestore.ErrForeignDatabase) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("Open crash-state foreign file: err=%v, want ErrForeignDatabase", err)
	}
	for _, suffix := range kept {
		after, err := os.ReadFile(path + suffix)
		if err != nil {
			t.Fatalf("refused Open removed %s: %v", path+suffix, err)
		}
		if !bytes.Equal(before[suffix], after) {
			t.Fatalf("refused Open modified %q (%d → %d bytes)", "admission.db"+suffix, len(before[suffix]), len(after))
		}
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if suffix == sidecar {
			continue
		}
		if _, err := os.Stat(path + suffix); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("refused Open left %s behind: %v", suffix, err)
		}
	}
}

// Blocker owners are stored as JSON, which cannot carry invalid UTF-8 byte for byte.
// Such an owner is refused before anything is written, on create and through an
// operation, whether it is the map key, Blocker.Owner, or both; valid owners,
// including case- and accent-distinct ones, keep their exact bytes across a reopen.
func TestBlockerOwnerByteIdentity(t *testing.T) {
	ctx := context.Background()
	path := dbPath(t)
	s := mustOpen(t, path)
	for name, owners := range map[string][2]admission.BlockerOwner{
		"invalid-byte":     {"\xff", "\xff"},
		"truncated-rune":   {"readi\xc3", "readi\xc3"},
		"invalid-in-valid": {"Read\xfeiness", "Read\xfeiness"},
		"invalid-key-only": {"\xff", "Readiness"},
		"invalid-owner":    {"Readiness", "\xff"},
	} {
		cand := admission.Candidate{CandidateID: admission.CandidateID("c-" + name), Blockers: map[admission.BlockerOwner]admission.Blocker{
			owners[0]: {Owner: owners[1], State: admission.BlockerActive},
		}}
		if _, err := s.CreateCandidate(ctx, cand); !errors.Is(err, sqlitestore.ErrInvalidOwner) {
			t.Fatalf("%s: CreateCandidate err=%v, want ErrInvalidOwner", name, err)
		}
		if _, ok, err := s.GetCandidate(ctx, cand.CandidateID); ok || err != nil {
			t.Fatalf("%s: refused candidate stored: ok=%v err=%v", name, ok, err)
		}
	}

	valid := admission.Candidate{CandidateID: "c-valid", Blockers: map[admission.BlockerOwner]admission.Blocker{
		"Prüfung": {Owner: "Prüfung", State: admission.BlockerActive},
		"prüfung": {Owner: "prüfung", State: admission.BlockerActive},
		"PRÜFUNG": {Owner: "PRÜFUNG", State: admission.BlockerActive},
	}}
	if _, err := s.CreateCandidate(ctx, valid); err != nil {
		t.Fatalf("CreateCandidate(valid): %v", err)
	}
	op := admission.OperationRecord{OperationID: "op-invalid", CandidateID: "c-valid", Fingerprint: "f"}
	if _, err := s.ApplyOperation(ctx, op, func(c *admission.Candidate) error {
		c.Blockers["\xff"] = admission.Blocker{Owner: "\xff", State: admission.BlockerActive}
		return nil
	}); !errors.Is(err, sqlitestore.ErrInvalidOwner) {
		t.Fatalf("ApplyOperation adding invalid owner: err=%v, want ErrInvalidOwner", err)
	}
	// The refused operation was not recorded: the same ID now applies as a fresh operation.
	applied := false
	if _, err := s.ApplyOperation(ctx, op, func(*admission.Candidate) error { applied = true; return nil }); err != nil || !applied {
		t.Fatalf("operation after refusal: applied=%v err=%v, want a fresh apply", applied, err)
	}

	r := reopen(t, s, path)
	got, ok, err := r.GetCandidate(ctx, "c-valid")
	if err != nil || !ok {
		t.Fatalf("GetCandidate after reopen: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(got, valid) {
		t.Fatalf("owner bytes changed across reopen\nwant=%#v\ngot=%#v", valid.Blockers, got.Blockers)
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
