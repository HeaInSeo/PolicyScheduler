// Package sqlitestore is the J1 durable admission.Store: one SQLite database file
// owned by exactly one writer process. It realizes the existing admission.Store
// contract (storetest C1–C8, including a true reopen) and adds no admission
// semantics of its own.
//
// Scope boundary: J1 single-writer only. It makes no J2/HA, replication,
// cross-service or deployment-topology claim, and it is not wired into any caller.
// The single-writer property is enforced, not assumed: the database is opened in
// SQLite EXCLUSIVE locking mode and the lock is taken at Open, so a second Open of
// the same file (another process, or another handle in this process) fails with
// ErrWriterFenced instead of silently becoming a second writer.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/HeaInSeo/PolicyScheduler/admission"
)

// schemaVersion is stored in PRAGMA user_version. A database written by a newer
// schema is refused rather than reinterpreted.
const schemaVersion = 1

// applicationID is stored in PRAGMA application_id (ASCII "PSAD") and marks a file
// as a PolicyScheduler admission store. Open adopts only a file carrying this marker
// or a brand-new empty file; any other file is foreign and is refused unwritten.
const applicationID = 0x50534144

const schema = `
CREATE TABLE IF NOT EXISTS candidates (
	candidate_id       TEXT NOT NULL PRIMARY KEY,
	clinical_urgency   TEXT NOT NULL,
	requested_priority TEXT NOT NULL,
	blockers           TEXT NOT NULL,
	auth_projection    TEXT
);
CREATE TABLE IF NOT EXISTS operations (
	operation_id TEXT NOT NULL PRIMARY KEY,
	candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
	fingerprint  TEXT NOT NULL
);
`

// ErrWriterFenced reports that another handle already owns the database file as its
// single J1 writer.
var ErrWriterFenced = errors.New("sqlitestore: database is owned by another writer (J1 single-writer fence)")

// ErrUnsupportedSchema reports a database whose schema version this build does not
// understand.
var ErrUnsupportedSchema = errors.New("sqlitestore: unsupported schema version")

// ErrForeignDatabase reports an existing database file that was not created by this
// package. It is never stamped, migrated or adopted.
var ErrForeignDatabase = errors.New("sqlitestore: database file is not a PolicyScheduler admission store")

// ErrInvalidOwner reports a blocker owner that is not valid UTF-8. Owners are stored
// as JSON, which would replace invalid bytes and so lose the owner's exact-byte
// identity; such a candidate is refused before anything is written.
var ErrInvalidOwner = errors.New("sqlitestore: blocker owner is not valid UTF-8")

// Store is a J1 SQLite admission.Store. It is safe for concurrent use within the
// owning process: every operation runs in its own transaction on the single
// connection, so operations are serialized.
type Store struct {
	db *sql.DB
}

var _ admission.Store = (*Store)(nil)

// Open opens (creating if needed) the database at path and takes the single-writer
// lock. The returned Store must be closed to release the lock.
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" || strings.ContainsRune(path, '?') {
		return nil, fmt.Errorf("sqlitestore: invalid database path %q", path)
	}
	if err := preflight(ctx, path); err != nil {
		return nil, err
	}
	// EXCLUSIVE locking must be set before WAL is first used so no shared-memory
	// index is created; busy_timeout(0) makes a fenced second writer fail fast. WAL is
	// not requested here: switching the journal mode writes the file header, so it is
	// enabled only after migrate has verified the file is ours (see enableWAL).
	dsn := path + "?_pragma=locking_mode(EXCLUSIVE)&_pragma=busy_timeout(0)" +
		"&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: open: %w", err)
	}
	// One long-lived connection holds the exclusive lock for the Store's lifetime.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := enableWAL(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the database and its single-writer lock.
func (s *Store) Close() error { return s.db.Close() }

// preflight protects a file left with a non-empty -wal (a crash, or a live writer).
// Closing the writable handle after migrate refuses such a file would checkpoint the
// WAL into it, and SQLite cannot read a WAL database through a read-only handle here
// without creating a -shm. So ownership is judged, with the WAL applied, on a private
// copy of the main file and its -wal; the original is opened writable only if the copy
// is ours or brand-new empty. Ownership refusals are returned; any other probe failure
// (for example a torn copy of a live writer's WAL) falls through to the writer open,
// which fences or reports it. A file without a -wal is verified by migrate as before.
func preflight(ctx context.Context, path string) error {
	info, err := os.Stat(path + "-wal")
	if err != nil || info.Size() == 0 {
		return nil
	}
	dir, err := os.MkdirTemp("", "sqlitestore-preflight-")
	if err != nil {
		return fmt.Errorf("sqlitestore: preflight: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	probe := filepath.Join(dir, "probe.db")
	for _, suffix := range []string{"", "-wal"} {
		if err := copyFile(path+suffix, probe+suffix); err != nil {
			return fmt.Errorf("sqlitestore: preflight copy: %w", err)
		}
	}
	db, err := sql.Open("sqlite", probe)
	if err != nil {
		return fmt.Errorf("sqlitestore: preflight open: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if err := verifyOwnership(ctx, db); errors.Is(err, ErrForeignDatabase) || errors.Is(err, ErrUnsupportedSchema) {
		return err
	}
	return nil
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src) // #nosec G304 -- the caller's own database path
	if err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0o600)
}

// verifyOwnership adopts only a file marked with applicationID (at a schema version
// this build understands) or an unmarked, brand-new empty database.
func verifyOwnership(ctx context.Context, q rowQuerier) error {
	var appID, version int
	if err := q.QueryRowContext(ctx, "PRAGMA application_id").Scan(&appID); err != nil {
		return classify("read application id", err)
	}
	if err := q.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return classify("read schema version", err)
	}
	switch {
	case appID == applicationID:
		if version > schemaVersion {
			return fmt.Errorf("%w: database has %d, this build supports %d", ErrUnsupportedSchema, version, schemaVersion)
		}
	case appID == 0 && version == 0:
		// Unmarked: adopt it only if it is a brand-new empty database.
		var objects int
		if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&objects); err != nil {
			return classify("inspect unmarked database", err)
		}
		if objects != 0 {
			return fmt.Errorf("%w: unmarked file already holds %d schema objects", ErrForeignDatabase, objects)
		}
	default:
		return fmt.Errorf("%w: application_id=%#x user_version=%d", ErrForeignDatabase, appID, version)
	}
	return nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return classify("begin migration", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Re-checked under the writer lock: the file may have changed since preflight.
	if err := verifyOwnership(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return classify("init schema", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", applicationID)); err != nil {
		return classify("write application id", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return classify("write schema version", err)
	}
	if err := tx.Commit(); err != nil {
		return classify("commit migration", err)
	}
	return nil
}

// enableWAL switches the verified database to WAL. The migration commit already holds
// the EXCLUSIVE lock, so no shared-memory index is created.
func enableWAL(ctx context.Context, db *sql.DB) error {
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return classify("enable WAL", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("sqlitestore: journal_mode is %q, want wal", mode)
	}
	return nil
}

// classify maps SQLite lock contention to ErrWriterFenced and wraps everything else.
func classify(step string, err error) error {
	var se *sqlite.Error
	if errors.As(err, &se) && (se.Code()&0xff == sqlite3.SQLITE_BUSY || se.Code()&0xff == sqlite3.SQLITE_LOCKED) {
		return fmt.Errorf("%w: %s: %v", ErrWriterFenced, step, err)
	}
	return fmt.Errorf("sqlitestore: %s: %w", step, err)
}

// CreateCandidate implements admission.Store.
func (s *Store) CreateCandidate(ctx context.Context, cand admission.Candidate) (admission.Candidate, error) {
	if err := checkOwners(cand); err != nil {
		return admission.Candidate{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return admission.Candidate{}, classify("begin create", err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, ok, err := loadCandidate(ctx, tx, cand.CandidateID)
	if err != nil {
		return admission.Candidate{}, err
	}
	if ok {
		if !admission.SameRegistration(existing, cand) {
			return admission.Candidate{}, fmt.Errorf("%w: %q", admission.ErrCandidateConflict, cand.CandidateID)
		}
		return existing, nil
	}
	row, err := encode(cand)
	if err != nil {
		return admission.Candidate{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO candidates (candidate_id, clinical_urgency, requested_priority, blockers, auth_projection) VALUES (?, ?, ?, ?, ?)`,
		string(cand.CandidateID), cand.ClinicalUrgency, cand.RequestedPriority, row.blockers, row.projection); err != nil {
		return admission.Candidate{}, classify("insert candidate", err)
	}
	if err := tx.Commit(); err != nil {
		return admission.Candidate{}, classify("commit create", err)
	}
	return decode(cand.CandidateID, cand.ClinicalUrgency, cand.RequestedPriority, row)
}

// GetCandidate implements admission.Store.
func (s *Store) GetCandidate(ctx context.Context, id admission.CandidateID) (admission.Candidate, bool, error) {
	return loadCandidate(ctx, s.db, id)
}

// rowQuerier is satisfied by both *sql.DB and *sql.Tx.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ApplyOperation implements admission.Store. The operation ledger row and the
// candidate mutation commit in one transaction. mutate runs inside that
// transaction and must not call back into this Store.
func (s *Store) ApplyOperation(ctx context.Context, op admission.OperationRecord, mutate func(*admission.Candidate) error) (admission.Candidate, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return admission.Candidate{}, classify("begin operation", err)
	}
	defer func() { _ = tx.Rollback() }()

	cand, ok, err := loadCandidate(ctx, tx, op.CandidateID)
	if err != nil {
		return admission.Candidate{}, err
	}
	if !ok {
		return admission.Candidate{}, fmt.Errorf("%w: %q", admission.ErrCandidateNotFound, op.CandidateID)
	}

	var priorCandidate, priorFingerprint string
	err = tx.QueryRowContext(ctx, `SELECT candidate_id, fingerprint FROM operations WHERE operation_id = ?`, op.OperationID).
		Scan(&priorCandidate, &priorFingerprint)
	switch {
	case err == nil:
		if priorFingerprint != op.Fingerprint || priorCandidate != string(op.CandidateID) {
			return admission.Candidate{}, fmt.Errorf("%w: %q", admission.ErrOperationConflict, op.OperationID)
		}
		// Idempotent replay: already durably applied; return the current candidate.
		return cand, nil
	case !errors.Is(err, sql.ErrNoRows):
		return admission.Candidate{}, classify("read operation", err)
	}

	// Registration identity (id, urgency, priority) is immutable: whatever mutate does
	// to those fields is neither persisted nor returned.
	id, urgency, priority := cand.CandidateID, cand.ClinicalUrgency, cand.RequestedPriority
	if err := mutate(&cand); err != nil {
		return admission.Candidate{}, err
	}
	row, err := encode(cand)
	if err != nil {
		return admission.Candidate{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE candidates SET blockers = ?, auth_projection = ? WHERE candidate_id = ?`,
		row.blockers, row.projection, string(id)); err != nil {
		return admission.Candidate{}, classify("update candidate", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations (operation_id, candidate_id, fingerprint) VALUES (?, ?, ?)`,
		op.OperationID, string(op.CandidateID), op.Fingerprint); err != nil {
		return admission.Candidate{}, classify("insert operation", err)
	}
	if err := tx.Commit(); err != nil {
		return admission.Candidate{}, classify("commit operation", err)
	}
	// Decode the committed row so the result is identical to a subsequent GetCandidate.
	return decode(id, urgency, priority, row)
}

func loadCandidate(ctx context.Context, q rowQuerier, id admission.CandidateID) (admission.Candidate, bool, error) {
	var urgency, priority string
	var row encodedRow
	err := q.QueryRowContext(ctx,
		`SELECT clinical_urgency, requested_priority, blockers, auth_projection FROM candidates WHERE candidate_id = ?`, string(id)).
		Scan(&urgency, &priority, &row.blockers, &row.projection)
	if errors.Is(err, sql.ErrNoRows) {
		return admission.Candidate{}, false, nil
	}
	if err != nil {
		return admission.Candidate{}, false, classify("read candidate", err)
	}
	cand, err := decode(id, urgency, priority, row)
	if err != nil {
		return admission.Candidate{}, false, err
	}
	return cand, true, nil
}

// encodedRow is the persisted form of a candidate's mutable state.
type encodedRow struct {
	blockers   string
	projection sql.NullString
}

// encode serializes the mutable candidate state. A nil Blockers map is stored as
// JSON null and an empty map as {}, so the distinction survives a reopen. The
// projection's ObservedAt is normalized to UTC: it is preserved to the nanosecond
// (time.Equal) but is read back in UTC without a monotonic clock reading.
func encode(c admission.Candidate) (encodedRow, error) {
	if err := checkOwners(c); err != nil {
		return encodedRow{}, err
	}
	blockers, err := json.Marshal(c.Blockers)
	if err != nil {
		return encodedRow{}, fmt.Errorf("sqlitestore: encode blockers: %w", err)
	}
	row := encodedRow{blockers: string(blockers)}
	if c.AuthProjection != nil {
		proj := *c.AuthProjection
		proj.Decision.Provenance.ObservedAt = proj.Decision.Provenance.ObservedAt.UTC()
		encoded, err := json.Marshal(proj)
		if err != nil {
			return encodedRow{}, fmt.Errorf("sqlitestore: encode authorization projection: %w", err)
		}
		row.projection = sql.NullString{String: string(encoded), Valid: true}
	}
	return row, nil
}

// checkOwners refuses a candidate whose blocker owners (map keys or Blocker.Owner)
// would not survive the JSON encoding byte for byte.
func checkOwners(c admission.Candidate) error {
	for owner, b := range c.Blockers {
		if !utf8.ValidString(string(owner)) || !utf8.ValidString(string(b.Owner)) {
			return fmt.Errorf("%w: candidate %q owner %q", ErrInvalidOwner, c.CandidateID, owner)
		}
	}
	return nil
}

func decode(id admission.CandidateID, urgency, priority string, row encodedRow) (admission.Candidate, error) {
	cand := admission.Candidate{CandidateID: id, ClinicalUrgency: urgency, RequestedPriority: priority}
	if err := json.Unmarshal([]byte(row.blockers), &cand.Blockers); err != nil {
		return admission.Candidate{}, fmt.Errorf("sqlitestore: decode blockers of %q: %w", id, err)
	}
	if row.projection.Valid {
		var proj admission.AuthorizationProjection
		if err := json.Unmarshal([]byte(row.projection.String), &proj); err != nil {
			return admission.Candidate{}, fmt.Errorf("sqlitestore: decode authorization projection of %q: %w", id, err)
		}
		cand.AuthProjection = &proj
	}
	return cand, nil
}
