// Package storage exposes the explicit local journal maintenance surfaces.
// It intentionally contains no CLI or daemon orchestration.
package storage

import (
	"context"
	"path/filepath"

	"github.com/agensfield/mektup/go/internal/journal"
)

type Status = journal.StorageStatus
type Check = journal.StorageCheck
type MaintenanceOptions = journal.MaintenanceOptions
type MaintenanceAction = journal.MaintenanceAction
type MaintenanceReceipt = journal.MaintenanceReceipt
type VacuumReceipt = journal.VacuumReceipt

const RetentionAge = journal.RetentionAge

// Store binds the maintenance API to an already-open journal. Opening a
// journal remains the caller's decision because Open may create/migrate state;
// CheckPath is available for a strictly read-only path inspection.
type Store struct{ Journal *journal.Journal }

func New(j *journal.Journal) Store { return Store{Journal: j} }

func (s Store) Status(ctx context.Context) (Status, error) { return s.Journal.StorageStatus(ctx) }
func (s Store) Check(ctx context.Context) (Check, error)   { return s.Journal.StorageCheck(ctx) }
func (s Store) Maintain(ctx context.Context, opts MaintenanceOptions) (MaintenanceReceipt, error) {
	return s.Journal.StorageMaintain(ctx, opts)
}
func (s Store) Vacuum(ctx context.Context) (VacuumReceipt, error) {
	return s.Journal.StorageVacuum(ctx)
}

func StatusOf(ctx context.Context, j *journal.Journal) (Status, error) {
	return j.StorageStatus(ctx)
}

func CheckOf(ctx context.Context, j *journal.Journal) (Check, error) {
	return j.StorageCheck(ctx)
}

func CheckPath(ctx context.Context, stateDir string) (Check, error) {
	return journal.CheckPath(ctx, filepath.Join(stateDir, "journal.sqlite3"))
}

func CheckDatabasePath(ctx context.Context, databasePath string) (Check, error) {
	return journal.CheckPath(ctx, databasePath)
}

func Maintain(ctx context.Context, j *journal.Journal, opts MaintenanceOptions) (MaintenanceReceipt, error) {
	return j.StorageMaintain(ctx, opts)
}

func Vacuum(ctx context.Context, j *journal.Journal) (VacuumReceipt, error) {
	return j.StorageVacuum(ctx)
}
