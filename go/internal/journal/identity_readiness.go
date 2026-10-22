package journal

import (
	"context"
	"fmt"
)

// ValidateStoreIdentity reads current metadata through the existing connection.
// It never opens another SQLite handle, migrates, repairs or changes identity.
func (j *Journal) ValidateStoreIdentity(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := j.ValidateOpenIdentity(); err != nil {
		return err
	}
	var actual string
	if err := j.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='store_id'").Scan(&actual); err != nil {
		return fmt.Errorf("journal: current store identity is unavailable: %w", err)
	}
	if actual != j.StoreID() {
		return fmt.Errorf("journal: current store identity differs from opened identity: %w", ErrCorrupt)
	}
	return j.ValidateOpenIdentity()
}
