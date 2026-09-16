package runtime

import (
	"errors"

	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
)

// NewJournalAdapter uses durable per-operation endpoint identity from the
// SQLite journal. An optional registry remains a compatibility/test seam and
// is checked against the durable values when supplied.
func NewJournalAdapter(inner *journal.Journal, registry service.IdentityRegistry) (service.SQLiteJournal, error) {
	if inner == nil {
		return service.SQLiteJournal{}, errors.New("runtime: journal is required")
	}
	return service.SQLiteJournal{Inner: inner, Registry: registry}, nil
}
