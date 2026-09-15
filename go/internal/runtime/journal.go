package runtime

import (
	"errors"

	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
)

// NewJournalAdapter binds endpoint identity through an operation-keyed
// registry. The journal schema intentionally stores routes/digests but not
// endpoint IDs, so a restart caller must provide a durable registry if it
// needs to reconstruct portable receipt identity.
func NewJournalAdapter(inner *journal.Journal, registry service.IdentityRegistry) (service.SQLiteJournal, error) {
	if inner == nil {
		return service.SQLiteJournal{}, errors.New("runtime: journal is required")
	}
	if registry == nil {
		return service.SQLiteJournal{}, errors.New("runtime: operation identity registry is required")
	}
	return service.SQLiteJournal{Inner: inner, Registry: registry}, nil
}
