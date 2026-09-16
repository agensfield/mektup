package endpoint

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestBuiltinIdentityLockIsSharedAcrossOperationJournals(t *testing.T) {
	root := t.TempDir()
	identity := filepath.Join(root, "identity")
	const callers = 48
	gate := make(chan struct{})
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for i := 0; i < callers; i++ {
		store := NewStoreWithIdentityHome(filepath.Join(root, "config"), filepath.Join(root, "ops", fmt.Sprint(i)), identity)
		group.Add(1)
		go func() {
			defer group.Done()
			<-gate
			local, err := store.EnsureBuiltinLocal(filepath.Join(root, "codex"))
			if err != nil {
				errs <- err
				return
			}
			ids <- local.ID
		}()
	}
	close(gate)
	group.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("same home returned %d identities", len(seen))
	}
}
