package journal

import (
	"context"
	"testing"
)

func TestValidateStoreIdentityReadsCurrentMetadata(t *testing.T) {
	for _, mode := range []string{"unchanged", "changed", "missing"} {
		t.Run(mode, func(t *testing.T) {
			j, err := Open(context.Background(), Options{StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			switch mode {
			case "changed":
				_, err = j.db.Exec("UPDATE meta SET value=? WHERE key='store_id'", "different-identity")
			case "missing":
				_, err = j.db.Exec("DELETE FROM meta WHERE key='store_id'")
			}
			if err != nil {
				t.Fatal(err)
			}
			err = j.ValidateStoreIdentity(context.Background())
			if (err != nil) != (mode != "unchanged") {
				t.Fatalf("validation=%v", err)
			}
		})
	}
}
