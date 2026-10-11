package migrations

import "testing"

func TestManagementJournalMigrationSequence(t *testing.T) {
	all, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 15 || all[14].name != "0015_runtime_management_transitions.sql" {
		t.Fatal("management journal migration is not in the executable sequence")
	}
}
