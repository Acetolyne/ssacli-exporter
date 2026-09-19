//go:build !windows

package main

import "testing"

func TestParseLogicalDriveStatus(t *testing.T) {
	out := `
Smart Array P440ar in Slot 0 (Embedded)

   logicaldrive 1 (279.4 GB, RAID 1): OK
   logicaldrive 2 (558.9 GB, RAID 5): Recovering, 45% complete
   logicaldrive 3 (136.7 GB, RAID 1): Interim Recovery Mode
   logicaldrive 4 (136.7 GB, RAID 1): Failed
   logicaldrive 5 (136.7 GB, RAID 1): Recovering, 250% complete
`
	want := []logicalDriveRebuild{
		{"1", "279.4 GB, RAID 1", 100},
		{"2", "558.9 GB, RAID 5", 45},
		{"3", "136.7 GB, RAID 1", 0},
		{"4", "136.7 GB, RAID 1", 0},
		{"5", "136.7 GB, RAID 1", 100},
	}
	got := parseLogicalDriveStatus(out)
	if len(got) != len(want) {
		t.Fatalf("got %d drives, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("drive %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}
