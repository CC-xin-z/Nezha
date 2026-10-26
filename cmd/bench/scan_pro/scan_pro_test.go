package main

import "testing"

func TestSplitScanRequests(t *testing.T) {
	cases := []struct {
		total   int
		clients int
		want    []int
	}{
		{39063, 16, []int{2442, 2442, 2442, 2442, 2442, 2442, 2442, 2441, 2441, 2441, 2441, 2441, 2441, 2441, 2441, 2441}},
		{10, 3, []int{4, 3, 3}},
		{2, 5, []int{1, 1, 0, 0, 0}},
		{0, 4, []int{0, 0, 0, 0}},
	}
	for _, tc := range cases {
		got := splitScanRequests(tc.total, tc.clients)
		if len(got) != len(tc.want) {
			t.Fatalf("splitScanRequests(%d,%d) length = %d, want %d",
				tc.total, tc.clients, len(got), len(tc.want))
		}
		sum := 0
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("splitScanRequests(%d,%d)[%d] = %d, want %d",
					tc.total, tc.clients, i, got[i], tc.want[i])
			}
			sum += got[i]
		}
		if sum != tc.total {
			t.Fatalf("splitScanRequests(%d,%d) sum = %d", tc.total, tc.clients, sum)
		}
	}
}
