package constants

import "testing"

func TestGetRawServiceLabel(t *testing.T) {
	tests := []struct {
		name    string
		service string
		want    string
	}{
		{
			name:    "short name unchanged",
			service: "test-isvc-engine",
			want:    "test-isvc-engine",
		},
		{
			name:    "long name matches truncation helper",
			service: "amaaaaaabgjpxjqa4tzjnvnaeioaw6ewzj5uevu2qlj6ii6vknafdarwgmfq-engine",
			want:    TruncateNameWithMaxLength("a5b5c2cf-jqa4tzjnvnaeioaw6ewzj5uevu2qlj6ii6vknafdarwgmfq-engine", 63),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetRawServiceLabel(tt.service)
			if got != tt.want {
				t.Fatalf("GetRawServiceLabel(%q) = %q, want %q", tt.service, got, tt.want)
			}
		})
	}
}
