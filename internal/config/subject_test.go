package config

import "testing"

// TestWithImage covers the image-name override and its optional ":tag"
// splitting, including the registry-port edge case where a colon is part of
// the host:port rather than a tag.
func TestWithImage(t *testing.T) {
	base := Subject{Name: "vmetric", Image: "vmetric/director", Version: "2.0.1"}

	tests := []struct {
		name        string
		image       string
		wantImage   string
		wantVersion string
	}{
		{
			name:        "name only keeps registry version",
			image:       "myrepo/director-dev",
			wantImage:   "myrepo/director-dev",
			wantVersion: "2.0.1",
		},
		{
			name:        "name with tag splits version",
			image:       "myrepo/director-dev:pr-1234",
			wantImage:   "myrepo/director-dev",
			wantVersion: "pr-1234",
		},
		{
			name:        "registry port is not mistaken for a tag",
			image:       "localhost:5000/director",
			wantImage:   "localhost:5000/director",
			wantVersion: "2.0.1",
		},
		{
			name:        "registry port with tag",
			image:       "localhost:5000/director:dev",
			wantImage:   "localhost:5000/director",
			wantVersion: "dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := base.WithImage(tt.image)
			if got.Image != tt.wantImage {
				t.Errorf("Image = %q, want %q", got.Image, tt.wantImage)
			}
			if got.Version != tt.wantVersion {
				t.Errorf("Version = %q, want %q", got.Version, tt.wantVersion)
			}
			// WithImage must not mutate the receiver.
			if base.Image != "vmetric/director" || base.Version != "2.0.1" {
				t.Errorf("receiver mutated: %+v", base)
			}
		})
	}
}
