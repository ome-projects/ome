module sigs.k8s.io/ome/site

go 1.21

// Docsy is imported by hugo.toml, not by Go code, so `go mod tidy` deletes
// this requirement and Hugo then builds against the latest Docsy. v0.16.0
// moved the theme to github.com/google/docsy/theme and raised the minimum
// Hugo version; upgrade with `hugo mod get` alongside those site changes.
require github.com/google/docsy v0.15.0
