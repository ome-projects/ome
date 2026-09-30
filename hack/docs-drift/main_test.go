package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// testRepo is a git repository with Hugo pages a.md, b.md and c.md and
// website pages x.md, y.md and generated.md, whose front matter sets
// generated: true.
type testRepo struct {
	dir    string
	first  string // adds every page, and d.md
	update string // changes a.md
	head   string // removes d.md
}

func newRepo(t *testing.T) testRepo {
	t.Helper()
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		// Fixed identities and dates, and no user or system config, so
		// commits don't depend on the machine.
		cmd.Env = append(os.Environ(),
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_AUTHOR_DATE=2026-01-02T03:04:05Z",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com", "GIT_COMMITTER_DATE=2026-01-02T03:04:05Z",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(name, content string) {
		t.Helper()
		file := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	git("init", "-q", "-b", "main")
	for _, page := range []string{"a.md", "b.md", "c.md", "d.md"} {
		write("site/content/en/docs/"+page, page+"\n")
	}
	write("website/src/lib/content/x.md", "x\n")
	write("website/src/lib/content/y.md", "y\n")
	write("website/src/lib/content/generated.md", "---\ntitle: Generated\ngenerated: true\n---\ngenerated\n")
	git("add", ".")
	git("commit", "-q", "-m", "Add pages")
	repo := testRepo{dir: dir, first: git("rev-parse", "--short", "HEAD")}
	write("site/content/en/docs/a.md", "a, updated\n")
	git("commit", "-q", "-am", "Update a")
	repo.update = git("rev-parse", "--short", "HEAD")
	git("rm", "-q", "site/content/en/docs/d.md")
	git("commit", "-q", "-m", "Remove d")
	repo.head = git("rev-parse", "--short", "HEAD")
	return repo
}

func TestRun(t *testing.T) {
	repo := newRepo(t)
	rewritten := func(rev string) *string { return &rev }
	redirects := func(entries ...entry) string {
		data, err := json.Marshal(entries)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	a := entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.update)}
	b := entry{Old: "b.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}
	c := entry{Old: "c.md", New: "x.md"}
	current := redirects(a, b, c)

	tests := []struct {
		name       string
		redirects  string   // the content of website/redirects.json
		args       []string // the arguments after -repo
		wantCode   int
		wantStdout string
		wantStderr []string
	}{
		{
			name:       "no drift",
			redirects:  current,
			wantCode:   0,
			wantStdout: "No drift: every Hugo page is mapped, and every rewrite is current.\n",
		},
		{
			name:       "drift",
			redirects:  redirects(a, c),
			wantCode:   1,
			wantStdout: "Hugo pages missing from website/redirects.json:\n  b.md\n",
		},
		{
			name:       "missing redirects file",
			redirects:  current,
			args:       []string{"-redirects", "website/missing.json"},
			wantCode:   2,
			wantStderr: []string{"docs-drift: open " + filepath.Join(repo.dir, "website", "missing.json") + ": no such file or directory"},
		},
		{
			name:       "malformed redirects file",
			redirects:  "{",
			wantCode:   2,
			wantStderr: []string{"docs-drift: parse website/redirects.json: unexpected end of JSON input"},
		},
		{
			name:       "missing Hugo directory",
			redirects:  current,
			args:       []string{"-old", "site/missing"},
			wantCode:   2,
			wantStderr: []string{"docs-drift: lstat " + filepath.Join(repo.dir, "site", "missing") + ": no such file or directory"},
		},
		{
			name:       "unknown flag",
			redirects:  current,
			args:       []string{"-olds", "site"},
			wantCode:   2,
			wantStderr: []string{"flag provided but not defined: -olds", "Usage of docs-drift:"},
		},
		{
			name:       "help",
			redirects:  current,
			args:       []string{"-h"},
			wantCode:   0,
			wantStderr: []string{"Usage of docs-drift:", "-repo directory"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(repo.dir, "website", "redirects.json"), []byte(tc.redirects), 0o644); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr strings.Builder
			if got := run(append([]string{"-repo", repo.dir}, tc.args...), &stdout, &stderr); got != tc.wantCode {
				t.Errorf("run() = %d, want %d; stderr:\n%s", got, tc.wantCode, stderr.String())
			}
			if diff := cmp.Diff(tc.wantStdout, stdout.String()); diff != "" {
				t.Errorf("stdout mismatch (-want +got):\n%s", diff)
			}
			if len(tc.wantStderr) == 0 && stderr.Len() > 0 {
				t.Errorf("run() wrote to stderr:\n%s", stderr.String())
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr doesn't contain %q:\n%s", want, stderr.String())
				}
			}
		})
	}
}

func TestDrift(t *testing.T) {
	repo := newRepo(t)
	rewritten := func(rev string) *string { return &rev }
	a := entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.update)}
	b := entry{Old: "b.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}
	c := entry{Old: "c.md", New: "x.md"}

	tests := []struct {
		name    string
		entries []entry
		want    report
	}{
		{
			name:    "current",
			entries: []entry{a, b, c},
		},
		{
			name:    "changed after the rewrite",
			entries: []entry{{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.first)}, b, c},
			want: report{changed: []change{{
				entry:   entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten(repo.first)},
				commits: []string{repo.update + " 2026-01-02 Update a"},
			}}},
		},
		{
			name:    "drafts are not compared",
			entries: []entry{{Old: "a.md", New: "x.md"}, b, c},
		},
		{
			name:    "generated pages are not compared",
			entries: []entry{{Old: "a.md", New: "generated.md", RewrittenFrom: rewritten(repo.first)}, b, c},
		},
		{
			name:    "removed Hugo page of a generated page",
			entries: []entry{a, b, c, {Old: "d.md", New: "generated.md", RewrittenFrom: rewritten(repo.first)}},
			want:    report{oldMissing: []entry{{Old: "d.md", New: "generated.md", RewrittenFrom: rewritten(repo.first)}}},
		},
		{
			name:    "unmapped page",
			entries: []entry{a, c},
			want:    report{unmapped: []string{"b.md"}},
		},
		{
			name:    "removed page",
			entries: []entry{a, b, c, {Old: "d.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}},
			want:    report{oldMissing: []entry{{Old: "d.md", New: "y.md", RewrittenFrom: rewritten(repo.first)}}},
		},
		{
			name:    "missing website page",
			entries: []entry{a, b, {Old: "c.md", New: "z.md"}},
			want:    report{newMissing: []entry{{Old: "c.md", New: "z.md"}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := drift(writeRedirects(t, repo, tc.entries))
			if err != nil {
				t.Fatalf("drift() error: %v", err)
			}
			if diff := cmp.Diff(&tc.want, got, cmp.AllowUnexported(report{}, change{})); diff != "" {
				t.Errorf("drift() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDriftErrors(t *testing.T) {
	repo := newRepo(t)
	invalid := filepath.Join(repo.dir, "website", "src", "lib", "content", "invalid.md")
	if err := os.WriteFile(invalid, []byte("---\ngenerated: \"yes\"\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rewritten := func(rev string) *string { return &rev }
	tests := []struct {
		name  string
		entry entry
		want  string
	}{
		{
			name:  "unknown commit",
			entry: entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten("0123456789")},
			want:  "git log 0123456789..HEAD",
		},
		{
			name:  "not a hash",
			entry: entry{Old: "a.md", New: "x.md", RewrittenFrom: rewritten("--output=log")},
			want:  `rewrittenFrom "--output=log" isn't a commit hash`,
		},
		{
			name:  "invalid front matter",
			entry: entry{Old: "a.md", New: "invalid.md", RewrittenFrom: rewritten(repo.first)},
			want:  invalid + `: front matter "generated" must be true or false`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := drift(writeRedirects(t, repo, []entry{tc.entry}))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("drift() error = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

func TestGenerated(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
		wantErr string // after the file name
	}{
		{name: "generated", content: "---\ntitle: API\ngenerated: true\n---\n# API\n", want: true},
		{name: "not generated", content: "---\ntitle: API\ngenerated: false\n---\n", want: false},
		{name: "no generated field", content: "---\ntitle: Page\n---\n", want: false},
		{name: "no front matter", content: "generated: true\n", want: false},
		{name: "CRLF line endings", content: "---\r\ntitle: API\r\ngenerated: true\r\n---\r\n# API\r\n", want: true},
		{name: "only front matter", content: "---\ngenerated: true\n---", want: true},
		{name: "a later block", content: "---\ntitle: Page\n---\n\n---\ngenerated: true\n---\n", want: false},
		{name: "not a boolean", content: "---\ngenerated: \"true\"\n---\n", wantErr: `front matter "generated" must be true or false`},
		{name: "no value", content: "---\ngenerated:\n---\n", wantErr: `front matter "generated" must be true or false`},
		{name: "invalid YAML", content: "---\ntitle: [\n---\n", wantErr: "invalid front matter: "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "page.md")
			if err := os.WriteFile(file, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := generated(file)
			if tc.wantErr != "" {
				if want := file + ": " + tc.wantErr; err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("generated() error = %v, want one containing %q", err, want)
				}
				return
			}
			if err != nil {
				t.Fatalf("generated() error: %v", err)
			}
			if got != tc.want {
				t.Errorf("generated() = %t, want %t", got, tc.want)
			}
		})
	}
}

// writeRedirects writes entries to the repository's redirects file, which
// stays untracked, and returns a config for the repository.
func writeRedirects(t *testing.T, repo testRepo, entries []entry) config {
	t.Helper()
	cfg := config{
		repo:      repo.dir,
		oldDir:    "site/content/en/docs",
		newDir:    "website/src/lib/content",
		redirects: "website/redirects.json",
	}
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, cfg.redirects), data, 0o644); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestPrint(t *testing.T) {
	rev := "1234abcd"
	tests := []struct {
		name string
		rep  report
		want string
	}{
		{
			name: "empty",
			want: "No drift: every Hugo page is mapped, and every rewrite is current.\n",
		},
		{
			name: "every section",
			rep: report{
				unmapped:   []string{"b.md"},
				oldMissing: []entry{{Old: "d.md", New: "y.md"}},
				changed: []change{{
					entry:   entry{Old: "a.md", New: "x.md", RewrittenFrom: &rev},
					commits: []string{"5678ef90 2026-01-03 Update a again", "90ab1234 2026-01-02 Update a"},
				}},
				newMissing: []entry{{Old: "c.md", New: "z.md"}},
			},
			want: `Hugo pages missing from website/redirects.json:
  b.md
Entries whose Hugo page no longer exists:
  d.md (replaced by y.md)
Hugo pages changed since they were rewritten:
  a.md, rewritten from 1234abcd as x.md:
    5678ef90 2026-01-03 Update a again
    90ab1234 2026-01-02 Update a
Entries whose website page doesn't exist:
  c.md (replaced by z.md)
`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			tc.rep.print(&b, "website/redirects.json")
			if diff := cmp.Diff(tc.want, b.String()); diff != "" {
				t.Errorf("print() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
