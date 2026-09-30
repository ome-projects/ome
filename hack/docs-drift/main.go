// Command docs-drift reports where the Hugo documentation in site/ has
// drifted from its rewrite in website/.
//
// website/redirects.json maps every Hugo page to the website page that
// replaces it, with rewrittenFrom: the last commit that changed the Hugo
// page when the website page was written, or null while the website page
// is a draft. docs-drift reports:
//
//   - Hugo pages missing from redirects.json;
//   - entries whose Hugo page no longer exists;
//   - entries whose Hugo page changed after rewrittenFrom, unless the
//     website page has generated: true in its front matter;
//   - entries whose website page doesn't exist.
//
// It exits 0 when there is nothing to report, 1 when there is, and 2 when
// it can't run.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run runs the command with args, the arguments after the program name,
// and returns its exit code.
func run(args []string, stdout, stderr io.Writer) int {
	var cfg config
	flags := flag.NewFlagSet("docs-drift", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.repo, "repo", ".", "`directory` of the git repository")
	flags.StringVar(&cfg.oldDir, "old", "site/content/en/docs", "`directory` of the Hugo pages, relative to -repo")
	flags.StringVar(&cfg.newDir, "new", "website/src/lib/content", "`directory` of the website pages, relative to -repo")
	flags.StringVar(&cfg.redirects, "redirects", "website/redirects.json", "redirects `file`, relative to -repo")
	if err := flags.Parse(args); err != nil {
		// Parse has printed the error, or the usage for -h.
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	rep, err := drift(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "docs-drift: %v\n", err)
		return 2
	}
	rep.print(stdout, cfg.redirects)
	if !rep.empty() {
		return 1
	}
	return 0
}

type config struct {
	repo      string
	oldDir    string
	newDir    string
	redirects string
}

// An entry is one row of redirects.json.
type entry struct {
	Old           string  `json:"old"`
	New           string  `json:"new"`
	RewrittenFrom *string `json:"rewrittenFrom"`
}

// A change is a Hugo page that changed after it was rewritten.
type change struct {
	entry   entry
	commits []string // "hash date subject", newest first
}

// A report lists the drift between the Hugo pages and their rewrites.
type report struct {
	unmapped   []string // Hugo pages with no entry
	oldMissing []entry  // entries whose Hugo page no longer exists
	changed    []change
	newMissing []entry // entries whose website page doesn't exist
}

func (r *report) empty() bool {
	return len(r.unmapped)+len(r.oldMissing)+len(r.changed)+len(r.newMissing) == 0
}

// print writes the report to w. redirects names the redirects file.
func (r *report) print(w io.Writer, redirects string) {
	if r.empty() {
		fmt.Fprintln(w, "No drift: every Hugo page is mapped, and every rewrite is current.")
		return
	}
	if len(r.unmapped) > 0 {
		fmt.Fprintf(w, "Hugo pages missing from %s:\n", redirects)
		for _, p := range r.unmapped {
			fmt.Fprintf(w, "  %s\n", p)
		}
	}
	if len(r.oldMissing) > 0 {
		fmt.Fprintln(w, "Entries whose Hugo page no longer exists:")
		for _, e := range r.oldMissing {
			fmt.Fprintf(w, "  %s (replaced by %s)\n", e.Old, e.New)
		}
	}
	if len(r.changed) > 0 {
		fmt.Fprintln(w, "Hugo pages changed since they were rewritten:")
		for _, c := range r.changed {
			fmt.Fprintf(w, "  %s, rewritten from %s as %s:\n", c.entry.Old, *c.entry.RewrittenFrom, c.entry.New)
			for _, commit := range c.commits {
				fmt.Fprintf(w, "    %s\n", commit)
			}
		}
	}
	if len(r.newMissing) > 0 {
		fmt.Fprintln(w, "Entries whose website page doesn't exist:")
		for _, e := range r.newMissing {
			fmt.Fprintf(w, "  %s (replaced by %s)\n", e.Old, e.New)
		}
	}
}

// commitHash matches an abbreviated or full commit hash.
var commitHash = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// drift compares the Hugo pages with the redirects file and the website
// pages.
func drift(cfg config) (*report, error) {
	data, err := os.ReadFile(filepath.Join(cfg.repo, cfg.redirects))
	if err != nil {
		return nil, err
	}
	var entries []entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse %s: %w", cfg.redirects, err)
	}
	oldPages, err := listPages(filepath.Join(cfg.repo, cfg.oldDir))
	if err != nil {
		return nil, err
	}

	rep := &report{}
	mapped := make(map[string]bool)
	for _, e := range entries {
		mapped[e.Old] = true
		newPage := filepath.Join(cfg.repo, cfg.newDir, filepath.FromSlash(e.New))
		switch {
		case !oldPages[e.Old]:
			rep.oldMissing = append(rep.oldMissing, e)
		case e.RewrittenFrom != nil:
			// A generated page, such as the API reference, is built from
			// another source, so a change to its Hugo page doesn't make it
			// stale. A missing page is reported below.
			gen, err := generated(newPage)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			if gen {
				break
			}
			commits, err := commitsSince(cfg.repo, *e.RewrittenFrom, path.Join(filepath.ToSlash(cfg.oldDir), e.Old))
			if err != nil {
				return nil, fmt.Errorf("%s: %w", e.Old, err)
			}
			if len(commits) > 0 {
				rep.changed = append(rep.changed, change{entry: e, commits: commits})
			}
		}
		if _, err := os.Stat(newPage); errors.Is(err, fs.ErrNotExist) {
			rep.newMissing = append(rep.newMissing, e)
		} else if err != nil {
			return nil, err
		}
	}
	for _, p := range slices.Sorted(maps.Keys(oldPages)) {
		if !mapped[p] {
			rep.unmapped = append(rep.unmapped, p)
		}
	}
	return rep, nil
}

// listPages returns the Markdown files under dir, as slash-separated paths
// relative to dir.
func listPages(dir string) (map[string]bool, error) {
	pages := make(map[string]bool)
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".md" {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		pages[filepath.ToSlash(rel)] = true
		return nil
	})
	return pages, err
}

// lineBreaks converts CRLF and CR line breaks to LF.
var lineBreaks = strings.NewReplacer("\r\n", "\n", "\r", "\n")

// frontMatter matches the front matter at the start of a website page:
// YAML between two --- lines, as the website reads it.
var frontMatter = regexp.MustCompile(`(?s)^---\n(.*?)\n---(?:\n|$)`)

// generated reports whether the website page in file has generated: true
// in its front matter. A page with no front matter isn't generated.
func generated(file string) (bool, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return false, err
	}
	m := frontMatter.FindStringSubmatch(lineBreaks.Replace(string(data)))
	if m == nil {
		return false, nil
	}
	var fields map[string]any
	if err := yaml.Unmarshal([]byte(m[1]), &fields); err != nil {
		return false, fmt.Errorf("%s: invalid front matter: %w", file, err)
	}
	v, ok := fields["generated"]
	if !ok {
		return false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf(`%s: front matter "generated" must be true or false`, file)
	}
	return b, nil
}

// commitsSince returns the commits after rev that changed file, newest
// first, as "hash date subject". file is relative to the repository.
func commitsSince(repo, rev, file string) ([]string, error) {
	if !commitHash.MatchString(rev) {
		return nil, fmt.Errorf("rewrittenFrom %q isn't a commit hash", rev)
	}
	cmd := exec.Command("git", "-C", repo, "log", "--date=short", "--format=%h %ad %s", rev+"..HEAD", "--", file)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log %s..HEAD: %v: %s", rev, err, strings.TrimSpace(stderr.String()))
	}
	if len(bytes.TrimSpace(out)) == 0 {
		return nil, nil
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n"), nil
}
