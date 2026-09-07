package printers

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTableAlignsColumns(t *testing.T) {
	var buf bytes.Buffer
	table := Table{
		Headers: []string{"NAME", "READY"},
		Rows:    [][]string{{"llama-70b", "True"}, {"m", "False"}},
	}
	require.NoError(t, table.Write(&buf))
	assert.Equal(t, "NAME        READY\nllama-70b   True\nm           False\n", buf.String())
}

func TestTableWrapsValuesWithinTerminalWidth(t *testing.T) {
	w := &terminalBuffer{width: 24, terminal: true}
	table := Table{
		Headers: []string{"NAME", "MESSAGE"},
		Rows: [][]string{{
			"engine", "deployment has minimum availability",
		}},
	}

	require.NoError(t, table.Write(w))
	assertPhysicalLinesWithin(t, w.String(), 24)
	assert.Equal(t, "NAME     MESSAGE\n"+
		"engine   deployment has\n"+
		"         minimum\n"+
		"         availability\n", w.String())
}

func TestTerminalTablesDoNotPadPresentEmptyFinalCells(t *testing.T) {
	t.Run("aligned", func(t *testing.T) {
		w := &terminalBuffer{width: 20, terminal: true}
		table := Table{
			Headers: []string{"NAME", "VALUE"},
			Rows:    [][]string{{"alpha", ""}},
		}

		require.NoError(t, table.Write(w))
		assert.Equal(t, "NAME    VALUE\nalpha\n", w.String())
	})

	t.Run("wrapped", func(t *testing.T) {
		w := &terminalBuffer{width: 12, terminal: true}
		table := Table{
			Headers: []string{"NAME", "VALUE"},
			Rows:    [][]string{{"alpha", ""}},
		}

		require.NoError(t, table.Write(w))
		assert.Equal(t, "NAME   VALUE\nalph\na\n", w.String())
	})

	t.Run("stacked", func(t *testing.T) {
		w := &terminalBuffer{width: 15, terminal: true}
		table := Table{
			Headers: []string{"FIRST", "SECOND", "THIRD"},
			Rows:    [][]string{{"abcdefghijklmnop", "", ""}},
		}

		require.NoError(t, table.Write(w))
		assert.Equal(t, "FIRST:\n"+
			"  abcdefghijklm\n"+
			"  nop\n"+
			"SECOND:\n"+
			"THIRD:\n", w.String())
	})

	t.Run("stacked with narrow labels", func(t *testing.T) {
		w := &terminalBuffer{width: 8, terminal: true}
		table := Table{
			Headers: []string{"LONGER", "OTHER"},
			Rows:    [][]string{{"", ""}},
		}

		require.NoError(t, table.Write(w))
		assert.Equal(t, "LONGER:\nOTHER:\n", w.String())
	})
}

func TestTableStacksRowsWhenHeadersCannotFit(t *testing.T) {
	w := &terminalBuffer{width: 30, terminal: true}
	table := Table{
		Headers: []string{"COMPONENT-STATE", "TARGET-EVIDENCE", "REPLICA-EVIDENCE"},
		Rows:    [][]string{{"Reported", "Reported", "Unavailable"}, {"Partial", "NotReported", "Invalid"}},
	}

	require.NoError(t, table.Write(w))
	assertPhysicalLinesWithin(t, w.String(), 30)
	assert.Equal(t, "COMPONENT-STATE:    Reported\n"+
		"TARGET-EVIDENCE:    Reported\n"+
		"REPLICA-EVIDENCE:\n"+
		"  Unavailable\n\n"+
		"COMPONENT-STATE:    Partial\n"+
		"TARGET-EVIDENCE:\n"+
		"  NotReported\n"+
		"REPLICA-EVIDENCE:   Invalid\n", w.String())
}

func TestStackedTablePreservesIndentedHeaders(t *testing.T) {
	w := &terminalBuffer{width: 14, terminal: true}
	table := Table{
		Headers: []string{"  TYPE", "OBJECT"},
		Rows:    [][]string{{"Reported", "Ready"}},
	}

	require.NoError(t, table.Write(w))
	assertPhysicalLinesWithin(t, w.String(), 14)
	assert.True(t, strings.HasPrefix(w.String(), "  TYPE:"), w.String())
}

func TestTableEscapesControlCharacters(t *testing.T) {
	w := &terminalBuffer{width: 80, terminal: true}
	table := Table{
		Headers: []string{"NAME", "VALUE"},
		Rows:    [][]string{{"unsafe", "tab\tline\nreturn\rescape\x1b"}},
	}

	require.NoError(t, table.Write(w))
	assert.NotContains(t, w.String(), "\t")
	assert.NotContains(t, w.String(), "\r")
	assert.NotContains(t, w.String(), "\x1b")
	assert.Contains(t, w.String(), `tab\tline\nreturn\rescape\u001b`)
}

func TestTableEscapesBidiControlsWithoutBreakingLegitimateFormatCharacters(t *testing.T) {
	w := &terminalBuffer{width: 240, terminal: true}
	table := Table{
		Headers: []string{"VALUE"},
		Rows: [][]string{{
			"before\u061c\u200e\u200f\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069after 👩‍💻 A\u200cB",
		}},
	}

	require.NoError(t, table.Write(w))
	assert.Contains(t, w.String(), `before\u061c\u200e\u200f\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069after`)
	assert.Contains(t, w.String(), "👩‍💻")
	assert.Contains(t, w.String(), "A\u200cB")
	for _, char := range []rune{'\u061c', '\u200e', '\u200f', '\u202a', '\u202b', '\u202c', '\u202d', '\u202e', '\u2066', '\u2067', '\u2068', '\u2069'} {
		assert.NotContains(t, w.String(), string(char))
	}
}

func TestTablePreservesLegacyNonTerminalCells(t *testing.T) {
	// Keep this unkeyed literal: Table historically had exactly two exported
	// fields, and adding another field would break existing callers at compile
	// time.
	table := Table{
		[]string{"NA\tME", "VALUE"},
		[][]string{{"unsafe\nname", "return\rvalue\x01"}},
	}
	var got bytes.Buffer
	var want bytes.Buffer

	require.NoError(t, table.Write(&got))
	require.NoError(t, writeLegacyTable(&want, table.Headers, table.Rows))
	assert.Equal(t, want.String(), got.String())
}

func TestTableAlignsTerminalCellsAtExactBoundary(t *testing.T) {
	w := &terminalBuffer{width: 6, terminal: true}
	table := Table{
		Headers: []string{"N", "V"},
		Rows: [][]string{
			{"AA", "x"},
			{"界", "x"},
			{"e\u0301", "x"},
		},
	}

	require.NoError(t, table.Write(w))
	assertPhysicalLinesWithin(t, w.String(), 6)
	assert.Equal(t, "N    V\nAA   x\n界   x\ne\u0301    x\n", w.String())
}

func TestTableMeasuresWideRunes(t *testing.T) {
	w := &terminalBuffer{width: 16, terminal: true}
	table := Table{
		Headers: []string{"NAME", "VALUE"},
		Rows:    [][]string{{"wide", "界界界🙂🙂🙂"}},
	}

	require.NoError(t, table.Write(w))
	assertPhysicalLinesWithin(t, w.String(), 16)
}

func TestDisplayWidthTreatsEmojiSequencesAsOneCluster(t *testing.T) {
	for value, want := range map[string]int{
		"⌚":               2,
		"☔":               2,
		"⚽":               2,
		"✅":               2,
		"⭐":               2,
		"🈀":               2,
		"☀️":              2,
		"1️⃣":             2,
		"🇺🇸":              2,
		"🏽":               2,
		"🏽\u0301":         2,
		"A🏽":              3,
		"☝🏽":              2,
		"☝🏽\u0301":        2,
		"☝🏽\u093e":        3,
		"☀🏽":              3,
		"👩‍💻":             2,
		"👩🏽‍💻":            2,
		"👩‍💻\u093e":       3,
		"e\u0301":         1,
		"का":              2,
		"\u302e":          2,
		"\U00016ff0":      2,
		"A\u200dB\u200dC": 3,
		"가":              2,
	} {
		assert.Equal(t, want, displayWidth(value), "display width of %q", value)
	}
	assert.Equal(t, []string{"👩‍💻", "x"}, wrapCell("👩‍💻x", 2))
	assert.Equal(t, []string{"का", "x"}, wrapCell("काx", 2))
	assert.Equal(t, []string{"가", "x"}, wrapCell("가x", 2))
	assert.Equal(t, []displayCluster{
		{value: "A\u200d", width: 1},
		{value: "B\u200d", width: 1},
		{value: "C", width: 1},
	}, displayClusters("A\u200dB\u200dC"))
}

func TestEmojiZWJSequencesRetainPositiveWidthExtendRunes(t *testing.T) {
	for _, test := range []struct {
		name     string
		value    string
		wantWrap []string
	}{
		{
			name:  "hangul single dot tone mark",
			value: "👩\u302e\u200d💻",
			wantWrap: []string{
				`\U`, `00`, `01`, `f4`, `69`, `\u`, `30`, `2e`,
				`\u`, `20`, `0d`, `\U`, `00`, `01`, `f4`, `bb`,
			},
		},
		{
			name:  "vietnamese alternate reading mark",
			value: "👩\U00016ff0\u200d💻",
			wantWrap: []string{
				`\U`, `00`, `01`, `f4`, `69`, `\U`, `00`, `01`, `6f`, `f0`,
				`\u`, `20`, `0d`, `\U`, `00`, `01`, `f4`, `bb`,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, 4, displayWidth(test.value))
			assert.Equal(t, test.wantWrap, wrapCell(test.value, 2))

			w := &terminalBuffer{width: 2, terminal: true}
			require.NoError(t, (Table{Headers: []string{"V"}, Rows: [][]string{{test.value}}}).Write(w))
			assert.Equal(t, "V\n"+strings.Join(test.wantWrap, "\n")+"\n", w.String())
		})
	}
}

func TestEmojiZWJSequencesStillConsumeZeroWidthAndPresentationSuffixes(t *testing.T) {
	for _, value := range []string{
		"👩\ufe0e\u200d💻",
		"👩\ufe0f\u200d💻",
		"👩🏽\u200d💻",
		"👩\u0301\u200d💻",
	} {
		assert.Equal(t, 2, displayWidth(value), "display width of %q", value)
		assert.Equal(t, []string{value}, wrapCell(value, 2))
	}
}

func TestRegionalIndicatorOddParityResetsAndToggles(t *testing.T) {
	properties := []graphemeProperty{
		graphemeOther,
		graphemeRegionalIndicator,
		graphemeRegionalIndicator,
		graphemeRegionalIndicator,
		graphemeOther,
		graphemeRegionalIndicator,
	}

	got := make([]bool, len(properties))
	odd := false
	for i, property := range properties {
		odd = nextRegionalIndicatorOddParity(odd, property)
		got[i] = odd
	}
	assert.Equal(t, []bool{false, true, false, true, false, true}, got)
}

func BenchmarkDisplayClustersRegionalIndicatorRun(b *testing.B) {
	value := strings.Repeat("🇦", 4096)
	b.ReportAllocs()
	for b.Loop() {
		allocationSink = wrapCell(value, 80)
	}
}

func TestDisplayWidthTreatsFormatPrependAsZeroWidth(t *testing.T) {
	for _, value := range []string{"\u0600", "\u06dd", "\u070f"} {
		assert.Zero(t, displayWidth(value), "display width of %q", value)
		assert.Equal(t, 1, displayWidth(value+"A"), "display width of %q", value+"A")
	}
	assert.Equal(t, 1, displayWidth("\u0d4e"), "visible Prepend characters retain their cell")
}

func TestDisplayWidthRecognizesSpecialCoreAfterPrepend(t *testing.T) {
	for value, want := range map[string]int{
		"\u0600👩‍💻": 2,
		"\u0600🇺🇸":  2,
		"\u06001️⃣": 2,
		"\u0d4e👩‍💻": 3,
	} {
		assert.Equal(t, want, displayWidth(value), "display width of %q", value)
	}
}

func TestDisplayWidthRetainsSuffixesAfterSpecialSequences(t *testing.T) {
	for value, want := range map[string]int{
		"1️⃣\u0301": 2,
		"1️⃣\u093e": 3,
		"©\u093e":   2,
		"©️\u093e":  3,
	} {
		assert.Equal(t, want, displayWidth(value), "display width of %q", value)
	}

	for _, value := range []string{"1️⃣\u0301", "1️⃣\u093e", "©\u093e", "©️\u093e"} {
		segments := wrapCell(value, 1)
		for _, segment := range segments {
			assert.LessOrEqual(t, displayWidth(segment), 1, "segment %q for %q", segment, value)
		}
		quoted := strconv.QuoteToASCII(value)
		assert.Equal(t, quoted[1:len(quoted)-1], strings.Join(segments, ""))
	}
}

func TestWideClusterAtWidthOneUsesASCIIEscapes(t *testing.T) {
	for _, value := range []string{"⌚", "🏽", "☝🏽", "\u302e", "\U00016ff0", "가"} {
		segments := wrapCell(value, 1)
		for _, segment := range segments {
			assert.Len(t, []byte(segment), 1, "segment %q for %q", segment, value)
			assert.Less(t, segment[0], byte(0x80), "segment %q for %q", segment, value)
		}
		assert.Equal(t, strconv.QuoteToASCII(value)[1:len(strconv.QuoteToASCII(value))-1], strings.Join(segments, ""))
	}
}

func TestHardWrapPreservesLeadingZeroWidthClusterOrder(t *testing.T) {
	assert.Equal(t, "\u0301\\u754c", strings.Join(wrapCell("\u0301界", 1), ""))
}

func TestMalformedUTF8AndZeroWidthOnlyClustersRemainBounded(t *testing.T) {
	malformed := string([]byte{'a', 0xff, 'b'})
	clean := sanitizeCell(malformed)
	assert.True(t, utf8.ValidString(clean))
	assert.Equal(t, "a\ufffdb", clean)

	zeroWidth := "\u200b\u2060"
	assert.Zero(t, displayWidth(zeroWidth))
	assert.Equal(t, []string{zeroWidth}, wrapCell(zeroWidth, 1))

	w := &terminalBuffer{width: 1, terminal: true}
	require.NoError(t, (Table{Headers: []string{"V"}, Rows: [][]string{{malformed}, {zeroWidth}}}).Write(w))
	assert.True(t, utf8.ValidString(w.String()))
	assertPhysicalLinesWithin(t, w.String(), 1)
}

func TestUnicode17GraphemeBreakConformance(t *testing.T) {
	file, err := os.Open("testdata/GraphemeBreakTest-17.0.0.txt")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	caseCount := 0
	for scanner.Scan() {
		lineNumber++
		definition := strings.TrimSpace(strings.SplitN(scanner.Text(), "#", 2)[0])
		if definition == "" {
			continue
		}
		fields := strings.Fields(definition)
		require.GreaterOrEqual(t, len(fields), 3, "line %d", lineNumber)

		var value strings.Builder
		want := []string{}
		var cluster strings.Builder
		for index := 1; index < len(fields); index += 2 {
			codePoint, parseErr := strconv.ParseInt(fields[index], 16, 32)
			require.NoError(t, parseErr, "line %d", lineNumber)
			if fields[index-1] == "÷" && cluster.Len() > 0 {
				want = append(want, cluster.String())
				cluster.Reset()
			}
			cluster.WriteRune(rune(codePoint))
			value.WriteRune(rune(codePoint))
		}
		if cluster.Len() > 0 {
			want = append(want, cluster.String())
		}

		gotClusters := displayClusters(value.String())
		got := make([]string, len(gotClusters))
		for i := range gotClusters {
			got[i] = gotClusters[i].value
		}
		assert.Equal(t, want, got, "Unicode conformance line %d", lineNumber)
		caseCount++
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, 766, caseCount)
}

func TestTableBoundsTinyTerminalWidths(t *testing.T) {
	for width := 1; width <= 3; width++ {
		t.Run(fmt.Sprintf("width-%d", width), func(t *testing.T) {
			w := &terminalBuffer{width: width, terminal: true}
			table := Table{Headers: []string{"界"}, Rows: [][]string{{"🙂"}}}

			require.NoError(t, table.Write(w))
			assertPhysicalLinesWithin(t, w.String(), width)
		})
	}
}

func TestTableRepeatsLeadingIndentOnWrappedCells(t *testing.T) {
	w := &terminalBuffer{width: 18, terminal: true}
	table := Table{
		Headers: []string{"  POD", "READY"},
		Rows:    [][]string{{"  long-pod-identifier", "True"}},
	}

	require.NoError(t, table.Write(w))
	assertPhysicalLinesWithin(t, w.String(), 18)
	lines := strings.Split(strings.TrimSuffix(w.String(), "\n"), "\n")
	require.GreaterOrEqual(t, len(lines), 3)
	assert.True(t, strings.HasPrefix(lines[1], "  "))
	assert.True(t, strings.HasPrefix(lines[2], "  "))
	for _, line := range lines {
		assert.False(t, strings.HasSuffix(line, " "), "line %q has trailing spaces", line)
	}
}

func TestWrapCellPrefersIdentifierBoundaries(t *testing.T) {
	assert.Equal(t,
		[]string{"kome-tree-b4d9098-", "collision"},
		wrapCell("kome-tree-b4d9098-collision", 20),
	)
	assert.Equal(t,
		[]string{"deployment.apps/", "engine"},
		wrapCell("deployment.apps/engine", 17),
	)
}

func TestWrapCellKeepsIndentedTreeBranchesWithTheirIdentity(t *testing.T) {
	for _, branch := range []string{"`--", "|--"} {
		t.Run(branch, func(t *testing.T) {
			const width = 24
			segments := wrapCell(
				"    "+branch+" ClusterServingRuntime/"+strings.Repeat("a", 80),
				width,
			)

			require.Greater(t, len(segments), 1)
			assert.True(t,
				strings.HasPrefix(segments[0], "    "+branch+" C"),
				"branch must share its first line with the identity: %q",
				segments[0],
			)
			for _, segment := range segments {
				assert.LessOrEqual(t, displayWidth(segment), width)
				assert.NotEqual(t, "    "+branch, segment)
			}
		})
	}
}

func TestTreeBranchWrappingLeavesUnrelatedWhitespaceTokensUnchanged(t *testing.T) {
	for _, value := range []string{
		"alpha  beta",
		"prefix `-- identity",
		"prefix |-- identity",
		"`--identity",
	} {
		t.Run(value, func(t *testing.T) {
			tokens := splitWhitespaceTokens(value)
			assert.Equal(t, tokens, joinTreeBranchIdentity(tokens))
		})
	}
}

func TestSplitIdentifierDoesNotSplitDelimiterGrapheme(t *testing.T) {
	assert.Equal(t,
		[]string{"runtime-\u0301name/", "leaf"},
		splitIdentifier("runtime-\u0301name/leaf"),
	)
}

func TestWrapCellPreservesWhitespaceRuns(t *testing.T) {
	assert.Equal(t, []string{"alpha  beta"}, wrapCell("alpha  beta", 20))
	assert.Equal(t, []string{"alpha", " beta"}, wrapCell("alpha  beta", 7))
	assert.Equal(t, []string{"  alpha", "   beta"}, wrapCell("  alpha  beta", 8))
	assert.Equal(t, "alpha  beta", strings.Join(wrapCell("alpha  beta", 7), " "))
}

func TestWrapCellAllocationGrowthIsLinear(t *testing.T) {
	for _, test := range []struct {
		name  string
		value func(int) string
	}{
		{name: "identifier", value: func(size int) string { return strings.Repeat("a", size) }},
		{name: "leading whitespace", value: func(size int) string { return strings.Repeat(" ", size) + "x" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			const smallSize = 2048
			small := allocatedBytes(func() { allocationSink = wrapCell(test.value(smallSize), 16) })
			large := allocatedBytes(func() { allocationSink = wrapCell(test.value(2*smallSize), 16) })

			assert.Less(t, large, 3*small,
				"doubling input allocated %d bytes after %d bytes for the smaller input", large, small)
		})
	}
}

func TestWrappedTablePreservesIntentionalTrailingWhitespace(t *testing.T) {
	w := &terminalBuffer{width: 5, terminal: true}
	table := Table{Headers: []string{"VALUE"}, Rows: [][]string{{"value  "}}}

	require.NoError(t, table.Write(w))
	assert.Equal(t, "VALUE\nvalue\n  \n", w.String())
}

func TestAdaptiveTableReturnsWriterErrors(t *testing.T) {
	wantErr := errors.New("destination closed")
	table := Table{
		Headers: []string{"FIRST-LONG-HEADER", "SECOND-LONG-HEADER"},
		Rows:    [][]string{{"alpha", "beta"}},
	}

	err := table.Write(terminalFailureWriter{width: 20, err: wantErr})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

func TestAdaptiveTableReturnsShortWrite(t *testing.T) {
	table := Table{
		Headers: []string{"FIRST-LONG-HEADER", "SECOND-LONG-HEADER"},
		Rows:    [][]string{{"alpha", "beta"}},
	}

	err := table.Write(terminalFailureWriter{width: 20, short: true})

	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrShortWrite)
}

func TestAdaptiveTableDoesNotMutateInput(t *testing.T) {
	w := &terminalBuffer{width: 20, terminal: true}
	table := Table{
		Headers: []string{"FIRST-LONG-HEADER", "SECOND-LONG-HEADER"},
		Rows:    [][]string{{"unsafe\nname", "a long value that wraps"}},
	}

	require.NoError(t, table.Write(w))
	assert.Equal(t, []string{"FIRST-LONG-HEADER", "SECOND-LONG-HEADER"}, table.Headers)
	assert.Equal(t, [][]string{{"unsafe\nname", "a long value that wraps"}}, table.Rows)
}

func TestPrintObjJSONRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}}
	require.NoError(t, PrintObj(pod, "json", &buf))
	assert.Contains(t, buf.String(), `"name": "p"`)
}

func TestPrintObjRejectsUnknownFormat(t *testing.T) {
	assert.Error(t, PrintObj(&corev1.Pod{}, "toml", &bytes.Buffer{}))
}

func TestPrintObjReturnsShortWrite(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			err := PrintObj(
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod"}},
				format,
				terminalFailureWriter{short: true},
			)

			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrShortWrite)
		})
	}
}

func TestAge(t *testing.T) {
	assert.Equal(t, "10m", Age(metav1.NewTime(time.Now().Add(-10*time.Minute))))
}

func TestOrDash(t *testing.T) {
	assert.Equal(t, "-", OrDash(""))
	assert.Equal(t, "x", OrDash("x"))
}

type terminalBuffer struct {
	bytes.Buffer
	width    int
	terminal bool
}

var allocationSink []string

func allocatedBytes(action func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	action()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

type terminalFailureWriter struct {
	width int
	err   error
	short bool
}

func (w terminalFailureWriter) TerminalWidth() (int, bool) {
	return w.width, true
}

func (w terminalFailureWriter) Write(value []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.short {
		return len(value) - 1, nil
	}
	return len(value), nil
}

func (w *terminalBuffer) TerminalWidth() (int, bool) {
	return w.width, w.terminal
}

func assertPhysicalLinesWithin(t *testing.T, output string, width int) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		assert.LessOrEqual(t, displayWidth(line), width, "line %q", line)
	}
}

func writeLegacyTable(w io.Writer, headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 8, tablePadding, ' ', 0)
	if _, err := fmt.Fprintln(tw, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(tw, strings.Join(row, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}
