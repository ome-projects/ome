package printers

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"
)

const tablePadding = 3

func writeTable(w io.Writer, headers []string, rows [][]string, sanitizeNonTerminal bool) error {
	width, terminal := TerminalWidth(w)
	if !terminal {
		if sanitizeNonTerminal {
			headers, rows = sanitizedTable(headers, rows)
		}
		return writeAlignedTable(w, headers, rows)
	}
	cleanHeaders, cleanRows := sanitizedTable(headers, rows)

	natural := columnWidths(cleanHeaders, cleanRows)
	if tableWidth(natural) <= width {
		return writeTerminalAlignedTable(w, cleanHeaders, cleanRows, natural)
	}

	minimum := headerWidths(cleanHeaders, len(natural))
	if tableWidth(minimum) <= width {
		return writeWrappedTable(w, cleanHeaders, cleanRows, growWidths(minimum, natural, width))
	}
	return writeStackedTable(w, cleanHeaders, cleanRows, width)
}

func writeTerminalAlignedTable(w io.Writer, headers []string, rows [][]string, widths []int) error {
	var output strings.Builder
	writeTerminalAlignedRow(&output, headers, widths)
	for _, row := range rows {
		writeTerminalAlignedRow(&output, row, widths)
	}
	return writeString(w, output.String())
}

func writeTerminalAlignedRow(output *strings.Builder, row []string, widths []int) {
	var line strings.Builder
	contentEnd := 0
	for column, width := range widths {
		value := ""
		if column < len(row) {
			value = row[column]
		}
		line.WriteString(value)
		if column < len(row) && value != "" {
			contentEnd = line.Len()
		}
		if column < len(widths)-1 {
			line.WriteString(strings.Repeat(" ", width-displayWidth(value)+tablePadding))
		}
	}
	output.WriteString(line.String()[:contentEnd])
	output.WriteByte('\n')
}

func sanitizedTable(headers []string, rows [][]string) ([]string, [][]string) {
	cleanHeaders := make([]string, len(headers))
	for i, header := range headers {
		cleanHeaders[i] = sanitizeCell(header)
	}
	cleanRows := make([][]string, len(rows))
	for i, row := range rows {
		cleanRows[i] = make([]string, len(row))
		for j, cell := range row {
			cleanRows[i][j] = sanitizeCell(cell)
		}
	}
	return cleanHeaders, cleanRows
}

func writeAlignedTable(w io.Writer, headers []string, rows [][]string) error {
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

func columnWidths(headers []string, rows [][]string) []int {
	count := len(headers)
	for _, row := range rows {
		if len(row) > count {
			count = len(row)
		}
	}
	widths := make([]int, count)
	for i, header := range headers {
		widths[i] = displayWidth(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			if width := displayWidth(cell); width > widths[i] {
				widths[i] = width
			}
		}
	}
	return widths
}

func headerWidths(headers []string, count int) []int {
	widths := make([]int, count)
	for i := range widths {
		widths[i] = 1
		if i < len(headers) && displayWidth(headers[i]) > widths[i] {
			widths[i] = displayWidth(headers[i])
		}
	}
	return widths
}

func tableWidth(widths []int) int {
	if len(widths) == 0 {
		return 0
	}
	total := tablePadding * (len(widths) - 1)
	for _, width := range widths {
		total += width
	}
	return total
}

func growWidths(widths, natural []int, limit int) []int {
	result := append([]int{}, widths...)
	remaining := limit - tableWidth(result)
	for remaining > 0 {
		grew := false
		for i := range result {
			if remaining == 0 {
				break
			}
			if result[i] < natural[i] {
				result[i]++
				remaining--
				grew = true
			}
		}
		if !grew {
			break
		}
	}
	return result
}

func writeWrappedTable(w io.Writer, headers []string, rows [][]string, widths []int) error {
	var output strings.Builder
	writeWrappedRow(&output, headers, widths)
	for _, row := range rows {
		writeWrappedRow(&output, row, widths)
	}
	return writeString(w, output.String())
}

func writeWrappedRow(output *strings.Builder, row []string, widths []int) {
	wrapped := make([][]string, len(widths))
	height := 1
	for i, width := range widths {
		cell := ""
		if i < len(row) {
			cell = row[i]
		}
		wrapped[i] = wrapCell(cell, width)
		if len(wrapped[i]) > height {
			height = len(wrapped[i])
		}
	}
	for line := 0; line < height; line++ {
		var physicalLine strings.Builder
		contentEnd := 0
		for column, width := range widths {
			value := ""
			if line < len(wrapped[column]) {
				value = wrapped[column][line]
			}
			physicalLine.WriteString(value)
			if line < len(wrapped[column]) && value != "" {
				contentEnd = physicalLine.Len()
			}
			if column < len(widths)-1 {
				physicalLine.WriteString(strings.Repeat(" ", width-displayWidth(value)+tablePadding))
			}
		}
		output.WriteString(physicalLine.String()[:contentEnd])
		output.WriteByte('\n')
	}
}

func writeStackedTable(w io.Writer, headers []string, rows [][]string, width int) error {
	var output strings.Builder
	for rowIndex, row := range rows {
		if rowIndex > 0 {
			output.WriteByte('\n')
		}
		writeStackedRow(&output, headers, row, width)
	}
	if len(rows) == 0 {
		writeStackedRow(&output, headers, nil, width)
	}
	return writeString(w, output.String())
}

func writeStackedRow(output *strings.Builder, headers, row []string, width int) {
	count := len(headers)
	if len(row) > count {
		count = len(row)
	}
	labels := make([]string, count)
	labelWidth := 0
	for i := range labels {
		if i < len(headers) {
			labels[i] = headers[i]
		}
		if strings.TrimSpace(labels[i]) == "" {
			labels[i] = fmt.Sprintf("COLUMN-%d", i+1)
		}
		if measured := displayWidth(labels[i]); measured > labelWidth {
			labelWidth = measured
		}
	}

	for i, label := range labels {
		value := ""
		if i < len(row) {
			value = row[i]
		}
		prefixWidth := labelWidth + 1 + tablePadding
		if prefixWidth < width {
			prefix := label + ":" + strings.Repeat(" ", labelWidth-displayWidth(label)+tablePadding)
			if value == "" {
				output.WriteString(strings.TrimRight(prefix, " "))
				output.WriteByte('\n')
				continue
			}
			if displayWidth(value) > width-prefixWidth {
				output.WriteString(strings.TrimRight(prefix, " "))
				output.WriteByte('\n')
				for _, segment := range wrapCell(value, width-2) {
					output.WriteString("  ")
					output.WriteString(segment)
					output.WriteByte('\n')
				}
				continue
			}
			output.WriteString(prefix)
			output.WriteString(value)
			output.WriteByte('\n')
			continue
		}

		for _, segment := range wrapCell(label+":", width) {
			output.WriteString(segment)
			output.WriteByte('\n')
		}
		if value == "" {
			continue
		}
		indent := 2
		if width <= indent {
			indent = 0
		}
		for _, segment := range wrapCell(value, width-indent) {
			output.WriteString(strings.Repeat(" ", indent))
			output.WriteString(segment)
			output.WriteByte('\n')
		}
	}
}

func wrapCell(value string, width int) []string {
	if value == "" {
		return []string{""}
	}
	if width < 1 {
		width = 1
	}
	if displayWidth(value) <= width {
		return []string{value}
	}
	indentLength := len(value) - len(strings.TrimLeft(value, " "))
	if indentLength > 0 && indentLength < len(value) && indentLength < width {
		indent := value[:indentLength]
		segments := wrapCell(strings.TrimLeft(value, " "), width-indentLength)
		for i := range segments {
			segments[i] = indent + segments[i]
		}
		return segments
	}
	tokens := splitWhitespaceTokens(value)
	if len(tokens) > 1 {
		return wrapWhitespaceTokens(joinTreeBranchIdentity(tokens), width)
	}
	return wrapIdentifier(value, width)
}

// joinTreeBranchIdentity keeps an ASCII tree branch and its following
// identity in the same wrap token. Identifier-aware wrapping can then split
// the combined token after the identity's kind separator instead of emitting
// a visually detached `-- or |-- line.
func joinTreeBranchIdentity(tokens []whitespaceToken) []whitespaceToken {
	if len(tokens) < 3 || tokens[0].whitespace || !tokens[1].whitespace || tokens[2].whitespace {
		return tokens
	}
	if tokens[0].value != "`--" && tokens[0].value != "|--" {
		return tokens
	}
	joined := make([]whitespaceToken, 0, len(tokens)-2)
	joined = append(joined, whitespaceToken{
		value: tokens[0].value + tokens[1].value + tokens[2].value,
		width: tokens[0].width + tokens[1].width + tokens[2].width,
	})
	return append(joined, tokens[3:]...)
}

type whitespaceToken struct {
	value      string
	whitespace bool
	width      int
}

func splitWhitespaceTokens(value string) []whitespaceToken {
	clusters := displayClusters(value)
	if len(clusters) == 0 {
		return nil
	}

	tokens := make([]whitespaceToken, 0, len(clusters))
	var token strings.Builder
	whitespace := clusterStartsWithSpace(clusters[0].value)
	tokenWidth := 0
	for _, cluster := range clusters {
		space := clusterStartsWithSpace(cluster.value)
		if space != whitespace {
			tokens = append(tokens, whitespaceToken{
				value: token.String(), whitespace: whitespace, width: tokenWidth,
			})
			token = strings.Builder{}
			whitespace = space
			tokenWidth = 0
		}
		token.WriteString(cluster.value)
		tokenWidth += cluster.width
	}
	tokens = append(tokens, whitespaceToken{
		value: token.String(), whitespace: whitespace, width: tokenWidth,
	})
	return tokens
}

func clusterStartsWithSpace(value string) bool {
	for _, char := range value {
		return unicode.IsSpace(char)
	}
	return false
}

func wrapWhitespaceTokens(tokens []whitespaceToken, width int) []string {
	result := []string{}
	var line strings.Builder
	lineWidth := 0
	pendingWhitespace := ""
	pendingWhitespaceWidth := 0

	flushLine := func() {
		if line.Len() != 0 {
			result = append(result, line.String())
			line = strings.Builder{}
			lineWidth = 0
		}
	}

	for _, token := range tokens {
		if token.whitespace {
			pendingWhitespace = token.value
			pendingWhitespaceWidth = token.width
			continue
		}

		if lineWidth+pendingWhitespaceWidth+token.width <= width {
			line.WriteString(pendingWhitespace)
			line.WriteString(token.value)
			lineWidth += pendingWhitespaceWidth + token.width
			pendingWhitespace = ""
			pendingWhitespaceWidth = 0
			continue
		}

		hadLine := line.Len() != 0
		flushLine()
		if hadLine {
			pendingWhitespace = trimFirstDisplayCluster(pendingWhitespace)
			pendingWhitespaceWidth = displayWidth(pendingWhitespace)
		}
		line.WriteString(pendingWhitespace)
		lineWidth = pendingWhitespaceWidth
		pendingWhitespace = ""
		pendingWhitespaceWidth = 0
		if lineWidth >= width {
			parts := wrapCellHard(line.String(), width)
			line = strings.Builder{}
			last := len(parts) - 1
			if displayWidth(parts[last]) == width {
				result = append(result, parts...)
				lineWidth = 0
			} else {
				result = append(result, parts[:last]...)
				line.WriteString(parts[last])
				lineWidth = displayWidth(parts[last])
			}
		}

		available := width - lineWidth
		if available == 0 {
			flushLine()
			available = width
		}
		parts := wrapIdentifier(token.value, available)
		line.WriteString(parts[0])
		lineWidth += displayWidth(parts[0])
		if len(parts) > 1 {
			result = append(result, line.String())
			result = append(result, parts[1:len(parts)-1]...)
			line = strings.Builder{}
			line.WriteString(parts[len(parts)-1])
			lineWidth = displayWidth(parts[len(parts)-1])
		}
	}

	if pendingWhitespace != "" {
		if lineWidth+pendingWhitespaceWidth <= width {
			line.WriteString(pendingWhitespace)
		} else {
			flushLine()
			parts := wrapCellHard(pendingWhitespace, width)
			result = append(result, parts[:len(parts)-1]...)
			line.WriteString(parts[len(parts)-1])
		}
	}
	if line.Len() != 0 || len(result) == 0 {
		result = append(result, line.String())
	}
	return result
}

func trimFirstDisplayCluster(value string) string {
	clusters := displayClusters(value)
	if len(clusters) == 0 {
		return value
	}
	return strings.TrimPrefix(value, clusters[0].value)
}

func wrapIdentifier(value string, width int) []string {
	if displayWidth(value) <= width {
		return []string{value}
	}

	segments := splitIdentifier(value)
	if len(segments) == 1 {
		return wrapCellHard(value, width)
	}

	var result []string
	var line strings.Builder
	lineWidth := 0
	for _, segment := range segments {
		segmentWidth := displayWidth(segment)
		if segmentWidth > width {
			if line.Len() != 0 {
				result = append(result, line.String())
				line = strings.Builder{}
			}
			parts := wrapCellHard(segment, width)
			result = append(result, parts[:len(parts)-1]...)
			line.WriteString(parts[len(parts)-1])
			lineWidth = displayWidth(parts[len(parts)-1])
			continue
		}
		if line.Len() == 0 || lineWidth+segmentWidth <= width {
			line.WriteString(segment)
			lineWidth += segmentWidth
			continue
		}
		result = append(result, line.String())
		line = strings.Builder{}
		line.WriteString(segment)
		lineWidth = segmentWidth
	}
	if line.Len() != 0 {
		result = append(result, line.String())
	}
	return result
}

func splitIdentifier(value string) []string {
	var segments []string
	var segment strings.Builder
	for _, prefix := range []string{"`-- ", "|-- "} {
		if strings.HasPrefix(value, prefix) {
			segment.WriteString(prefix)
			value = strings.TrimPrefix(value, prefix)
			break
		}
	}
	for _, cluster := range displayClusters(value) {
		segment.WriteString(cluster.value)
		switch cluster.value {
		case "-", "_", "/", ".", ":":
			segments = append(segments, segment.String())
			segment.Reset()
		}
	}
	if segment.Len() > 0 {
		segments = append(segments, segment.String())
	}
	return segments
}

func wrapCellHard(value string, width int) []string {
	result := []string{}
	var line strings.Builder
	lineWidth := 0
	for _, cluster := range displayClusters(value) {
		if cluster.width > width {
			if line.Len() > 0 {
				result = append(result, line.String())
				line.Reset()
				lineWidth = 0
			}
			quoted := strconv.QuoteToASCII(cluster.value)
			result = append(result, wrapCellHard(quoted[1:len(quoted)-1], width)...)
			continue
		}
		if lineWidth > 0 && lineWidth+cluster.width > width {
			result = append(result, line.String())
			line.Reset()
			lineWidth = 0
		}
		line.WriteString(cluster.value)
		lineWidth += cluster.width
	}
	if line.Len() > 0 || len(result) == 0 {
		result = append(result, line.String())
	}
	return result
}

func displayWidth(value string) int {
	width := 0
	for _, cluster := range displayClusters(value) {
		width += cluster.width
	}
	return width
}

type displayCluster struct {
	value string
	width int
}

func displayClusters(value string) []displayCluster {
	return segmentDisplayClusters(value)
}

func isEmojiModifier(char rune) bool {
	return char >= 0x1f3fb && char <= 0x1f3ff
}

// isWideRune recognizes Unicode 17 code points with a default terminal width
// of two cells. The ranges are generated from EastAsianWidth.txt together with
// the default emoji-presentation assignments. displayClusters handles joined
// and presentation-selected emoji as one visible cluster.
func isWideRune(char rune) bool {
	low, high := 0, len(doubleWidthRanges)
	for low < high {
		middle := low + (high-low)/2
		interval := doubleWidthRanges[middle]
		if char < interval.first {
			high = middle
			continue
		}
		if char > interval.last {
			low = middle + 1
			continue
		}
		return true
	}
	return false
}

func sanitizeCell(value string) string {
	var result strings.Builder
	for _, char := range value {
		switch char {
		case '\t':
			result.WriteString(`\t`)
		case '\n':
			result.WriteString(`\n`)
		case '\r':
			result.WriteString(`\r`)
		default:
			if unicode.IsControl(char) || isBidiControl(char) {
				fmt.Fprintf(&result, `\u%04x`, char)
				continue
			}
			result.WriteRune(char)
		}
	}
	return result.String()
}

func isBidiControl(char rune) bool {
	return char == '\u061c' ||
		(char >= '\u200e' && char <= '\u200f') ||
		(char >= '\u202a' && char <= '\u202e') ||
		(char >= '\u2066' && char <= '\u2069')
}

func writeString(w io.Writer, value string) error {
	written, err := io.WriteString(w, value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
}
