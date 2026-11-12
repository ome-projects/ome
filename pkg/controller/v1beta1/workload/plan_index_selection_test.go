package workload

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var updateIndexSelection = flag.Bool("update-index-selection", false, "re-record testdata/index_selection_pictures.json and testdata/index_selection_allocations.json from this run")

// indexPicture is one input of the plan's index selection: the rows a
// Component records, the count it is asked for, the retry blocks, the
// Instances on a cordoned node and the revision the roll converges to.
type indexPicture struct {
	rows       []types.InstanceStatus
	replicas   int32
	blocks     []types.RetryBlock
	cordoned   map[int32]struct{}
	rollTarget string
}

// chosen is the selection's answer on the picture.
func (pic indexPicture) chosen() []int32 {
	return instancePlanIndices(pic.rows, pic.replicas, pic.blocks, pic.cordoned, pic.rollTarget)
}

const (
	pictureRevisionA = "comp-rev-aaaaaaaa"
	pictureRevisionB = "comp-rev-bbbbbbbb"
)

// pictureRow builders, each one row as a pass records it.
func readyRow(index int32) types.InstanceStatus {
	return types.InstanceStatus{Index: index, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: pictureRevisionA, PodCount: 1, ServingPodCount: 1}
}

// readyOutOfRotation is a Ready row whose published counts say one of its
// pods left rotation, which ranks it behind every Ready row in rotation.
func readyOutOfRotation(index int32) types.InstanceStatus {
	row := readyRow(index)
	row.PodCount, row.ServingPodCount = 2, 1
	return row
}

// gangSurgeSource is the source of a gang surge whose replacement is built
// at surge: Updating at the Surge step, or Failed once the attempt ended.
func gangSurgeSource(index, surge int32, phase types.InstancePhase) types.InstanceStatus {
	return types.InstanceStatus{Index: index, Incarnation: 1, Phase: phase, RunningRevision: pictureRevisionA, TargetRevision: pictureRevisionB,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, SurgeIndex: &surge, TargetRevision: pictureRevisionB}}
}

// gangSurgeReplacementRow is the row a gang surge claims its replacement's
// index with, at the step given.
func gangSurgeReplacementRow(index int32, phase types.InstancePhase, step string) types.InstanceStatus {
	return types.InstanceStatus{Index: index, Phase: phase,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, TargetRevision: pictureRevisionB}}
}

// migrationSource is a source mid-move whose replacement is built at surge.
func migrationSource(index, surge int32) types.InstanceStatus {
	return types.InstanceStatus{Index: index, Incarnation: 1, Phase: types.InstancePhaseMigrating, RunningRevision: pictureRevisionA,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &surge}}
}

// promotedReplacement is a replacement row that carries the proof a move
// or a surge demands: first generation, slot 0, Ready on rev, nothing open.
func promotedReplacement(index int32, rev string) types.InstanceStatus {
	return types.InstanceStatus{Index: index, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: rev}
}

// drainingSurgeSource is the source of a single-pod or gang surge at the
// SurgeDrain step: its replacement is up and the old pod set drains.
func drainingSurgeSource(index, surge int32) types.InstanceStatus {
	row := gangSurgeSource(index, surge, types.InstancePhaseUpdating)
	row.Operation.Step = types.UpdateStepSurgeDrain
	return row
}

func indexSet(indexes ...int32) map[int32]struct{} {
	set := make(map[int32]struct{}, len(indexes))
	for _, idx := range indexes {
		set[idx] = struct{}{}
	}
	return set
}

// indexPictureCase is one picture of the selection's inputs and the
// indexes the plan keeps on it.
type indexPictureCase struct {
	name    string
	picture indexPicture
	want    []int32
}

// indexPicturesInWords are the pictures where the rows are read by their
// index: rows in any order, a repeated index, a replacement row found
// whatever its position, a failed gang surge pair read against the row at
// its replacement's index, a scale-up filling the gaps, a count already
// filled, a held revision and a roll target.
var indexPicturesInWords = []indexPictureCase{
	{name: "rows in index order at rest",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), readyRow(1), readyRow(2), readyRow(3)}, replicas: 4},
		want:    []int32{0, 1, 2, 3}},
	{name: "rows out of index order answer in index order",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(3), readyRow(1), readyRow(0), readyRow(2)}, replicas: 4},
		want:    []int32{0, 1, 2, 3}},
	{name: "a scale-down ranks the rows wherever they sit",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(3), readyOutOfRotation(1), readyRow(0), readyRow(2)}, replicas: 3},
		want:    []int32{0, 2, 3}},
	{name: "two rows with one index: the later row's reading counts",
		picture: indexPicture{rows: []types.InstanceStatus{readyOutOfRotation(0), readyOutOfRotation(1), readyRow(1)}, replicas: 1},
		want:    []int32{1}},
	{name: "two rows with one index: the index is kept once",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), readyRow(1), readyRow(1)}, replicas: 3},
		want:    []int32{0, 1, 2}},
	{name: "a promoted move retires its source, the replacement listed first",
		picture: indexPicture{rows: []types.InstanceStatus{promotedReplacement(0, pictureRevisionA), readyRow(1), migrationSource(2, 0)}, replicas: 2},
		want:    []int32{0, 1}},
	{name: "a promoted move retires its source, the replacement listed last",
		picture: indexPicture{rows: []types.InstanceStatus{migrationSource(2, 0), readyRow(1), promotedReplacement(0, pictureRevisionA)}, replicas: 2},
		want:    []int32{0, 1}},
	{name: "a move whose replacement is not yet Ready keeps both halves whatever the count",
		picture: indexPicture{rows: []types.InstanceStatus{migrationSource(0, 2), readyRow(1),
			{Index: 2, Phase: types.InstancePhaseCreating, Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}}}, replicas: 1},
		want: []int32{0, 2}},
	{name: "a promoted gang surge retires its source and keeps the replacement",
		picture: indexPicture{rows: []types.InstanceStatus{drainingSurgeSource(0, 3), readyRow(1), readyRow(2), promotedReplacement(3, pictureRevisionB)}, replicas: 3},
		want:    []int32{1, 2, 3}},
	{name: "a replacement index with two rows: the later row is the one read",
		picture: indexPicture{rows: []types.InstanceStatus{drainingSurgeSource(0, 2), readyRow(1), promotedReplacement(2, pictureRevisionB),
			{Index: 2, Phase: types.InstancePhaseCreating}}, replicas: 2},
		want: []int32{0, 1, 2}},
	{name: "a gang surge in flight keeps both halves whatever the count",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), gangSurgeSource(1, 2, types.InstancePhaseUpdating),
			gangSurgeReplacementRow(2, types.InstancePhaseCreating, types.UpdateStepGangSurgeTarget)}, replicas: 1},
		want: []int32{1, 2}},
	{name: "a failed gang surge source whose replacement index another row holds keeps both",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(0, 2, types.InstancePhaseFailed), readyRow(1), readyRow(2)}, replicas: 1},
		want:    []int32{0, 2}},
	{name: "a failed gang surge source whose replacement index is empty waits behind the Ready rows",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(0, 5, types.InstancePhaseFailed), readyRow(1)}, replicas: 1},
		want:    []int32{1}},
	{name: "a failed gang surge source whose replacement index is empty fills the next slot",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(0, 5, types.InstancePhaseFailed), readyRow(1)}, replicas: 2},
		want:    []int32{0, 1}},
	{name: "a failed gang surge pair waits whole behind the Ready rows",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(0, 2, types.InstancePhaseFailed), readyRow(1),
			gangSurgeReplacementRow(2, types.InstancePhaseFailed, types.UpdateStepGangSurgeTarget)}, replicas: 1},
		want: []int32{1}},
	{name: "a failed gang surge pair follows its source in behind the Ready rows",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(0, 2, types.InstancePhaseFailed), readyRow(1),
			gangSurgeReplacementRow(2, types.InstancePhaseFailed, types.UpdateStepGangSurgeTargetCleanup)}, replicas: 2},
		want: []int32{0, 1, 2}},
	{name: "a failed gang surge pair listed replacement first follows the same way",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeReplacementRow(2, types.InstancePhaseFailed, types.UpdateStepGangSurgeTarget),
			readyRow(1), gangSurgeSource(0, 2, types.InstancePhaseFailed)}, replicas: 2},
		want: []int32{0, 1, 2}},
	{name: "two failed sources naming one replacement: the lower index fills a slot alone",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(1, 3, types.InstancePhaseFailed), gangSurgeSource(0, 3, types.InstancePhaseFailed),
			readyRow(2), gangSurgeReplacementRow(3, types.InstancePhaseFailed, types.UpdateStepGangSurgeTarget)}, replicas: 2},
		want: []int32{0, 2}},
	{name: "two failed sources naming one replacement: the first listed brings it in",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(1, 3, types.InstancePhaseFailed), gangSurgeSource(0, 3, types.InstancePhaseFailed),
			readyRow(2), gangSurgeReplacementRow(3, types.InstancePhaseFailed, types.UpdateStepGangSurgeTarget)}, replicas: 3},
		want: []int32{0, 1, 2, 3}},
	{name: "a failed gang surge pair counts as cordoned when its replacement is",
		picture: indexPicture{rows: []types.InstanceStatus{gangSurgeSource(0, 3, types.InstancePhaseFailed), readyOutOfRotation(1), readyOutOfRotation(2),
			gangSurgeReplacementRow(3, types.InstancePhaseFailed, types.UpdateStepGangSurgeTarget)}, replicas: 2, cordoned: indexSet(3)},
		want: []int32{1, 2}},
	{name: "a scale-up fills the gaps lowest first",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), readyRow(2), readyRow(5)}, replicas: 5},
		want:    []int32{0, 1, 2, 3, 5}},
	{name: "no rows: the count is new indexes",
		picture: indexPicture{replicas: 3},
		want:    []int32{0, 1, 2}},
	{name: "a count already filled adds nothing",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), readyRow(1), readyRow(2), readyRow(3)}, replicas: 2},
		want:    []int32{0, 1}},
	{name: "a row on a held revision leaves before a row on a sound one",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), {Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: pictureRevisionB, PodCount: 1, ServingPodCount: 1}},
			replicas: 1, blocks: []types.RetryBlock{{TargetRevision: pictureRevisionA, State: types.RetryBlockHeld}}},
		want: []int32{1}},
	{name: "a serving row on the roll's target is kept ahead of one off it",
		picture: indexPicture{rows: []types.InstanceStatus{readyRow(0), {Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: pictureRevisionB, PodCount: 1, ServingPodCount: 1}},
			replicas: 1, rollTarget: pictureRevisionB},
		want: []int32{1}},
}

// TestInstancePlanIndices_OnEveryPicture holds the selection's answer on
// each of indexPicturesInWords.
func TestInstancePlanIndices_OnEveryPicture(t *testing.T) {
	for _, tc := range indexPicturesInWords {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.picture.chosen()); diff != "" {
				t.Errorf("instancePlanIndices (-want +got):\n%s", diff)
			}
		})
	}
}

// recordedPictures is the file the random pictures' answers are kept in:
// each picture spelled in words, and beside it the indexes the selection
// keeps, spelled in ascending order ("0 1 5"; "" when it keeps none).
type recordedPictures struct {
	Reason   string            `json:"reason"`
	Seed     int64             `json:"seed"`
	Pictures int               `json:"pictures"`
	Answers  map[string]string `json:"answers"`
}

const (
	indexSelectionSeed     = 2026
	indexSelectionPictures = 600
)

// TestInstancePlanIndices_RandomPicturesAsRecorded holds the selection's
// answer on random pictures of rows, counts, cordons, blocks and roll
// targets to the answers recorded in testdata/index_selection_pictures.json,
// each picture spelled in words beside its answer. The pictures come from
// one seed, so the file names every picture the check reads.
func TestInstancePlanIndices_RandomPicturesAsRecorded(t *testing.T) {
	path := filepath.Join("testdata", "index_selection_pictures.json")
	painter := &picturePainter{random: rand.New(rand.NewSource(indexSelectionSeed))}
	answers := map[string]string{}
	for i := 0; i < indexSelectionPictures; i++ {
		pic := painter.picture()
		answers[spellPicture(pic)] = spellIndexes(pic.chosen())
	}
	if *updateIndexSelection {
		writeRecordedPictures(t, path, answers)
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the recorded pictures: %v", err)
	}
	var recorded recordedPictures
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatalf("decoding the recorded pictures: %v", err)
	}
	if recorded.Seed != indexSelectionSeed || recorded.Pictures != indexSelectionPictures || len(recorded.Answers) != len(answers) {
		t.Fatalf("%s records %d answers of %d pictures from seed %d; this run painted %d distinct pictures of %d from seed %d: rerun with -update-index-selection",
			path, len(recorded.Answers), recorded.Pictures, recorded.Seed, len(answers), indexSelectionPictures, indexSelectionSeed)
	}
	moved := 0
	for _, picture := range slices.Sorted(mapKeys(answers)) {
		want, found := recorded.Answers[picture]
		if !found {
			moved++
			t.Errorf("picture not recorded: %s", picture)
			continue
		}
		if got := answers[picture]; got != want {
			moved++
			t.Errorf("the selection's answer moved on %s: recorded [%s], got [%s]", picture, want, got)
		}
	}
	if moved > 0 {
		t.Errorf("%d of %d pictures moved; a change made on purpose re-records them with -update-index-selection and names its reason in the file", moved, len(answers))
	}
}

func mapKeys(answers map[string]string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for key := range answers {
			if !yield(key) {
				return
			}
		}
	}
}

func writeRecordedPictures(t *testing.T, path string, answers map[string]string) {
	t.Helper()
	recorded := recordedPictures{
		Reason:   "The plan's index selection, read on random pictures of up to eight rows over the indexes 0 to 7: every phase, both generations and slots, revisions, pod counts in and out of rotation, update, migrate, restart and create operations at every step with or without a replacement index and a request, a crash remembered after Ready; counts 0 to 6, cordoned Instances, a held, backing-off or retrying revision, and a roll target. Each picture is spelled beside the indexes the selection keeps, in ascending order. Re-record with -update-index-selection and write here what changed the answers and why that is intended.",
		Seed:     indexSelectionSeed,
		Pictures: indexSelectionPictures,
		Answers:  answers,
	}
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(recorded); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// picturePainter paints random pictures from one seed.
type picturePainter struct {
	random *rand.Rand
}

func (p *picturePainter) pick(n int) int          { return p.random.Intn(n) }
func (p *picturePainter) chance(percent int) bool { return p.random.Intn(100) < percent }

var (
	pictureRevisions = []string{"", pictureRevisionA, pictureRevisionB}
	picturePhases    = []types.InstancePhase{types.InstancePhaseReady, types.InstancePhaseReady, types.InstancePhaseReady,
		types.InstancePhaseUpdating, types.InstancePhaseMigrating, types.InstancePhaseFailed, types.InstancePhaseCreating,
		types.InstancePhasePending, types.InstancePhaseRestarting}
	pictureOperationTypes = []types.InstanceOperationType{types.InstanceOperationUpdate, types.InstanceOperationUpdate, types.InstanceOperationUpdate,
		types.InstanceOperationMigrate, types.InstanceOperationMigrate, types.InstanceOperationRestart, types.InstanceOperationCreate}
	pictureSteps = []string{"", types.UpdateStepSurge, types.UpdateStepSurgeDrain, types.UpdateStepSurgeDrainSettle,
		types.UpdateStepGangSurgeTarget, types.UpdateStepGangSurgeTargetCleanup, types.UpdateStepDrain, types.UpdateStepInPlace}
	pictureBlockStates = []types.RetryBlockState{types.RetryBlockHeld, types.RetryBlockBackoff, types.RetryBlockRetryInProgress}
	pictureReadyAt     = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

// picture paints up to eight rows over the indexes 0 to 7, each index at
// most once and the rows in any order, with a count, cordons, blocks and a
// roll target. Two rows with one index are the explicit pictures' alone: a
// Component records each Instance once.
func (p *picturePainter) picture() indexPicture {
	count := p.pick(9)
	order := p.random.Perm(8)
	rows := make([]types.InstanceStatus, 0, count)
	for i := 0; i < count; i++ {
		rows = append(rows, p.row(int32(order[i])))
	}
	pic := indexPicture{rows: rows, replicas: int32(p.pick(7)), rollTarget: pictureRevisions[p.pick(len(pictureRevisions))]}
	for idx := int32(0); idx < 8; idx++ {
		if p.chance(12) {
			if pic.cordoned == nil {
				pic.cordoned = map[int32]struct{}{}
			}
			pic.cordoned[idx] = struct{}{}
		}
	}
	if p.chance(60) {
		pic.blocks = []types.RetryBlock{{TargetRevision: pictureRevisions[1+p.pick(2)], State: pictureBlockStates[p.pick(len(pictureBlockStates))]}}
	}
	return pic
}

func (p *picturePainter) row(index int32) types.InstanceStatus {
	row := types.InstanceStatus{
		Index:           index,
		Phase:           picturePhases[p.pick(len(picturePhases))],
		Incarnation:     int64(1 + p.pick(2)),
		ActiveOrdinal:   int32(p.pick(2)),
		RunningRevision: pictureRevisions[p.pick(len(pictureRevisions))],
		TargetRevision:  pictureRevisions[p.pick(len(pictureRevisions))],
		PodCount:        []int32{0, 1, 8}[p.pick(3)],
	}
	if row.PodCount > 0 {
		row.ServingPodCount = row.PodCount - int32(p.pick(2))
	}
	if p.chance(55) {
		row.Operation = p.operation()
	}
	if p.chance(15) {
		row.ReadySince = &metav1.Time{Time: pictureReadyAt}
		row.LastFailure = &types.InstanceTermination{Time: metav1.Time{Time: pictureReadyAt.Add(time.Minute)}}
	}
	return row
}

func (p *picturePainter) operation() *types.InstanceOperation {
	op := &types.InstanceOperation{
		Type:           pictureOperationTypes[p.pick(len(pictureOperationTypes))],
		Step:           pictureSteps[p.pick(len(pictureSteps))],
		TargetRevision: pictureRevisions[p.pick(len(pictureRevisions))],
	}
	if p.chance(60) {
		surge := int32(p.pick(8))
		op.SurgeIndex = &surge
	}
	if p.chance(50) {
		op.RequestUUID = "request-a"
	}
	return op
}

// spellPicture writes a picture in words, one row after another, then the
// count, the cordoned Instances, the roll target and the blocks.
func spellPicture(pic indexPicture) string {
	var text strings.Builder
	text.WriteString("rows[")
	for i := range pic.rows {
		if i > 0 {
			text.WriteString(" | ")
		}
		text.WriteString(spellRow(&pic.rows[i]))
	}
	fmt.Fprintf(&text, "] replicas=%d", pic.replicas)
	cordoned := slices.Sorted(mapIndexes(pic.cordoned))
	if len(cordoned) > 0 {
		fmt.Fprintf(&text, " cordoned=%v", cordoned)
	}
	if pic.rollTarget != "" {
		text.WriteString(" target=" + shortRevision(pic.rollTarget))
	}
	for _, block := range pic.blocks {
		fmt.Fprintf(&text, " block=%s:%s", shortRevision(block.TargetRevision), block.State)
	}
	return text.String()
}

func mapIndexes(set map[int32]struct{}) func(func(int32) bool) {
	return func(yield func(int32) bool) {
		for idx := range set {
			if !yield(idx) {
				return
			}
		}
	}
}

// spellIndexes writes the kept indexes in order, separated by spaces.
func spellIndexes(indexes []int32) string {
	words := make([]string, len(indexes))
	for i, idx := range indexes {
		words[i] = fmt.Sprint(idx)
	}
	return strings.Join(words, " ")
}

// shortRevision is the revision's last letter run: "a" for rev-aaaaaaaa.
func shortRevision(rev string) string {
	return strings.TrimPrefix(rev, "comp-rev-")[:1]
}

func spellRow(row *types.InstanceStatus) string {
	phase := string(row.Phase)
	if phase == "" {
		phase = "none"
	}
	parts := []string{fmt.Sprintf("%d:%s", row.Index, phase)}
	if row.Incarnation != 1 {
		parts = append(parts, fmt.Sprintf("generation=%d", row.Incarnation))
	}
	if row.ActiveOrdinal != 0 {
		parts = append(parts, fmt.Sprintf("slot=%d", row.ActiveOrdinal))
	}
	if row.RunningRevision != "" {
		parts = append(parts, "runs="+shortRevision(row.RunningRevision))
	}
	if row.TargetRevision != "" {
		parts = append(parts, "to="+shortRevision(row.TargetRevision))
	}
	if row.PodCount != 0 {
		parts = append(parts, fmt.Sprintf("serving=%d/%d", row.ServingPodCount, row.PodCount))
	}
	if row.Operation != nil {
		parts = append(parts, spellOperation(row.Operation))
	}
	if row.LastFailure != nil {
		parts = append(parts, "crashed-after-ready")
	}
	return strings.Join(parts, " ")
}

func spellOperation(op *types.InstanceOperation) string {
	text := "op=" + string(op.Type)
	if op.Step != "" {
		text += ":" + op.Step
	}
	if op.SurgeIndex != nil {
		text += fmt.Sprintf("->%d", *op.SurgeIndex)
	}
	if op.TargetRevision != "" {
		text += "=" + shortRevision(op.TargetRevision)
	}
	if op.RequestUUID != "" {
		text += "+request"
	}
	return text
}
