package query

import (
	"math/rand/v2"
	"strconv"
	"testing"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// podNameTuple is what a pod name spells: the service, the Component, the
// Instance, the Runner and the ordinal.
type podNameTuple struct {
	isvc      string
	component workload.ComponentType
	index     int32
	runner    string
	ordinal   int32
}

func (p podNameTuple) name() string {
	return PodName(p.isvc, p.component, p.index, p.runner, p.ordinal)
}

func (p podNameTuple) is(name string) bool {
	return PodNameIs(name, p.isvc, p.component, p.index, p.runner, p.ordinal)
}

var podNameBase = podNameTuple{isvc: "llama-70b", component: workload.ComponentEngine, index: 7, runner: workload.RunnerDefault, ordinal: 0}

// TestPodNameIs_OnEveryPicture holds the reading of a name against a tuple
// to whether the tuple builds that name: the name itself, and names that
// share a prefix, a piece or a spelling with it.
func TestPodNameIs_OnEveryPicture(t *testing.T) {
	pictures := []struct {
		name string
		text string
		want bool
	}{
		{name: "the name itself", text: "llama-70b-engine-7-default-0", want: true},
		{name: "the service cut short", text: "llama-70-engine-7-default-0", want: false},
		{name: "the service extended", text: "llama-70bx-engine-7-default-0", want: false},
		{name: "the service's dash moved", text: "llama-70b-engine7-default-0", want: false},
		{name: "another Component", text: "llama-70b-decoder-7-default-0", want: false},
		{name: "the Component extended", text: "llama-70b-engines-7-default-0", want: false},
		{name: "the index extended", text: "llama-70b-engine-70-default-0", want: false},
		{name: "the index with a leading zero", text: "llama-70b-engine-07-default-0", want: false},
		{name: "the index with a sign", text: "llama-70b-engine-+7-default-0", want: false},
		{name: "another runner", text: "llama-70b-engine-7-leader-0", want: false},
		{name: "the runner extended", text: "llama-70b-engine-7-defaults-0", want: false},
		{name: "the runner cut short", text: "llama-70b-engine-7-defaul-0", want: false},
		{name: "another ordinal", text: "llama-70b-engine-7-default-1", want: false},
		{name: "the ordinal extended", text: "llama-70b-engine-7-default-00", want: false},
		{name: "the ordinal missing", text: "llama-70b-engine-7-default-", want: false},
		{name: "a suffix", text: "llama-70b-engine-7-default-0-x", want: false},
		{name: "the empty name", text: "", want: false},
		{name: "the pieces out of order", text: "engine-llama-70b-7-default-0", want: false},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			if got := podNameBase.is(pic.text); got != pic.want {
				t.Errorf("PodNameIs(%q, %+v) = %t, want %t", pic.text, podNameBase, got, pic.want)
			}
			if built := podNameBase.name() == pic.text; built != pic.want {
				t.Errorf("PodName(%+v) == %q is %t, want %t", podNameBase, pic.text, built, pic.want)
			}
		})
	}
}

// TestPodNameIs_SpellsEveryNumberAsPodNameDoes holds the reading over the
// edges of the index and the ordinal: zero, the limits of an int32 and
// negative numbers read as PodName writes them.
func TestPodNameIs_SpellsEveryNumberAsPodNameDoes(t *testing.T) {
	for _, index := range []int32{0, 1, 9, 10, 99, 100, 2147483647, -1, -2147483648} {
		for _, ordinal := range []int32{0, 1, 7, 10, 2147483647, -1} {
			p := podNameTuple{isvc: "svc", component: workload.ComponentEngine, index: index, runner: workload.RunnerWorker, ordinal: ordinal}
			if !p.is(p.name()) {
				t.Errorf("PodNameIs does not read %q as its own name", p.name())
			}
			other := p
			other.index++
			if other.index != p.index && other.is(p.name()) {
				t.Errorf("PodNameIs reads %q as index %d", p.name(), other.index)
			}
		}
	}
}

// TestCutPodNamePrefix_OnEveryPicture holds what the cut leaves of a name:
// <runner>-<ordinal> for a pod of the Instance, nothing for a name of
// another service, Component or Instance.
func TestCutPodNamePrefix_OnEveryPicture(t *testing.T) {
	pictures := []struct {
		name, text, rest string
		ok               bool
	}{
		{name: "a pod of the Instance", text: "llama-70b-engine-7-default-0", rest: "default-0", ok: true},
		{name: "a gang worker of the Instance", text: "llama-70b-engine-7-worker-6", rest: "worker-6", ok: true},
		{name: "a rest of several dashes", text: "llama-70b-engine-7-worker-2-x-9", rest: "worker-2-x-9", ok: true},
		{name: "another Instance", text: "llama-70b-engine-70-default-0", ok: false},
		{name: "another Component", text: "llama-70b-decoder-7-default-0", ok: false},
		{name: "another service", text: "llama-70bx-engine-7-default-0", ok: false},
		{name: "the prefix alone", text: "llama-70b-engine-7-", rest: "", ok: true},
		{name: "the prefix without its last dash", text: "llama-70b-engine-7", ok: false},
		{name: "the empty name", text: "", ok: false},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			rest, ok := CutPodNamePrefix(pic.text, podNameBase.isvc, podNameBase.component, podNameBase.index)
			if rest != pic.rest || ok != pic.ok {
				t.Errorf("CutPodNamePrefix(%q) = %q, %t; want %q, %t", pic.text, rest, ok, pic.rest, pic.ok)
			}
		})
	}
}

// TestPodNameOrdinal_OnEveryPicture holds the ordinal read off the rest of
// a name for a runner, and the rests that read as none.
func TestPodNameOrdinal_OnEveryPicture(t *testing.T) {
	pictures := []struct {
		name, rest, runner string
		ordinal            int32
		ok                 bool
	}{
		{name: "ordinal 0", rest: "default-0", runner: "default", ordinal: 0, ok: true},
		{name: "ordinal 12", rest: "worker-12", runner: "worker", ordinal: 12, ok: true},
		{name: "a runner with a dash", rest: "worker-2-5", runner: "worker-2", ordinal: 5, ok: true},
		{name: "the largest ordinal", rest: "worker-2147483647", runner: "worker", ordinal: 2147483647, ok: true},
		{name: "a negative ordinal, as PodName writes one", rest: "worker--3", runner: "worker", ordinal: -3, ok: true},
		{name: "the smallest ordinal", rest: "worker--2147483648", runner: "worker", ordinal: -2147483648, ok: true},
		{name: "another runner", rest: "leader-0", runner: "worker", ok: false},
		{name: "the runner extended", rest: "workers-0", runner: "worker", ok: false},
		{name: "a leading zero", rest: "worker-01", runner: "worker", ok: false},
		{name: "a double zero", rest: "worker-00", runner: "worker", ok: false},
		{name: "a signed zero", rest: "worker--0", runner: "worker", ok: false},
		{name: "a plus sign", rest: "worker-+1", runner: "worker", ok: false},
		{name: "a suffix", rest: "worker-1-x", runner: "worker", ok: false},
		{name: "no ordinal", rest: "worker-", runner: "worker", ok: false},
		{name: "no dash", rest: "worker", runner: "worker", ok: false},
		{name: "past the largest ordinal", rest: "worker-2147483648", runner: "worker", ok: false},
		{name: "past the smallest ordinal", rest: "worker--2147483649", runner: "worker", ok: false},
		{name: "too many digits", rest: "worker-12345678901", runner: "worker", ok: false},
		{name: "a letter among the digits", rest: "worker-1a", runner: "worker", ok: false},
	}
	for _, pic := range pictures {
		t.Run(pic.name, func(t *testing.T) {
			ordinal, ok := PodNameOrdinal(pic.rest, pic.runner)
			if ordinal != pic.ordinal || ok != pic.ok {
				t.Errorf("PodNameOrdinal(%q, %q) = %d, %t; want %d, %t", pic.rest, pic.runner, ordinal, ok, pic.ordinal, pic.ok)
			}
		})
	}
}

// TestDecimalInt32_ReadsWhatPodNameWrites holds the reading to the writing
// over the edges of the type and over random text near a number: a text
// reads as a number exactly when that number is written as the text.
func TestDecimalInt32_ReadsWhatPodNameWrites(t *testing.T) {
	for _, value := range []int32{0, 1, 9, 10, 99, 100, 1000, 2147483646, 2147483647, -1, -9, -10, -2147483647, -2147483648} {
		text := strconv.FormatInt(int64(value), 10)
		if read, ok := decimalInt32(text); !ok || read != value {
			t.Errorf("decimalInt32(%q) = %d, %t; want %d, true", text, read, ok, value)
		}
	}
	rng := rand.New(rand.NewPCG(30, 1108))
	draws, numbers := 300000, 0
	for i := 0; i < draws; i++ {
		text := nearDecimal(rng)
		read, ok := decimalInt32(text)
		parsed, err := strconv.ParseInt(text, 10, 32)
		written := err == nil && strconv.FormatInt(parsed, 10) == text
		if ok != written || (ok && int64(read) != parsed) {
			t.Fatalf("draw %d: decimalInt32(%q) = %d, %t; the text %s", i, text, read, ok, map[bool]string{true: "is what the number is written as", false: "is not how any int32 is written"}[written])
		}
		if ok {
			numbers++
		}
	}
	t.Logf("%d draws: %d read as a number", draws, numbers)
	if numbers < draws/4 || draws-numbers < draws/4 {
		t.Fatalf("the draws are one-sided: %d numbers, %d others", numbers, draws-numbers)
	}
}

// nearDecimal is a number as written, or that text with a sign, a leading
// zero, a stray byte or a digit added, dropped or changed.
func nearDecimal(rng *rand.Rand) string {
	value := []int64{0, 1, 7, 10, 12, 99, 100, 2147483647, 2147483648, -1, -10, -2147483648, -2147483649, int64(rng.IntN(1 << 20)), -int64(rng.IntN(1 << 20))}[rng.IntN(15)]
	text := strconv.FormatInt(value, 10)
	switch rng.IntN(8) {
	case 0:
		return "0" + text
	case 1:
		return "+" + text
	case 2:
		return "-" + text
	case 3:
		at := rng.IntN(len(text) + 1)
		return text[:at] + string("0123456789-x "[rng.IntN(13)]) + text[at:]
	case 4:
		at := rng.IntN(len(text))
		return text[:at] + text[at+1:]
	case 5:
		return ""
	default:
		return text
	}
}

var (
	podNameServices   = []string{"", "a", "svc", "llama-70b", "svc-engine", "x-1-", "-"}
	podNameComponents = []workload.ComponentType{workload.ComponentEngine, workload.ComponentDecoder, workload.ComponentRouter, "eng", "engine-x"}
	podNameIndexes    = []int32{0, 1, 7, 10, 11, 70, 100, 2147483647, -1, -10}
	podNameRunners    = []string{workload.RunnerDefault, workload.RunnerLeader, workload.RunnerWorker, "workers", "worker-2", "", "default-0"}
	podNameOrdinals   = []int32{0, 1, 2, 7, 10, 2147483647, -1}
)

func randomPodNameTuple(rng *rand.Rand) podNameTuple {
	return podNameTuple{
		isvc:      podNameServices[rng.IntN(len(podNameServices))],
		component: podNameComponents[rng.IntN(len(podNameComponents))],
		index:     podNameIndexes[rng.IntN(len(podNameIndexes))],
		runner:    podNameRunners[rng.IntN(len(podNameRunners))],
		ordinal:   podNameOrdinals[rng.IntN(len(podNameOrdinals))],
	}
}

// nearName is a name close to the tuple's own: the name of a tuple that
// shares pieces with it, or its own name with one byte changed, dropped or
// added, or a number respelled.
func nearName(rng *rand.Rand, p podNameTuple) string {
	name := p.name()
	switch rng.IntN(6) {
	case 0:
		q := p
		switch rng.IntN(5) {
		case 0:
			q.isvc = podNameServices[rng.IntN(len(podNameServices))]
		case 1:
			q.component = podNameComponents[rng.IntN(len(podNameComponents))]
		case 2:
			q.index = podNameIndexes[rng.IntN(len(podNameIndexes))]
		case 3:
			q.runner = podNameRunners[rng.IntN(len(podNameRunners))]
		default:
			q.ordinal = podNameOrdinals[rng.IntN(len(podNameOrdinals))]
		}
		return q.name()
	case 1:
		if name == "" {
			return "-"
		}
		at := rng.IntN(len(name))
		return name[:at] + name[at+1:]
	case 2:
		at := rng.IntN(len(name) + 1)
		return name[:at] + string("-0x"[rng.IntN(3)]) + name[at:]
	case 3:
		if name == "" {
			return "0"
		}
		at := rng.IntN(len(name))
		return name[:at] + string("-0x"[rng.IntN(3)]) + name[at+1:]
	case 4:
		return p.isvc + "-" + string(p.component) + "-0" + strconv.Itoa(int(p.index)) + "-" + p.runner + "-" + strconv.Itoa(int(p.ordinal))
	default:
		return p.isvc + "-" + string(p.component) + "-" + strconv.Itoa(int(p.index)) + "-" + p.runner + "-0" + strconv.Itoa(int(p.ordinal))
	}
}

// TestPodNameIs_AgreesWithTheBuiltNameOnRandomDraws holds, over random
// tuples and names near them, that a name reads as the tuple's exactly
// when the tuple builds it.
func TestPodNameIs_AgreesWithTheBuiltNameOnRandomDraws(t *testing.T) {
	rng := rand.New(rand.NewPCG(30, 1107))
	draws, same, other := 300000, 0, 0
	for i := 0; i < draws; i++ {
		p := randomPodNameTuple(rng)
		name := p.name()
		if rng.IntN(2) == 0 {
			name = nearName(rng, p)
		}
		built := p.name() == name
		if got := p.is(name); got != built {
			t.Fatalf("draw %d: PodNameIs(%q, %+v) = %t, PodName builds %q", i, name, p, got, p.name())
		}
		if built {
			same++
		} else {
			other++
		}
	}
	t.Logf("%d draws: %d names the tuple builds, %d others", draws, same, other)
	if same < draws/4 || other < draws/4 {
		t.Fatalf("the draws are one-sided: %d names built, %d others", same, other)
	}
}
