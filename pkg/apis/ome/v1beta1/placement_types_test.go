package v1beta1

import (
	"testing"

	"k8s.io/utils/ptr"
)

func TestSplitSpecCeiling(t *testing.T) {
	type want struct{ engine, decoder, router int32 }
	for _, tt := range []struct {
		name  string
		split *SplitSpec
		want  want
	}{
		{name: "no split declares no cap", want: want{}},
		{name: "an engine key caps only the engine", split: &SplitSpec{MaxPerCluster: &SplitCeilings{Engine: ptr.To[int32](60)}}, want: want{engine: 60}},
		{name: "default caps every component the map does not name", split: &SplitSpec{MaxPerCluster: &SplitCeilings{Default: ptr.To[int32](60), Router: ptr.To[int32](8)}}, want: want{engine: 60, decoder: 60, router: 8}},
		{name: "a key wins over default", split: &SplitSpec{MaxPerCluster: &SplitCeilings{Default: ptr.To[int32](60), Engine: ptr.To[int32](40), Decoder: ptr.To[int32](20)}}, want: want{engine: 40, decoder: 20, router: 60}},
		{name: "a map without default caps only the components it names", split: &SplitSpec{MaxPerCluster: &SplitCeilings{Router: ptr.To[int32](8)}}, want: want{router: 8}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := want{
				engine:  tt.split.Ceiling(EngineComponent),
				decoder: tt.split.Ceiling(DecoderComponent),
				router:  tt.split.Ceiling(RouterComponent),
			}
			if got != tt.want {
				t.Errorf("Ceiling() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
