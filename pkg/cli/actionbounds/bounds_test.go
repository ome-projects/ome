package actionbounds

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPrivatePayloadByteNodeDepthBudgets(t *testing.T) {
	require.True(t, PrivatePayload(strings.Repeat("a", 1048576-8)))
	require.False(t, PrivatePayload(strings.Repeat("a", 1048576-7)))
	require.True(t, PrivatePayload([]byte(strings.Repeat("a", 1048576-8))))
	require.False(t, PrivatePayload([]byte(strings.Repeat("a", 1048576-7))))
	require.True(t, PrivatePayload(make([]int, 32767)))
	require.False(t, PrivatePayload(make([]int, 32768)))
	var nested any = "leaf"
	for range 16 {
		nested = []any{nested}
	}
	require.True(t, PrivatePayload(nested))
	nested = []any{nested}
	require.False(t, PrivatePayload(nested))
	require.True(t, PrivatePayload(time.Now()))
	require.True(t, PrivatePayload(nil))
	require.True(t, PrivatePayload(struct{ Value any }{Value: map[string]any{"time": time.Now(), "nested": []any{nil, 1}}}))
	require.False(t, PrivatePayload(struct{ Value string }{Value: strings.Repeat("a", 1048576)}))
	require.False(t, PrivatePayload(map[string]string{"value": strings.Repeat("a", 1048576)}))
	require.True(t, PrivatePayload([3]byte{1, 2, 3}))
	require.False(t, PrivatePayload([1048576]byte{}))
	largeMap := map[int]int{}
	for i := range 32769 {
		largeMap[i] = i
	}
	require.False(t, PrivatePayload(largeMap))
	largeArray := [32768]int{}
	require.False(t, PrivatePayload(largeArray))
	type cycle struct{ Next *cycle }
	loop := &cycle{}
	loop.Next = loop
	require.False(t, PrivatePayload(loop))
}

func TestRawRevisionJSONMustBeBoundedBeforeSpecDecode(t *testing.T) {
	for _, raw := range []string{strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33), "[" + strings.Repeat("0,", 32768) + "0]", `{"unknown":"` + strings.Repeat("a", 1048576) + `"}`} {
		// The existing typed bound treats RawExtension as bytes. It must also
		// refuse nested decoded payloads before revision spec decoding.
		require.False(t, JSONPayload([]byte(raw)))
	}
	require.True(t, JSONPayload([]byte(`{"valid":[1,"value",true,null]}`)))
	for _, raw := range []string{"", "{", "{}{}", "[1,]"} {
		require.False(t, JSONPayload([]byte(raw)))
	}
}
