package types

import "testing"

func TestSweptPods_RemembersWhatThePassRemoved(t *testing.T) {
	var s SweptPods
	if s.Swept("a") {
		t.Fatalf("nothing recorded yet")
	}
	s.Record("a")
	if !s.Swept("a") || s.Swept("b") {
		t.Fatalf("only the recorded pod reads as swept: a=%v b=%v", s.Swept("a"), s.Swept("b"))
	}
}

func TestSweptPods_IsNilSafe(t *testing.T) {
	var s *SweptPods
	s.Record("a")
	if s.Swept("a") {
		t.Fatalf("a nil record holds nothing")
	}
}
