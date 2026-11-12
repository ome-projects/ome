// Package fakeapiserver answers the list and watch requests an informer
// makes, over HTTP, from objects a test loads, with the bytes a
// kube-apiserver would send: protobuf for the kinds client-go negotiates it
// for (the built-in API), JSON for custom resources. A controller-runtime
// cache built against it runs its production decode, transform and index
// path in-process, without a cluster.
//
// The server keeps no object graph. Load encodes the objects at once and
// keeps the bytes, and Forget drops them, so a test that measures the
// client's memory can make sure what it reads is the client's. Selectors and
// pagination in a request are not applied: every loaded object of a kind is
// one page. A request for one object, discovery, or any write is refused.
package fakeapiserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	restclientwatch "k8s.io/client-go/rest/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Verb names the kind of request an informer made.
type Verb string

const (
	// VerbList is a list of a kind.
	VerbList Verb = "list"
	// VerbWatch is a watch that starts after the objects the client listed.
	VerbWatch Verb = "watch"
	// VerbWatchList is a watch that asks for the current objects as its
	// first events (the request carries sendInitialEvents).
	VerbWatchList Verb = "watch-list"
)

// Request is one request the server answered.
type Request struct {
	Verb     Verb
	Resource string
	// Refused is set when the server answered with an error status.
	Refused bool
}

// streamBatch is how many watch frames one write to a connection carries.
// The client decodes frame by frame whatever the batch; the batch keeps
// the server's own work per frame to a fraction of an allocation.
const streamBatch = 256

// watchWait bounds how long Stream waits for a watch of the kind to open.
const watchWait = 30 * time.Second

// Server is one fake kube-apiserver. Serve registers the kinds it answers
// for, Load gives a kind its objects, Stream sends watch events, and Config
// and Mapper are what a client or cache needs to reach it.
type Server struct {
	tb        testing.TB
	scheme    *runtime.Scheme
	codecs    serializer.CodecFactory
	mapper    *meta.DefaultRESTMapper
	server    *httptest.Server
	closeOnce sync.Once

	// InitialEvents makes a watch that asks for the current objects as its
	// first events get them, followed by the bookmark that ends them. Off,
	// such a watch is refused and the informer lists instead, as it does
	// against an apiserver without the watch-list feature. Set it before
	// Serve and Load.
	InitialEvents bool

	mu              sync.Mutex
	kinds           map[schema.GroupVersionResource]*kind
	byGVK           map[schema.GroupVersionKind]*kind
	resourceVersion uint64
	requests        []Request
}

// kind is one served resource with its encoded objects.
type kind struct {
	gvk      schema.GroupVersionKind
	resource string
	// protobuf says the kind is served as protobuf: the rule
	// controller-runtime applies when it picks the content type it asks for.
	protobuf bool
	// list is the encoded list body; nil after Forget.
	list []byte
	// frames are the encoded ADDED events of the loaded objects, built when
	// InitialEvents was on at Load, and bookmark the event that ends them,
	// built at Serve and at every Load; both nil after Forget.
	frames   [][]byte
	bookmark []byte
	// forgotten says Forget ran: a list is refused until the next Load.
	forgotten bool
	watchers  map[*watcher]struct{}
}

// watcher is one open watch connection; frames arrive in batches.
type watcher struct {
	frames chan [][]byte
}

// New starts a server for the kinds of scheme and closes it when the test
// ends.
func New(tb testing.TB, scheme *runtime.Scheme) *Server {
	tb.Helper()
	s := &Server{
		tb:     tb,
		scheme: scheme,
		codecs: serializer.NewCodecFactory(scheme),
		mapper: meta.NewDefaultRESTMapper(nil),
		kinds:  map[schema.GroupVersionResource]*kind{},
		byGVK:  map[schema.GroupVersionKind]*kind{},
	}
	s.server = httptest.NewServer(s)
	tb.Cleanup(s.Close)
	return s
}

// Close stops the server and its open watches. The test's cleanup calls it
// too; a test that measures what outlives the server calls it first.
func (s *Server) Close() {
	s.closeOnce.Do(s.server.Close)
}

// Config reaches the server. Client-side rate limiting is off: the limit is
// the real client's, and a test measures the server's answers, not the wait
// for a token.
func (s *Server) Config() *rest.Config {
	return &rest.Config{Host: s.server.URL, QPS: -1}
}

// Mapper maps every served kind to its resource and scope, which a cache
// otherwise learns from discovery.
func (s *Server) Mapper() meta.RESTMapper {
	return s.mapper
}

// Serve registers obj's kind at scope. The kind lists empty until Load.
func (s *Server) Serve(obj client.Object, scope meta.RESTScopeName) {
	s.tb.Helper()
	gvk := s.gvkOf(obj)
	plural, _ := meta.UnsafeGuessKindToResource(gvk)
	restScope := meta.RESTScopeNamespace
	if scope == meta.RESTScopeNameRoot {
		restScope = meta.RESTScopeRoot
	}
	s.mapper.Add(gvk, restScope)
	k := &kind{
		gvk:      gvk,
		resource: plural.Resource,
		protobuf: clientgoscheme.Scheme.Recognizes(gvk),
		watchers: map[*watcher]struct{}{},
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resourceVersion++
	k.list = s.encodeList(k, nil, s.resourceVersion)
	k.bookmark = s.encodeBookmark(k, s.resourceVersion)
	s.kinds[plural] = k
	s.byGVK[gvk] = k
}

// Load makes objs the objects of their kinds: what a list returns and, with
// InitialEvents on, what a watch that asks for them streams first. The
// objects are encoded now and only the bytes are kept. An object without a
// resourceVersion is given one, as the apiserver gives every object one.
// Each call replaces the kind's earlier objects.
func (s *Server) Load(objs ...client.Object) {
	s.tb.Helper()
	groups := map[schema.GroupVersionKind][]runtime.Object{}
	for _, obj := range objs {
		gvk := s.gvkOf(obj)
		groups[gvk] = append(groups[gvk], obj)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for gvk, items := range groups {
		k := s.kindFor(gvk)
		for _, item := range items {
			s.resourceVersion++
			if accessor, err := meta.Accessor(item); err == nil && accessor.GetResourceVersion() == "" {
				accessor.SetResourceVersion(strconv.FormatUint(s.resourceVersion, 10))
			}
		}
		s.resourceVersion++
		k.list = s.encodeList(k, items, s.resourceVersion)
		k.bookmark = s.encodeBookmark(k, s.resourceVersion)
		k.frames = nil
		if s.InitialEvents {
			k.frames = s.encodeEvents(k, watch.Added, items)
		}
		k.forgotten = false
	}
}

// Forget drops the encoded objects of obj's kind. A list of the kind is
// refused until the next Load; watches stay open. What the server holds
// for the kind afterwards is nothing.
func (s *Server) Forget(obj client.Object) {
	s.tb.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.kindFor(s.gvkOf(obj))
	k.list, k.frames, k.bookmark = nil, nil, nil
	k.forgotten = true
}

// Encode returns one watch frame per object, of the given event type, as
// Stream sends them. Encoding ahead of Stream keeps the encoding out of
// what a test measures while the frames are streamed.
func (s *Server) Encode(eventType watch.EventType, objs ...client.Object) [][]byte {
	s.tb.Helper()
	var frames [][]byte
	for _, obj := range objs {
		k := s.kindFor(s.gvkOf(obj))
		s.mu.Lock()
		s.resourceVersion++
		if obj.GetResourceVersion() == "" || eventType == watch.Modified {
			obj.SetResourceVersion(strconv.FormatUint(s.resourceVersion, 10))
		}
		s.mu.Unlock()
		frames = append(frames, s.encodeEvents(k, eventType, []runtime.Object{obj})...)
	}
	return frames
}

// Stream hands frames, in batches, to every open watch of obj's kind. It
// waits for a watch to open first and fails the test when none does.
func (s *Server) Stream(obj client.Object, frames [][]byte) {
	s.tb.Helper()
	k := s.kindFor(s.gvkOf(obj))
	deadline := time.Now().Add(watchWait)
	for {
		s.mu.Lock()
		open := len(k.watchers) > 0
		s.mu.Unlock()
		if open {
			break
		}
		if time.Now().After(deadline) {
			s.tb.Fatalf("fakeapiserver: no watch of %s opened within %s", k.resource, watchWait)
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	watchers := make([]*watcher, 0, len(k.watchers))
	for w := range k.watchers {
		watchers = append(watchers, w)
	}
	s.mu.Unlock()
	for _, w := range watchers {
		for start := 0; start < len(frames); start += streamBatch {
			end := min(start+streamBatch, len(frames))
			w.frames <- frames[start:end]
		}
	}
}

// Send encodes objs as events of the given type and streams them.
func (s *Server) Send(eventType watch.EventType, objs ...client.Object) {
	s.tb.Helper()
	if len(objs) == 0 {
		return
	}
	s.Stream(objs[0], s.Encode(eventType, objs...))
}

// Requests returns every request answered so far, in order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// ServeHTTP routes a request to its kind's list or watch.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	gvr, ok := parseCollectionPath(r.URL.Path)
	s.mu.Lock()
	k := s.kinds[gvr]
	s.mu.Unlock()
	if !ok || k == nil || r.Method != http.MethodGet {
		s.record(Request{Verb: VerbList, Resource: gvr.Resource, Refused: true})
		writeStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound, "fakeapiserver serves only lists and watches of the kinds registered with Serve")
		return
	}
	query := r.URL.Query()
	switch {
	case query.Get("watch") == "true" && query.Get("sendInitialEvents") == "true":
		s.serveWatch(w, r, k, VerbWatchList)
	case query.Get("watch") == "true":
		s.serveWatch(w, r, k, VerbWatch)
	default:
		s.serveList(w, k)
	}
}

func (s *Server) serveList(w http.ResponseWriter, k *kind) {
	s.mu.Lock()
	body, forgotten := k.list, k.forgotten
	s.mu.Unlock()
	if forgotten {
		s.record(Request{Verb: VerbList, Resource: k.resource, Refused: true})
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, "the objects of "+k.resource+" were forgotten; load them again")
		return
	}
	s.record(Request{Verb: VerbList, Resource: k.resource})
	w.Header().Set("Content-Type", k.contentType())
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) serveWatch(w http.ResponseWriter, r *http.Request, k *kind, verb Verb) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.record(Request{Verb: verb, Resource: k.resource, Refused: true})
		writeStatus(w, http.StatusInternalServerError, metav1.StatusReasonInternalError, "the connection cannot stream")
		return
	}
	s.mu.Lock()
	initial, bookmark, forgotten := k.frames, k.bookmark, k.forgotten
	s.mu.Unlock()
	if verb == VerbWatchList && (!s.InitialEvents || forgotten) {
		s.record(Request{Verb: verb, Resource: k.resource, Refused: true})
		writeStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest, "sendInitialEvents is not served")
		return
	}
	s.record(Request{Verb: verb, Resource: k.resource})
	w.Header().Set("Content-Type", k.streamContentType())
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	wt := &watcher{frames: make(chan [][]byte, 16)}
	s.mu.Lock()
	k.watchers[wt] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(k.watchers, wt)
		s.mu.Unlock()
	}()
	if verb == VerbWatchList {
		for start := 0; start < len(initial); start += streamBatch {
			writeFrames(w, initial[start:min(start+streamBatch, len(initial))])
		}
		writeFrames(w, [][]byte{bookmark})
		flusher.Flush()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case batch := <-wt.frames:
			writeFrames(w, batch)
			flusher.Flush()
		}
	}
}

// writeFrames writes a batch of frames as one write.
func writeFrames(w http.ResponseWriter, frames [][]byte) {
	_, _ = w.Write(bytes.Join(frames, nil))
}

func (s *Server) record(req Request) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()
}

func (s *Server) gvkOf(obj runtime.Object) schema.GroupVersionKind {
	s.tb.Helper()
	gvk, err := apiutil.GVKForObject(obj, s.scheme)
	if err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	return gvk
}

// kindFor is the served kind of gvk; a kind Serve never saw fails the test.
func (s *Server) kindFor(gvk schema.GroupVersionKind) *kind {
	s.tb.Helper()
	k := s.byGVK[gvk]
	if k == nil {
		s.tb.Fatalf("fakeapiserver: %s is not served; call Serve first", gvk)
	}
	return k
}

// serializerInfo is the kind's wire format: protobuf or JSON.
func (s *Server) serializerInfo(k *kind) runtime.SerializerInfo {
	s.tb.Helper()
	mediaType := runtime.ContentTypeJSON
	if k.protobuf {
		mediaType = runtime.ContentTypeProtobuf
	}
	info, ok := runtime.SerializerInfoForMediaType(s.codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		s.tb.Fatalf("fakeapiserver: no serializer for %s", mediaType)
	}
	return info
}

// encodeList encodes items as the kind's list at resourceVersion.
func (s *Server) encodeList(k *kind, items []runtime.Object, resourceVersion uint64) []byte {
	s.tb.Helper()
	list, err := s.scheme.New(k.gvk.GroupVersion().WithKind(k.gvk.Kind + "List"))
	if err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	if err := meta.SetList(list, items); err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	accessor, err := meta.ListAccessor(list)
	if err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	accessor.SetResourceVersion(strconv.FormatUint(resourceVersion, 10))
	encoder := s.codecs.EncoderForVersion(s.serializerInfo(k).Serializer, k.gvk.GroupVersion())
	data, err := runtime.Encode(encoder, list)
	if err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	return data
}

// encodeEvents encodes one watch frame per object, framed as the kind's
// stream format frames them.
func (s *Server) encodeEvents(k *kind, eventType watch.EventType, objs []runtime.Object) [][]byte {
	s.tb.Helper()
	info := s.serializerInfo(k)
	version := k.gvk.GroupVersion()
	embedded := s.codecs.EncoderForVersion(info.Serializer, version)
	outer := s.codecs.EncoderForVersion(info.StreamSerializer.Serializer, version)
	frames := make([][]byte, 0, len(objs))
	for _, obj := range objs {
		var buf bytes.Buffer
		encoder := restclientwatch.NewEncoder(streaming.NewEncoder(info.StreamSerializer.Framer.NewFrameWriter(&buf), outer), embedded)
		if err := encoder.Encode(&watch.Event{Type: eventType, Object: obj}); err != nil {
			s.tb.Fatalf("fakeapiserver: %v", err)
		}
		frames = append(frames, buf.Bytes())
	}
	return frames
}

// encodeBookmark encodes the bookmark that ends the initial events of a
// watch: an empty object of the kind at resourceVersion, annotated as the
// apiserver annotates it.
func (s *Server) encodeBookmark(k *kind, resourceVersion uint64) []byte {
	s.tb.Helper()
	obj, err := s.scheme.New(k.gvk)
	if err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		s.tb.Fatalf("fakeapiserver: %v", err)
	}
	accessor.SetResourceVersion(strconv.FormatUint(resourceVersion, 10))
	accessor.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	return s.encodeEvents(k, watch.Bookmark, []runtime.Object{obj})[0]
}

func (k *kind) contentType() string {
	if k.protobuf {
		return runtime.ContentTypeProtobuf
	}
	return runtime.ContentTypeJSON
}

func (k *kind) streamContentType() string {
	return k.contentType() + ";stream=watch"
}

// parseCollectionPath reads the resource a collection path names:
// /api/v1/pods, /api/v1/namespaces/<ns>/pods, /apis/<group>/<version>/<resource>
// and its namespaced form. A path naming one object is not a collection.
func parseCollectionPath(path string) (schema.GroupVersionResource, bool) {
	var gvr schema.GroupVersionResource
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var rest []string
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		gvr.Version, rest = parts[1], parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		gvr.Group, gvr.Version, rest = parts[1], parts[2], parts[3:]
	default:
		return gvr, false
	}
	if len(rest) >= 3 && rest[0] == "namespaces" {
		rest = rest[2:]
	}
	if len(rest) != 1 {
		return gvr, false
	}
	gvr.Resource = rest[0]
	return gvr, true
}

// writeStatus answers with a Status the client decodes as an error.
func writeStatus(w http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	status := metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Code:     int32(code),
		Reason:   reason,
		Message:  message,
	}
	w.Header().Set("Content-Type", runtime.ContentTypeJSON)
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(status); err != nil {
		fmt.Fprint(w, message)
	}
}
