package appstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const observationTTL = 30 * time.Second
const fullSyncInterval = 60 * time.Second
const maxInventory = 10000
const maxBindings = 1000

var (
	ErrCollectorBusy = errors.New("collector already connected")
	ErrStreamClosed  = errors.New("collector stream closed")
	ErrStreamFrame   = errors.New("invalid collector stream frame")
)

// Memory holds reconstructible Kubernetes evidence only. It has no database,
// network, or workload mutation dependency. One instance belongs to one Core
// process and one Station; a new process starts with unknown observations.
type Memory struct {
	mu            sync.Mutex
	station, boot string
	now           func() time.Time
	collectors    map[string]*memoryCollector
	bindings      map[string]*memoryBinding
	scopeRevision map[string]uint64
	legacy        []legacyBinding
}

type memoryCollector struct {
	config                        Collector
	active                        *StreamSession
	objects                       map[string]memoryResource
	lastFull                      time.Time
	inventoryAt, inventoryExpires time.Time
}

type memoryResource struct {
	ResourceObservation
	received, expires, healthExpires time.Time
	metadataDigest                   string
}

// Binding is supplied by Core's authenticated persistent installation read,
// never by Runtime. Location history is kept only after an exact identity match.
type Binding struct {
	InstallationID, OrganizationID, AppID string
}

type memoryBinding struct {
	Binding
	collector, namespace, name, uid string
	absentAt, absentExpires         time.Time
	view                            Runtime
	revision                        uint64
	semantic                        string
}

type MemoryView struct {
	Runtime  Runtime
	Revision string
	Resource *ResourceObservation
}

// StreamSession is a capability tied to one live connection. Even if a handler
// keeps an old pointer after timeout, it cannot modify the replacement session.
type StreamSession struct {
	memory         *Memory
	collector      *memoryCollector
	id             string
	lastFrame      time.Time
	synced, closed bool
	syncID         string
	started        time.Time
	staging        map[string]memoryResource
}

func NewMemory(station string, collectors []Collector) (*Memory, error) {
	if !validID(station) || len(collectors) > 128 {
		return nil, ErrStreamFrame
	}
	m := &Memory{station: station, boot: uuid.NewString(), now: time.Now,
		collectors: map[string]*memoryCollector{}, bindings: map[string]*memoryBinding{}, scopeRevision: map[string]uint64{}}
	namespaces := map[string]bool{}
	for _, c := range collectors {
		if !c.valid() || m.collectors[c.ID] != nil {
			return nil, ErrStreamFrame
		}
		for _, ns := range c.Namespaces {
			if namespaces[ns] {
				return nil, ErrStreamFrame
			}
			namespaces[ns] = true
		}
		c.Namespaces = slices.Clone(c.Namespaces)
		c.HealthRequiredApps = slices.Clone(c.HealthRequiredApps)
		m.collectors[c.ID] = &memoryCollector{config: c, objects: map[string]memoryResource{}}
	}
	return m, nil
}

func (m *Memory) Open(collector string) (*StreamSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.collectors[collector]
	if c == nil {
		return nil, ErrStreamFrame
	}
	now := m.now()
	if old := c.active; old != nil {
		if old.live(now) {
			return nil, ErrCollectorBusy
		}
		old.closeLocked()
	}
	s := &StreamSession{memory: m, collector: c, id: uuid.NewString(), lastFrame: now}
	c.active = s
	return s, nil
}

func (s *StreamSession) ID() string { return s.id }
func (m *Memory) BootID() string    { return m.boot }

func (s *StreamSession) live(now time.Time) bool {
	return !s.closed && s.collector.active == s && now.Before(s.lastFrame.Add(observationTTL)) &&
		(s.staging == nil || now.Before(s.started.Add(observationTTL)))
}

func (s *StreamSession) closeLocked() {
	s.closed = true
	s.staging = nil
	s.syncID = ""
	if s.collector.active == s {
		s.collector.active = nil
	}
}

func (s *StreamSession) Close() {
	s.memory.mu.Lock()
	defer s.memory.mu.Unlock()
	s.closeLocked()
}

// apply serializes the connection's accepted frames. Any invalid frame closes
// its session and discards staging, without publishing a partial inventory.
func (s *StreamSession) apply(fn func(time.Time) error) error {
	m := s.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if !s.live(now) {
		s.closeLocked()
		return ErrStreamClosed
	}
	if err := fn(now); err != nil {
		s.closeLocked()
		return err
	}
	s.lastFrame = now
	m.projectAll(now)
	return nil
}

func (s *StreamSession) Begin(syncID string) error {
	return s.apply(func(now time.Time) error {
		if !validID(syncID) || s.staging != nil {
			return ErrStreamFrame
		}
		s.syncID, s.started = syncID, now
		s.staging = map[string]memoryResource{}
		return nil
	})
}

func (s *StreamSession) Objects(syncID string, objects []ResourceObservation) error {
	return s.apply(func(now time.Time) error {
		if s.staging == nil || syncID != s.syncID || len(objects) == 0 || len(objects) > 500 {
			return ErrStreamFrame
		}
		if len(s.staging)+len(objects) > maxInventory {
			return ErrLimit
		}
		// Validate the whole frame before changing even the private staging set.
		seen := map[string]bool{}
		for _, o := range objects {
			key := o.Namespace + "/" + o.Name
			if !validStreamResource(o, s.collector.config) || o.Deleted || seen[key] {
				return ErrStreamFrame
			}
			if _, exists := s.staging[key]; exists {
				return ErrStreamFrame
			}
			seen[key] = true
		}
		for _, o := range objects {
			s.staging[o.Namespace+"/"+o.Name] = s.sample(o, now)
		}
		return nil
	})
}

func sampled(o ResourceObservation, now time.Time) memoryResource {
	if o.Metadata != nil {
		metadata := *o.Metadata
		metadata.Images = slices.Clone(metadata.Images)
		o.Metadata = &metadata
	}
	raw, _ := json.Marshal(o.Metadata)
	sum := sha256.Sum256(raw)
	return memoryResource{ResourceObservation: o, received: now, expires: now.Add(observationTTL), healthExpires: now.Add(observationTTL), metadataDigest: hex.EncodeToString(sum[:])}
}

// A relist is not a new HTTP probe. Preserve a matching sample's original
// deadline, and never carry health across a connection or changed facts.
func (s *StreamSession) sample(o ResourceObservation, now time.Time) memoryResource {
	next := sampled(o, now)
	if s.synced && o.Health == "not_checked" {
		old, exists := s.collector.objects[o.Namespace+"/"+o.Name]
		previous := old.ResourceObservation
		previous.Health = "not_checked"
		if exists && reflect.DeepEqual(previous, o) {
			next.Health, next.healthExpires = old.Health, old.healthExpires
		}
	}
	return next
}

func (s *StreamSession) Health(f Health) error {
	return s.apply(func(now time.Time) error {
		if !s.synced || s.staging != nil || !validStreamLocation(f.Namespace, f.Name, f.UID, s.collector.config) ||
			len(f.ResourceVersion) == 0 || len(f.ResourceVersion) > 256 || (f.Health != "ready" && f.Health != "failed") {
			return ErrStreamFrame
		}
		key := f.Namespace + "/" + f.Name
		o, exists := s.collector.objects[key]
		if !exists || o.Deleted || o.UID != f.UID || o.ResourceVersion != f.ResourceVersion || o.InstanceID != "" || o.Terminating || o.Desired == 0 {
			return nil
		}
		o.Health, o.healthExpires = f.Health, now.Add(observationTTL)
		s.collector.objects[key] = o
		return nil
	})
}

func (s *StreamSession) End(syncID string, count int) error {
	return s.apply(func(now time.Time) error {
		if s.staging == nil || s.syncID != syncID || count != len(s.staging) {
			return ErrStreamFrame
		}
		c := s.collector
		for _, b := range s.memory.bindings {
			if b.collector != c.config.ID || b.namespace == "" {
				continue
			}
			if _, present := s.staging[b.namespace+"/"+b.name]; !present {
				b.absentAt, b.absentExpires = now, now.Add(observationTTL)
			}
		}
		c.objects, c.lastFull = s.staging, s.started
		c.inventoryAt, c.inventoryExpires = s.started, s.started.Add(observationTTL)
		s.staging, s.syncID, s.synced = nil, "", true
		return nil
	})
}

func (s *StreamSession) Upsert(o ResourceObservation) error {
	return s.apply(func(now time.Time) error {
		if !s.synced || s.staging != nil || o.Deleted || !validStreamResource(o, s.collector.config) {
			return ErrStreamFrame
		}
		key := o.Namespace + "/" + o.Name
		if _, exists := s.collector.objects[key]; !exists && len(s.collector.objects) >= maxInventory {
			return ErrLimit
		}
		s.collector.objects[key] = s.sample(o, now)
		return nil
	})
}

func (s *StreamSession) Delete(namespace, name, uid string) error {
	return s.apply(func(now time.Time) error {
		if !s.synced || s.staging != nil || !validStreamLocation(namespace, name, uid, s.collector.config) {
			return ErrStreamFrame
		}
		key := namespace + "/" + name
		o, exists := s.collector.objects[key]
		if !exists || o.UID != uid {
			return nil
		}
		delete(s.collector.objects, key)
		for _, b := range s.memory.bindings {
			if b.collector == s.collector.config.ID && b.namespace == namespace && b.name == name && b.uid == uid {
				b.absentAt, b.absentExpires = now, now.Add(observationTTL)
			}
		}
		return nil
	})
}

func (s *StreamSession) Heartbeat() error {
	return s.apply(func(now time.Time) error {
		c := s.collector
		if !s.synced || s.staging != nil || !now.Before(c.lastFull.Add(fullSyncInterval)) {
			return ErrStreamFrame
		}
		for key, o := range c.objects {
			// A Watch heartbeat confirms inventory freshness, never HTTP health.
			o.received, o.expires = now, now.Add(observationTTL)
			c.objects[key] = o
		}
		c.inventoryAt, c.inventoryExpires = now, now.Add(observationTTL)
		for _, b := range s.memory.bindings {
			if b.collector == c.config.ID && !b.absentAt.IsZero() {
				b.absentAt, b.absentExpires = now, now.Add(observationTTL)
			}
		}
		return nil
	})
}

// Views registers only server-read identities and returns one coherent copy.
// The caller supplies all installations required by its DB snapshot. Observed
// identifiers alone never create an installation or grant organization access.
func (m *Memory) Views(bindings []Binding) (map[string]MemoryView, map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot, err := m.snapshotLocked(bindings)
	return snapshot.installations, snapshot.revisions, err
}

func (m *Memory) viewsLocked(bindings []Binding) (map[string]MemoryView, map[string]string, error) {
	add := map[string]Binding{}
	wanted := map[string]bool{}
	for _, b := range bindings {
		wanted[b.InstallationID] = true
		if !validID(b.InstallationID) || !validID(b.OrganizationID) || !appIdentifier.MatchString(b.AppID) {
			return nil, nil, ErrStreamFrame
		}
		if old, ok := add[b.InstallationID]; ok && old != b {
			return nil, nil, ErrStreamFrame
		}
		if old := m.bindings[b.InstallationID]; old != nil && old.Binding != b {
			return nil, nil, ErrStreamFrame
		}
		if m.bindings[b.InstallationID] == nil {
			add[b.InstallationID] = b
		}
	}
	if len(wanted) > maxBindings {
		return nil, nil, ErrLimit
	}
	// The caller reads the full active installation registry in its DB
	// transaction. Reclaim history outside that registry when needed; repeated
	// uninstall/reinstall must not exhaust the cache's active binding budget.
	if len(m.bindings)+len(add) > maxBindings {
		for id, b := range m.bindings {
			if !wanted[id] {
				delete(m.bindings, id)
				m.scopeRevision[b.OrganizationID]++
			}
		}
	}
	for id, b := range add {
		m.bindings[id] = &memoryBinding{Binding: b}
	}
	m.projectAll(m.now())
	views, revisions := map[string]MemoryView{}, map[string]string{}
	for _, b := range bindings {
		stored := m.bindings[b.InstallationID]
		view := MemoryView{Runtime: cloneRuntime(stored.view), Revision: m.token(stored.revision)}
		if c := m.collectors[stored.collector]; c != nil && stored.view.ReasonCode != "IDENTITY_CONFLICT" {
			if o, ok := c.objects[stored.namespace+"/"+stored.name]; ok && o.UID == stored.uid && o.StationID == m.station && o.OrganizationID == b.OrganizationID && o.AppID == b.AppID && o.InstallationID == b.InstallationID && o.InstanceID == "" {
				copy := sampled(o.ResourceObservation, o.received).ResourceObservation
				view.Resource = &copy
			}
		}
		views[b.InstallationID] = view
		revisions[b.OrganizationID] = m.token(m.scopeRevision[b.OrganizationID])
	}
	return views, revisions, nil
}

func (m *Memory) token(revision uint64) string {
	return m.boot + ":" + strconv.FormatUint(revision, 10)
}

func cloneRuntime(v Runtime) Runtime {
	if v.ObservedAt != nil {
		x := *v.ObservedAt
		v.ObservedAt = &x
	}
	if v.ExpiresAt != nil {
		x := *v.ExpiresAt
		v.ExpiresAt = &x
	}
	if v.LastKnownState != nil {
		x := *v.LastKnownState
		v.LastKnownState = &x
	}
	return v
}

// Expire advances quality even without events. Views repeats this check so a
// delayed scanner can never serve stale ready evidence.
func (m *Memory) Expire() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for _, c := range m.collectors {
		if c.active != nil && !c.active.live(now) {
			c.active.closeLocked()
		}
	}
	m.projectAll(now)
}

type memoryCandidate struct {
	c *memoryCollector
	o memoryResource
}

func (m *Memory) projectAll(now time.Time) {
	index := map[string][]memoryCandidate{}
	for _, c := range m.collectors {
		for _, o := range c.objects {
			if o.StationID == m.station && o.InstanceID == "" && o.InstallationID != "" {
				index[o.InstallationID] = append(index[o.InstallationID], memoryCandidate{c, o})
			}
		}
	}
	for _, b := range m.bindings {
		v := m.project(b, now, index[b.InstallationID])
		if v.ReasonCode == "HEALTH_NOT_OBSERVED" && b.view.LastKnownState != nil {
			x := *b.view.LastKnownState
			v.LastKnownState = &x
		}
		last := ""
		if v.LastKnownState != nil {
			last = *v.LastKnownState
		}
		semantic := b.uid + "/" + v.State + "/" + v.Quality + "/" + v.ReasonCode + "/" + last
		if c := m.collectors[b.collector]; c != nil {
			if o, ok := c.objects[b.namespace+"/"+b.name]; ok && o.UID == b.uid && o.StationID == m.station && o.OrganizationID == b.OrganizationID && o.AppID == b.AppID && o.InstallationID == b.InstallationID && o.InstanceID == "" {
				semantic += "/" + o.metadataDigest
			}
		}
		if semantic != b.semantic {
			b.semantic = semantic
			b.revision++
			m.scopeRevision[b.OrganizationID]++
		}
		b.view = v
	}
}

func (m *Memory) project(b *memoryBinding, now time.Time, resources []memoryCandidate) Runtime {
	unknown := Runtime{State: "unknown", Quality: "unknown", ReasonCode: "NOT_SYNCED"}
	var candidates []memoryCandidate
	for _, r := range resources {
		if r.o.OrganizationID == b.OrganizationID && r.o.AppID == b.AppID {
			candidates = append(candidates, r)
		}
	}
	if len(candidates) > 1 {
		unknown.ReasonCode = "IDENTITY_CONFLICT"
		return unknown
	}
	if len(candidates) == 1 {
		c, o := candidates[0].c, candidates[0].o
		if b.collector != "" && (b.collector != c.config.ID || b.namespace != o.Namespace || b.name != o.Name) {
			unknown.ReasonCode = "IDENTITY_CONFLICT"
			return unknown
		}
		b.collector, b.namespace, b.name, b.uid = c.config.ID, o.Namespace, o.Name, o.UID
		b.absentAt, b.absentExpires = time.Time{}, time.Time{}
		required := slices.Contains(c.config.HealthRequiredApps, b.AppID)
		fact := o.ResourceObservation
		if now.Before(o.expires) && !now.Before(o.healthExpires) {
			fact.Health = "not_checked"
		}
		state, reason := evaluate(fact, required)
		return RuntimeView(now, &state, &reason, &o.received, &o.expires, false)
	}
	if b.collector != "" {
		c := m.collectors[b.collector]
		if c == nil || !slices.Contains(c.config.Namespaces, b.namespace) {
			unknown.ReasonCode = "COLLECTOR_SCOPE_CHANGED"
			return unknown
		}
		if o, present := c.objects[b.namespace+"/"+b.name]; present {
			state, reason := "unknown", "IDENTITY_CONFLICT"
			return RuntimeView(now, &state, &reason, &o.received, &o.expires, false)
		}
		if !b.absentAt.IsZero() {
			state, reason := "absent", "RESOURCE_ABSENT"
			return RuntimeView(now, &state, &reason, &b.absentAt, &b.absentExpires, false)
		}
	}
	return unknown
}
