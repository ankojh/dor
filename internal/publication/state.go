// Package publication implements the durable publication layout and its read-only
// recovery path. Sequence allocation and input acknowledgement belong to agents.
package publication

import (
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/ankojh/dor/internal/pb/dorv1"
	"google.golang.org/protobuf/proto"
)

// State contains committed data only. Admitted but uncommitted rows never enter
// a base checkpoint. Orders are persisted because map iteration is not eviction.
type State struct {
	Seq        uint64
	Raw        uint64
	Ordinal    uint64
	Stories    map[string]*dorv1.Story
	Links      map[string]*dorv1.Link
	Rows       map[string]*dorv1.WireRow
	WireOrder  []string
	RecentWire []*dorv1.WireRow
	Revisions  map[string]uint64
	Scores     map[string]*dorv1.MatchScore
	ScoreOrder []string
}

func NewState(snapshot *dorv1.Snapshot) *State {
	s := &State{Stories: map[string]*dorv1.Story{}, Links: map[string]*dorv1.Link{}, Rows: map[string]*dorv1.WireRow{}, Revisions: map[string]uint64{}, Scores: map[string]*dorv1.MatchScore{}}
	if snapshot == nil {
		return s
	}
	s.Seq = snapshot.Seq
	for _, v := range snapshot.Stories {
		s.Stories[v.Id] = v
	}
	for _, v := range snapshot.Links {
		s.Links[v.A+"|"+v.B] = v
	}
	for _, v := range snapshot.RecentWire {
		s.Rows[v.ArticleId] = v
		s.WireOrder = append(s.WireOrder, v.ArticleId)
		s.RecentWire = append(s.RecentWire, v)
	}
	return s
}

// Clone copies containers. Values are immutable after batch sealing.
func (s *State) Clone() *State {
	r := *s
	r.Stories, r.Links, r.Rows = maps.Clone(s.Stories), maps.Clone(s.Links), maps.Clone(s.Rows)
	r.Revisions, r.Scores = maps.Clone(s.Revisions), maps.Clone(s.Scores)
	r.WireOrder, r.ScoreOrder = append([]string(nil), s.WireOrder...), append([]string(nil), s.ScoreOrder...)
	r.RecentWire = append([]*dorv1.WireRow(nil), s.RecentWire...)
	return &r
}

func (s *State) Snapshot() *dorv1.Snapshot {
	r := &dorv1.Snapshot{Seq: s.Seq, RecentWire: append([]*dorv1.WireRow(nil), s.RecentWire...)}
	for _, v := range s.Stories {
		r.Stories = append(r.Stories, v)
	}
	for _, v := range s.Links {
		r.Links = append(r.Links, v)
	}
	sort.Slice(r.Stories, func(i, j int) bool { return r.Stories[i].Id < r.Stories[j].Id })
	sort.Slice(r.Links, func(i, j int) bool { return r.Links[i].A+"|"+r.Links[i].B < r.Links[j].A+"|"+r.Links[j].B })
	return r
}

type OrderChange struct {
	Remove []string
	Append []string
}

// DiffOrder preserves a surviving prefix of the desired order and appends only
// the suffix. It also handles eviction and re-admission within the same batch.
func DiffOrder(before, after []string) OrderChange {
	keep := map[string]bool{}
	idx := 0
	for _, id := range before {
		if idx < len(after) && id == after[idx] {
			keep[id] = true
			idx++
		}
	}
	r := OrderChange{Append: append([]string(nil), after[idx:]...)}
	for _, id := range before {
		if !keep[id] {
			r.Remove = append(r.Remove, id)
		}
	}
	return r
}

func (o OrderChange) Apply(order []string) []string {
	remove := map[string]bool{}
	for _, id := range o.Remove {
		remove[id] = true
	}
	r := make([]string, 0, len(order)+len(o.Append))
	for _, id := range order {
		if !remove[id] {
			r = append(r, id)
		}
	}
	return append(r, o.Append...)
}

// Record is the immutable journal/outbox unit, including metadata-only input.
type Record struct {
	Ordinal         uint64
	PreviousSeq     uint64
	PreviousRaw     uint64
	Data            []byte
	Revisions       map[string]uint64
	RevisionDeletes []string
	Scores          map[string]*dorv1.MatchScore
	ScoreDeletes    []string
	WireOrder       OrderChange
	ScoreOrder      OrderChange
}

func (r *Record) End() (uint64, uint64, error) {
	var batch dorv1.DeltaBatch
	if err := proto.Unmarshal(r.Data, &batch); err != nil {
		return 0, 0, err
	}
	seq := r.PreviousSeq
	for _, d := range batch.Deltas {
		if d == nil || d.Seq != seq+1 {
			return 0, 0, errors.New("publication: invalid record sequence")
		}
		seq = d.Seq
	}
	raw := r.PreviousRaw
	if len(batch.Deltas) > 0 {
		raw++
	}
	return seq, raw, nil
}

func (s *State) Apply(r *Record, raw uint64) error {
	if r.Ordinal != s.Ordinal+1 || r.PreviousSeq != s.Seq || r.PreviousRaw != s.Raw {
		return errors.New("publication: journal watermark mismatch")
	}
	var batch dorv1.DeltaBatch
	if err := proto.Unmarshal(r.Data, &batch); err != nil {
		return err
	}
	for _, d := range batch.Deltas {
		if d == nil || d.Seq != s.Seq+1 {
			return fmt.Errorf("publication: non-contiguous delta after %d", s.Seq)
		}
		switch b := d.Body.(type) {
		case *dorv1.Delta_StoryUpsert:
			if b.StoryUpsert == nil {
				return errors.New("publication: nil story")
			}
			s.Stories[b.StoryUpsert.Id] = b.StoryUpsert
		case *dorv1.Delta_StoryExpire:
			delete(s.Stories, b.StoryExpire)
		case *dorv1.Delta_LinkUpsert:
			if b.LinkUpsert == nil {
				return errors.New("publication: nil link")
			}
			s.Links[b.LinkUpsert.A+"|"+b.LinkUpsert.B] = b.LinkUpsert
		case *dorv1.Delta_LinkRemove:
			if b.LinkRemove == nil {
				return errors.New("publication: nil link key")
			}
			delete(s.Links, b.LinkRemove.A+"|"+b.LinkRemove.B)
		case *dorv1.Delta_Wire:
			if b.Wire == nil {
				return errors.New("publication: nil wire")
			}
			s.Rows[b.Wire.ArticleId] = b.Wire
			for i, v := range s.RecentWire {
				if v.ArticleId == b.Wire.ArticleId {
					s.RecentWire = append(s.RecentWire[:i], s.RecentWire[i+1:]...)
					break
				}
			}
			s.RecentWire = append(s.RecentWire, b.Wire)
			if len(s.RecentWire) > 500 {
				s.RecentWire = append([]*dorv1.WireRow(nil), s.RecentWire[len(s.RecentWire)-500:]...)
			}
		}
		s.Seq = d.Seq
	}
	if len(batch.Deltas) > 0 && raw != s.Raw+1 || len(batch.Deltas) == 0 && raw != s.Raw {
		return errors.New("publication: raw journal watermark mismatch")
	}
	s.Raw, s.Ordinal = raw, r.Ordinal
	for id, v := range r.Revisions {
		s.Revisions[id] = v
	}
	for _, id := range r.RevisionDeletes {
		delete(s.Revisions, id)
	}
	for id, v := range r.Scores {
		s.Scores[id] = v
	}
	for _, id := range r.ScoreDeletes {
		delete(s.Scores, id)
	}
	s.WireOrder = r.WireOrder.Apply(s.WireOrder)
	s.ScoreOrder = r.ScoreOrder.Apply(s.ScoreOrder)
	keep := map[string]bool{}
	for _, id := range s.WireOrder {
		keep[id] = true
	}
	for id := range s.Rows {
		if !keep[id] {
			delete(s.Rows, id)
		}
	}
	return nil
}
