package geo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
)

const RepairRevisionBinding = "unbound_requires_fenced_hot_state_plan"

// LocationRepairBundleRunID hashes the sole canonical JSON representation with
// an empty run ID. It binds a proposal, not an actual hot-state revision fence.
func LocationRepairBundleRunID(b LocationRepairBundle) (string, error) {
	b.RunID = ""
	data, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// ValidateLocationRepairBundle checks DB proposal structure only. It neither
// reads RawHints nor materializes final wire patches or serving permission.
func ValidateLocationRepairBundle(b LocationRepairBundle) error {
	if b.Version != 1 || b.RevisionBinding != RepairRevisionBinding {
		return fmt.Errorf("unsupported/unbound bundle contract")
	}
	digest := func(s string) bool { v, err := hex.DecodeString(s); return err == nil && len(v) == sha256.Size }
	if !digest(b.DatasetSHA256) {
		return fmt.Errorf("missing dataset SHA-256")
	}
	expected, err := LocationRepairBundleRunID(b)
	if err != nil {
		return err
	}
	if b.RunID != expected {
		return fmt.Errorf("bundle run identity mismatch")
	}
	if b.Stories == nil || b.Articles == nil {
		return fmt.Errorf("entity arrays must be explicit, including empty arrays")
	}
	previous := ""
	for _, p := range b.Stories {
		if p.ID == "" || p.ID <= previous || !digest(p.DBBeforeHash) {
			return fmt.Errorf("invalid/unsorted story identity %q", p.ID)
		}
		previous = p.ID
		if err := validateVotes(p.Locations, false); err != nil {
			return fmt.Errorf("story %s: %w", p.ID, err)
		}
		if len(p.Locations) == 0 {
			if !p.Unlocated || p.Primary != 0 || p.PrimaryPlaceID != "" {
				return fmt.Errorf("invalid unlocated story %s", p.ID)
			}
		} else {
			if p.Unlocated || int(p.Primary) >= len(p.Locations) || p.Locations[p.Primary].PlaceID != p.PrimaryPlaceID {
				return fmt.Errorf("invalid primary story %s", p.ID)
			}
		}
	}
	previous = ""
	for _, p := range b.Articles {
		if p.ID == "" || p.ID <= previous || !digest(p.DBBeforeHash) {
			return fmt.Errorf("invalid/unsorted article identity %q", p.ID)
		}
		previous = p.ID
		if err := validateVotes(p.Locations, true); err != nil {
			return fmt.Errorf("article %s: %w", p.ID, err)
		}
		if p.Unlocated != (len(p.Locations) == 0) || !p.NeedsHotEvidence {
			return fmt.Errorf("article %s requires unresolved hot evidence", p.ID)
		}
		c := p.EventCandidate
		if c.Located {
			if !ValidCoordinates(c.Lat, c.Lon) {
				return fmt.Errorf("invalid event candidate %s", p.ID)
			}
		} else if c.Place != "" || c.Lat != 0 || c.Lon != 0 {
			return fmt.Errorf("unlocated event candidate has fields %s", p.ID)
		}
	}
	return nil
}
func validateVotes(votes []LocationVote, article bool) error {
	if votes == nil {
		return fmt.Errorf("after votes must be an explicit array")
	}
	previous := ""
	var previousRole int16
	for i, v := range votes {
		if v.PlaceID == "" || i > 0 && (v.PlaceID < previous || v.PlaceID == previous && v.Role <= previousRole) {
			return fmt.Errorf("invalid/duplicate/unsorted vote")
		}
		previous, previousRole = v.PlaceID, v.Role
		if !ValidCoordinates(v.Lat, v.Lon) || v.Precision < 0 || v.Precision > 4 || v.Role < 0 || v.Role > 4 {
			return fmt.Errorf("invalid coordinate or enum")
		}
		for _, n := range []float32{v.Weight, v.Confidence} {
			if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) || n < 0 {
				return fmt.Errorf("invalid weight/confidence")
			}
		}
		// Story weights are accumulated votes, not probabilities.
		if v.Confidence > 1 {
			return fmt.Errorf("invalid article confidence")
		}
		if article && v.Weight != 0 || !article && v.Confidence != 0 {
			return fmt.Errorf("confidence and story weight cannot be interchanged")
		}
	}
	return nil
}
