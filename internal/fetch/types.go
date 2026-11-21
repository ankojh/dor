package fetch

// PlacePrecision is how exact a parser-supplied location is. The values are
// deliberately parallel to dor.v1.Precision so the agents can map them with a
// short switch.
type PlacePrecision int

// Precision levels, coarsest to exact.
const (
	PrecisionUnknown PlacePrecision = iota - 1
	PrecisionCountry
	PrecisionRegion
	PrecisionCity
	PrecisionPoint
)
