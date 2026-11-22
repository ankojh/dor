package agents

import (
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/pemistahl/lingua-go"
	"golang.org/x/text/language"
)

// detectedLanguages is the news-facing subset loaded into the detector. The
// list covers the common source and GDELT languages without embedding every
// model Lingua ships; unknown scripts still resolve to "und".
var detectedLanguages = []lingua.Language{
	lingua.English, lingua.French, lingua.German, lingua.Spanish,
	lingua.Portuguese, lingua.Italian, lingua.Dutch, lingua.Russian,
	lingua.Ukrainian, lingua.Polish, lingua.Czech, lingua.Romanian,
	lingua.Swedish, lingua.Bokmal, lingua.Nynorsk, lingua.Danish, lingua.Finnish,
	lingua.Turkish, lingua.Arabic, lingua.Hebrew, lingua.Persian,
	lingua.Hindi, lingua.Bengali, lingua.Urdu, lingua.Chinese,
	lingua.Japanese, lingua.Korean, lingua.Indonesian, lingua.Vietnamese,
	lingua.Thai, lingua.Greek, lingua.Hungarian,
}

const (
	// minimumDetectRunes keeps very short fragments out of the detector, where
	// a single word is not evidence enough to name a language.
	minimumDetectRunes = 8
	// minimumDetectConfidence rejects weak detections instead of guessing.
	minimumDetectConfidence = 0.20
	// minimumDetectMargin rejects near-ties between the top two languages.
	minimumDetectMargin = 0.05
)

var (
	detectorOnce sync.Once
	detector     lingua.LanguageDetector
)

func languageDetector() lingua.LanguageDetector {
	detectorOnce.Do(func() {
		// Low-accuracy mode keeps only trigram models: about 18 MiB per
		// process instead of roughly 390 MiB, with comparable accuracy on
		// headline-length news text.
		detector = lingua.NewLanguageDetectorBuilder().
			FromLanguages(detectedLanguages...).
			WithLowAccuracyMode().
			Build()
	})
	return detector
}

// resolveLanguage returns a declared BCP 47 language when the source supplied
// one, and otherwise detects it from the article text. It never defaults to
// English: an unreadable or too-short fragment returns "und" (undetermined).
func resolveLanguage(declared, title, lede string) string {
	if tag, ok := declaredLanguage(declared); ok {
		return tag
	}
	text := strings.TrimSpace(strings.Join([]string{title, lede}, " "))
	if utf8.RuneCountInString(text) < minimumDetectRunes {
		return "und"
	}
	// One confidence pass supplies both the winner and its score; asking
	// DetectLanguageOf and then ComputeLanguageConfidence computed the same
	// n-gram statistics twice.
	values := languageDetector().ComputeLanguageConfidenceValues(text)
	if len(values) < 2 {
		return "und"
	}
	top := values[0]
	if top.Value() < minimumDetectConfidence || top.Value()-values[1].Value() < minimumDetectMargin {
		return "und"
	}
	lang := top.Language()
	iso := strings.ToLower(strings.TrimSpace(lang.IsoCode639_1().String()))
	if iso == "" || iso == "und" {
		return "und"
	}
	return iso
}

// declaredLanguage validates a source-declared language tag and returns its
// base language ("en-AU" -> "en"). Empty or invalid tags are not treated as a
// declaration.
func declaredLanguage(declared string) (string, bool) {
	declared = strings.TrimSpace(declared)
	if declared == "" {
		return "", false
	}
	tag, err := language.Parse(declared)
	if err != nil {
		return "", false
	}
	base, _ := tag.Base()
	iso := strings.ToLower(strings.TrimSpace(base.String()))
	if iso == "" || iso == "und" {
		return "", false
	}
	return iso, true
}
