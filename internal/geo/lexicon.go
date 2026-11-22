package geo

import (
	_ "embed"
	"strings"
)

// These word lists are only negative evidence for otherwise unqualified,
// single-token matches. Explicit location evidence can still admit a place.
//
//go:embed data/common-english.txt
var commonEnglishData string

//go:embed data/given-names.txt
var givenNameData string

//go:embed data/proper-words.txt
var properWordData string

var commonEnglish = vocabularyLines(commonEnglishData)
var censusGivenNames = vocabularyLines(givenNameData)
var properWords = vocabularyLines(properWordData)

func vocabularyLines(data string) map[string]bool {
	words := make(map[string]bool)
	for _, word := range strings.Fields(data) {
		words[word] = true
	}
	return words
}
