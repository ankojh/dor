package llm

// Category definitions used by the enricher. They mirror decide.CategoryOptions
// (proto enum order); a test keeps the two lists equal so a prompt cannot drift
// from the schema.
var categoryDefs = []struct {
	Key        string
	Definition string
}{
	{"other", "None of the other categories fits, or the topic is unclear."},
	{"conflict", "War, armed conflict, terrorism, military action or civil unrest."},
	{"politics", "Elections, government, diplomacy, law, policy or political leadership."},
	{"economy", "Markets, business, trade, jobs, prices, inflation or public finance."},
	{"tech", "Technology, software, AI, telecoms, cyberattacks or digital policy."},
	{"climate", "Climate change, the environment, emissions, conservation or energy transition."},
	{"disaster", "Earthquakes, floods, storms, wildfires, accidents or humanitarian emergencies."},
	{"health", "Disease, medicine, public health, outbreaks or health systems."},
	{"science", "Research, space, discoveries or fundamental science."},
	{"sports", "Sport events, competitions, transfers or sporting governance."},
	{"culture", "Arts, entertainment, religion, lifestyle or media."},
}

// CategoryKeys returns the category keys in proto order.
func CategoryKeys() []string {
	out := make([]string, len(categoryDefs))
	for i, c := range categoryDefs {
		out[i] = c.Key
	}
	return out
}

func categoryEnum() []any {
	out := make([]any, len(categoryDefs))
	for i, c := range categoryDefs {
		out[i] = c.Key
	}
	return out
}
