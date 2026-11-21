package decide

import "fmt"

// Article is the classify/plot input. Full text is never stored or sent.
type Article struct {
	ID       string
	Title    string
	Lede     string
	Dateline string
	Source   string
}

// Candidate is one gazetteer candidate for the plot question.
type Candidate struct {
	ID         string
	Name       string
	Admin      string
	Country    string
	Population int64
}

// Story is the relate/match input: a story's brief plus its known facts.
type Story struct {
	ID       string
	Title    string
	Brief    string
	Entities []string
	Place    string
	Category string
}

// MaxCandidates is the number of gazetteer candidates sent with a plot
// question. More than eight makes the choice unreadable and inflates state.
const MaxCandidates = 8

type classifyState struct {
	Title  string `json:"title"`
	Lede   string `json:"lede"`
	Source string `json:"source"`
}

type candidateState struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Admin      string `json:"admin"`
	Country    string `json:"country"`
	Population int64  `json:"population"`
}

type plotState struct {
	Title      string           `json:"title"`
	Lede       string           `json:"lede"`
	Dateline   string           `json:"dateline"`
	Candidates []candidateState `json:"candidates"`
}

type storyState struct {
	Title    string   `json:"title"`
	Brief    string   `json:"brief"`
	Entities []string `json:"entities,omitempty"`
	Place    string   `json:"place,omitempty"`
	Category string   `json:"category,omitempty"`
}

type relateState struct {
	A storyState `json:"a"`
	B storyState `json:"b"`
}

type matchState struct {
	Title    string `json:"title"`
	Brief    string `json:"brief"`
	Place    string `json:"place,omitempty"`
	Category string `json:"category,omitempty"`
}

// candidateLabel is the human-readable definition shown for a candidate key.
func candidateLabel(c Candidate) string {
	label := c.Name
	if c.Admin != "" && c.Admin != c.Name {
		label += ", " + c.Admin
	}
	if c.Country != "" && c.Country != c.Name {
		label += ", " + c.Country
	}
	if c.Population > 0 {
		label += fmt.Sprintf(" (population %d)", c.Population)
	}
	return label
}

// ClassifyRequest builds the category question set:
// one choice over the 11 categories.
func ClassifyRequest(a Article) Request {
	return Request{
		State: classifyState{
			Title:  truncate(a.Title, 400),
			Lede:   truncate(a.Lede, 2000),
			Source: truncate(a.Source, 200),
		},
		Questions: map[string]Question{
			"category": {
				Type: TypeChoice,
				Instructions: withGuard("Assign the article to exactly one category. " +
					"Use the headline first and the lede to disambiguate. Choose other when none fits."),
				Criteria: optionsMap(CategoryOptions()),
			},
		},
	}
}

// PlotRequest builds the place question set:
// primary candidate (or none), precision, and one party question per
// candidate. Candidates beyond MaxCandidates are ignored.
func PlotRequest(a Article, candidates []Candidate) Request {
	if len(candidates) > MaxCandidates {
		candidates = candidates[:MaxCandidates]
	}
	state := plotState{
		Title:    truncate(a.Title, 400),
		Lede:     truncate(a.Lede, 2000),
		Dateline: truncate(a.Dateline, 200),
	}
	primaryCriteria := make(map[string]string, len(candidates)+1)
	for i, c := range candidates {
		state.Candidates = append(state.Candidates, candidateState{
			ID:         c.ID,
			Name:       c.Name,
			Admin:      c.Admin,
			Country:    c.Country,
			Population: c.Population,
		})
		primaryCriteria[fmt.Sprintf("c%d", i)] = candidateLabel(c)
	}
	primaryCriteria["none"] = "The article names no place for the event, or none of the candidates is the place."

	questions := map[string]Question{
		"primary": {
			Type: TypeChoice,
			Instructions: withGuard("Pick the primary place of the event the article reports: where it happened " +
				"or, for a policy or decision, where it takes effect. Prefer the specific place the event is about, " +
				"not a place merely mentioned. Choose none when no candidate is the event's place."),
			Criteria: primaryCriteria,
		},
		"precision": {
			Type: TypeChoice,
			Instructions: withGuard("How precisely does the article locate the event? Answer from the article's own wording. " +
				"country = only the country is stated; region = a state, province or broad area; city = a city or town; " +
				"street = a street, address or named building; point = an exact landmark or coordinate."),
			Criteria: optionsMap(PrecisionOptions()),
		},
	}
	for i := range candidates {
		c := candidates[i]
		questions[fmt.Sprintf("party_c%d", i)] = Question{
			Type: TypeNoul,
			Instructions: withGuard(fmt.Sprintf("Is %s a party to the event the article reports, rather than merely a place mentioned? "+
				"A party acted, was attacked, was directly affected, or is an organisation or government at the centre of the event.", candidateLabel(c))),
			Criteria: map[string]string{
				"true":  "The place is an actor, target or directly affected party in the event.",
				"false": "The place is only mentioned, used for comparison, or not involved.",
			},
		}
	}
	return Request{State: state, Questions: questions}
}

// RelateRequest builds the pair question set:
// related, type, direction and strength. Combinations are applied in code.
func RelateRequest(a, b Story) Request {
	return Request{
		State: relateState{
			A: toStoryState(a),
			B: toStoryState(b),
		},
		Questions: map[string]Question{
			"related": {
				Type: TypeNoul,
				Instructions: withGuard("Do these two news stories concern two distinct events that are connected: one affects, causes or responds to the other, " +
					"directly or through an indirect chain, or both share a specific condition or theme? Coverage of the same event does NOT count: " +
					"those articles belong to one story. A broad or coincidental overlap (same category, country or actor) does NOT count either."),
				Criteria: map[string]string{
					"true":  "The events are distinct and one affects, causes or responds to the other, possibly through an indirect chain; or they share a specific condition or theme that makes the connection meaningful.",
					"false": "They cover the same event, or only share a broad topic, category, country or actor with no specific connection.",
				},
			},
			"type": {
				Type: TypeChoice,
				Instructions: withGuard("Classify the connection between the two distinct events. causal: one event caused or materially contributed to the other, in either order. " +
					"reaction: one event is a response to the other. thematic: the events are distinct but share a specific condition or theme. " +
					"none: the same event, a broad or coincidental overlap, or no connection."),
				Criteria: optionsMap(RelationTypeOptions()),
			},
			"direction": {
				Type: TypeChoice,
				Instructions: withGuard("If one distinct event caused, drove or prompted the other, which direction is it? " +
					"a_to_b means story A's event acted on story B's; b_to_a means story B's acted on story A's. " +
					"Use unclear for a thematic connection, or when neither direction is supported."),
				Criteria: optionsMap(DirectionOptions()),
			},
			"strength": {
				Type: TypeScore,
				Instructions: withGuard("How strong and direct is the connection between the two distinct events? " +
					"0 = a specific shared condition or theme only; 1 = an indirect chain; 2 = a direct effect or response; 3 = an immediate, direct consequence."),
				Levels: StrengthLevels(),
			},
		},
	}
}

// MatchRequest builds the alert-match question set. The user's prompt lives in
// the instruction because question keys never reach the model.
func MatchRequest(s Story, prompt string) Request {
	return Request{
		State: matchState{
			Title:    truncate(s.Title, 400),
			Brief:    truncate(s.Brief, 2000),
			Place:    truncate(s.Place, 300),
			Category: truncate(s.Category, 50),
		},
		Questions: map[string]Question{
			"match": {
				Type: TypeNoul,
				Instructions: withGuard(fmt.Sprintf("Does this story match the user's alert prompt? "+
					"The prompt is: %q. Judge whether the story is about what the prompt asks for. "+
					"Be strict: a story that only mentions a keyword from the prompt without being about it does not match.", truncate(prompt, 1000))),
				Criteria: map[string]string{
					"true":  "The story is substantively about what the prompt asks for.",
					"false": "The story is unrelated, or only mentions a keyword from the prompt.",
				},
			},
		},
	}
}

func toStoryState(s Story) storyState {
	return storyState{
		Title:    truncate(s.Title, 400),
		Brief:    truncate(s.Brief, 2000),
		Entities: s.Entities,
		Place:    truncate(s.Place, 300),
		Category: truncate(s.Category, 50),
	}
}
