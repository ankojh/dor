package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func constrained() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []any{"title", "n", "x", "tags", "nested"},
		"properties": map[string]any{
			"title":  map[string]any{"type": "string", "minLength": 2, "maxLength": 10, "description": "A title."},
			"n":      map[string]any{"type": "integer", "minimum": 1, "maximum": 5},
			"x":      map[string]any{"type": "number", "minimum": -1.5, "maximum": 2.5},
			"tags":   map[string]any{"type": "array", "minItems": 1, "maxItems": 2, "items": map[string]any{"type": "string", "pattern": "^[a-z]+$", "enum": []any{"a", "b"}}},
			"nested": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"score": map[string]any{"type": "integer", "minimum": 0, "maximum": 3}}, "required": []any{"score"}},
		},
	}
}

func hasKeyDeep(v any, keys ...string) []string {
	var found []string
	var walk func(any, string)
	walk = func(v any, path string) {
		switch t := v.(type) {
		case map[string]any:
			for k, c := range t {
				for _, bad := range keys {
					if k == bad {
						found = append(found, path+"."+k)
					}
				}
				if k == "properties" {
					if props, ok := c.(map[string]any); ok {
						for name, p := range props {
							walk(p, path+".properties."+name)
						}
					}
					continue
				}
				walk(c, path+"."+k)
			}
		}
	}
	walk(v, "")
	return found
}

var constraintKeys = []string{"minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "pattern", "format"}

func TestSanitizeSchemaStripsConstraintsIntoDescriptions(t *testing.T) {
	orig := constrained()
	before, _ := json.Marshal(orig)
	clean := SanitizeSchema(orig)
	if after, _ := json.Marshal(orig); string(after) != string(before) {
		t.Error("SanitizeSchema mutated its input")
	}
	if found := hasKeyDeep(clean, constraintKeys...); len(found) != 0 {
		t.Errorf("constraint keywords survived: %v", found)
	}
	if bad := UnsupportedSchemaKeywords(clean); len(bad) != 0 {
		t.Errorf("unsupported keywords survived: %v", bad)
	}
	props := clean["properties"].(map[string]any)
	desc := func(name string) string { return props[name].(map[string]any)["description"].(string) }
	for name, want := range map[string][]string{
		"title": {"A title.", "At least 2 characters.", "At most 10 characters."},
		"n":     {"Minimum 1.", "Maximum 5."},
		"x":     {"Minimum -1.5.", "Maximum 2.5."},
		"tags":  {"At least 1 items.", "At most 2 items."},
	} {
		for _, w := range want {
			if !strings.Contains(desc(name), w) {
				t.Errorf("%s description %q lacks %q", name, desc(name), w)
			}
		}
	}
	items := props["tags"].(map[string]any)["items"].(map[string]any)
	if !strings.Contains(items["description"].(string), "Must match pattern ^[a-z]+$.") {
		t.Errorf("items description = %v", items["description"])
	}
	// Supported structure is preserved.
	if clean["type"] != "object" || clean["additionalProperties"] != false || len(clean["required"].([]any)) != 5 {
		t.Errorf("structure lost: %v", clean)
	}
	if enum := items["enum"].([]any); len(enum) != 2 {
		t.Errorf("enum = %v", enum)
	}
	nested := props["nested"].(map[string]any)["properties"].(map[string]any)["score"].(map[string]any)
	if !strings.Contains(nested["description"].(string), "Maximum 3.") {
		t.Errorf("nested description = %v", nested["description"])
	}
}

func TestSanitizeDropsUnknownKeywords(t *testing.T) {
	clean := SanitizeSchema(map[string]any{"type": "object", "$schema": "x", "default": 1, "oneOf": []any{}, "properties": map[string]any{"a": map[string]any{"type": "string", "default": "z"}}})
	if bad := UnsupportedSchemaKeywords(clean); len(bad) != 0 {
		t.Errorf("unsupported = %v", bad)
	}
	if bad := UnsupportedSchemaKeywords(map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string", "maxLength": 3}}}); len(bad) != 1 || !strings.Contains(bad[0], "properties.a.maxLength") {
		t.Errorf("raw schema unsupported = %v", bad)
	}
}

func TestValidateValueCatchesViolations(t *testing.T) {
	schema := constrained()
	good := map[string]any{"title": "Hello", "n": float64(3), "x": 1.0, "tags": []any{"a"}, "nested": map[string]any{"score": float64(2)}}
	if err := ValidateValue(schema, good); err != nil {
		t.Fatalf("valid value rejected: %v", err)
	}
	mutate := func(f func(m map[string]any)) map[string]any {
		raw, _ := json.Marshal(good)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		f(m)
		return m
	}
	cases := map[string]struct {
		v    map[string]any
		path string
	}{
		"integer above max": {mutate(func(m map[string]any) { m["n"] = 9.0 }), "n"},
		"integer below min": {mutate(func(m map[string]any) { m["n"] = 0.0 }), "n"},
		"non integer":       {mutate(func(m map[string]any) { m["n"] = 1.5 }), "n"},
		"number above max":  {mutate(func(m map[string]any) { m["x"] = 2.6 }), "x"},
		"string too long":   {mutate(func(m map[string]any) { m["title"] = "12345678901" }), "title"},
		"string too short":  {mutate(func(m map[string]any) { m["title"] = "a" }), "title"},
		"too many items":    {mutate(func(m map[string]any) { m["tags"] = []any{"a", "b", "a"} }), "tags"},
		"too few items":     {mutate(func(m map[string]any) { m["tags"] = []any{} }), "tags"},
		"enum":              {mutate(func(m map[string]any) { m["tags"] = []any{"z"} }), "tags[0]"},
		"nested":            {mutate(func(m map[string]any) { m["nested"] = map[string]any{"score": 7.0} }), "nested.score"},
		"missing required":  {mutate(func(m map[string]any) { delete(m, "n") }), "n"},
		"extra property":    {mutate(func(m map[string]any) { m["surprise"] = 1 }), "surprise"},
		"wrong type":        {mutate(func(m map[string]any) { m["title"] = 5.0 }), "title"},
	}
	for name, c := range cases {
		err := ValidateValue(schema, c.v)
		var se *SchemaError
		if !errors.As(err, &se) {
			t.Errorf("%s: err = %v, want *SchemaError", name, err)
			continue
		}
		if se.Path != c.path {
			t.Errorf("%s: path = %q, want %q (%v)", name, se.Path, c.path, err)
		}
		if !strings.HasPrefix(err.Error(), "llm: schema violation") {
			t.Errorf("%s: message = %q", name, err.Error())
		}
	}
	if err := ValidateValue(nil, 5); err != nil {
		t.Errorf("nil schema accepts everything: %v", err)
	}
	if err := ValidateValue(schema, "not an object"); err == nil {
		t.Error("a non-object must fail an object schema")
	}
	if (&SchemaError{Detail: "d"}).Error() != "llm: schema violation: d" {
		t.Error("pathless message")
	}
}

func TestProductionJobsHaveCleanWireSchemas(t *testing.T) {
	for name, job := range productionJobs() {
		clean := SanitizeSchema(job.Schema)
		if found := hasKeyDeep(clean, constraintKeys...); len(found) != 0 {
			t.Errorf("%s: %v", name, found)
		}
		if bad := UnsupportedSchemaKeywords(clean); len(bad) != 0 {
			t.Errorf("%s: %v", name, bad)
		}
	}
	// The limits are still stated to the model, and still enforced locally.
	brief := SanitizeSchema(BriefSchema())["properties"].(map[string]any)["title"].(map[string]any)
	if !strings.Contains(brief["description"].(string), "At most 90 characters.") {
		t.Errorf("title description = %v", brief["description"])
	}
	if err := ValidateValue(BriefSchema(), map[string]any{"title": strings.Repeat("x", 91), "brief": "b"}); err == nil {
		t.Error("a 91 character title must violate the original schema")
	}
}

func TestEstimateMatchesWhatClientReserves(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	fb := &fakeBudget{}
	c := newTestClient(t, api.srv.URL, fb, nil)
	for name, job := range productionJobs() {
		fb.checks = nil
		if _, err := c.Complete(context.Background(), job); err != nil {
			t.Fatal(err)
		}
		in, out := EstimateJobTokens(job)
		if fb.checks[0].EstimatedInputTokens != in || fb.checks[0].EstimatedOutputTokens != out {
			t.Errorf("%s: reserved %d/%d, EstimateJobTokens %d/%d", name, fb.checks[0].EstimatedInputTokens, fb.checks[0].EstimatedOutputTokens, in, out)
		}
	}
}

func TestWireSchemaOfProductionJobsAsSent(t *testing.T) {
	api := newFakeAPI(t, func(int) (int, string) { return 200, okMessage(`{}`) })
	c := newTestClient(t, api.srv.URL, &fakeBudget{}, nil)
	n := 0
	for name, job := range productionJobs() {
		if _, err := c.Complete(context.Background(), job); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		format := api.req(n).Body["output_config"].(map[string]any)["format"].(map[string]any)
		schema := format["schema"].(map[string]any)
		if found := hasKeyDeep(schema, constraintKeywordsAll()...); len(found) != 0 {
			t.Errorf("%s: wire schema carries %v", name, found)
		}
		if bad := UnsupportedSchemaKeywords(schema); len(bad) != 0 {
			t.Errorf("%s: %v", name, bad)
		}
		n++
	}
}

func constraintKeywordsAll() []string { return constraintKeys }

func TestValidateSchemaRejectsValidationKeywords(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{
			"type": "object", "additionalProperties": false, "required": []any{"a"},
			"properties": map[string]any{"a": map[string]any{"type": "string"}},
		}
	}
	cases := map[string]func(map[string]any){
		"format":      func(s map[string]any) { s["properties"].(map[string]any)["a"].(map[string]any)["format"] = "email" },
		"const":       func(s map[string]any) { s["properties"].(map[string]any)["a"].(map[string]any)["const"] = "x" },
		"multipleOf":  func(s map[string]any) { s["multipleOf"] = 2 },
		"allOf":       func(s map[string]any) { s["allOf"] = []any{map[string]any{"type": "string"}} },
		"anyOf":       func(s map[string]any) { s["anyOf"] = []any{map[string]any{"type": "string"}} },
		"oneOf":       func(s map[string]any) { s["oneOf"] = []any{map[string]any{"type": "string"}} },
		"not":         func(s map[string]any) { s["not"] = map[string]any{"type": "string"} },
		"$ref":        func(s map[string]any) { s["$ref"] = "#/x" },
		"$defs":       func(s map[string]any) { s["$defs"] = map[string]any{} },
		"open object": func(s map[string]any) { s["additionalProperties"] = true },
		"schema additionalProperties": func(s map[string]any) {
			s["additionalProperties"] = map[string]any{"type": "string"}
		},
	}
	for name, mutate := range cases {
		s := base()
		mutate(s)
		if err := ValidateSchema(s); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
}

func TestValidateSchemaChecksStructure(t *testing.T) {
	cases := map[string]map[string]any{
		"missing type":            {"type": "object", "additionalProperties": false, "required": []any{"a"}, "properties": map[string]any{"a": map[string]any{}}},
		"unclosed object":         {"type": "object", "required": []any{"a"}, "properties": map[string]any{"a": map[string]any{"type": "string"}}},
		"property not required":   {"type": "object", "additionalProperties": false, "required": []any{}, "properties": map[string]any{"a": map[string]any{"type": "string"}}},
		"required unknown":        {"type": "object", "additionalProperties": false, "required": []any{"b"}, "properties": map[string]any{"a": map[string]any{"type": "string"}}},
		"empty enum":              {"type": "string", "enum": []any{}},
		"non-scalar enum":         {"type": "string", "enum": []any{map[string]any{}}},
		"enum type mismatch":      {"type": "string", "enum": []any{1}},
		"array without items":     {"type": "array"},
		"bad type":                {"type": "null"},
		"array min above max":     {"type": "array", "items": map[string]any{"type": "string"}, "minItems": 3, "maxItems": 2},
		"string min above max":    {"type": "string", "minLength": 3, "maxLength": 2},
		"pattern is not a regexp": {"type": "string", "pattern": "["},
	}
	for name, s := range cases {
		if err := ValidateSchema(s); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
}
