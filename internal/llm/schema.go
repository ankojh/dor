package llm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// Supported structured-output schema keywords. Everything else is stripped
// from the wire schema: the API rejects unknown keywords with a 400, so a
// client that sends maxLength/minimum/maxItems cannot make a live call.
var supportedSchemaKeywords = map[string]bool{
	"type":                 true,
	"properties":           true,
	"required":             true,
	"additionalProperties": true,
	"items":                true,
	"enum":                 true,
	"description":          true,
}

// SchemaError is a typed post-decode constraint violation.
type SchemaError struct {
	Path   string
	Detail string
}

// Error implements error.
func (e *SchemaError) Error() string {
	if e.Path == "" {
		return "llm: schema violation: " + e.Detail
	}
	return fmt.Sprintf("llm: schema violation at %s: %s", e.Path, e.Detail)
}

// UnsupportedSchemaKeywords returns dotted paths of schema keywords the API
// does not support. The mock Messages server uses it to reject a request the
// real API would reject.
func UnsupportedSchemaKeywords(schema map[string]any) []string {
	var out []string
	walkSchema(schema, "output_config.format.schema", func(key, path string) {
		if !supportedSchemaKeywords[key] {
			out = append(out, path+"."+key)
		}
	})
	sort.Strings(out)
	return out
}

func walkSchema(node map[string]any, path string, visit func(key, path string)) {
	for key, value := range node {
		visit(key, path)
		switch key {
		case "properties":
			if props, ok := value.(map[string]any); ok {
				for name, prop := range props {
					if p, ok := prop.(map[string]any); ok {
						walkSchema(p, path+".properties."+name, visit)
					}
				}
			}
		case "items":
			if items, ok := value.(map[string]any); ok {
				walkSchema(items, path+".items", visit)
			}
		}
	}
}

// SanitizeSchema returns a deep copy safe for the structured-outputs API. It
// removes the constraint keywords it handles locally and appends the
// constraint to the description, so the model still sees the limit while Go
// validates it. Call ValidateSchema first: it is the fail-closed gate for
// unsupported keywords and invalid structure, and buildParams always does.
func SanitizeSchema(schema map[string]any) map[string]any {
	return sanitizeSchema(schema)
}

func sanitizeSchema(schema map[string]any) map[string]any {
	out := make(map[string]any, len(schema))
	for key, value := range schema {
		switch key {
		case "properties":
			props, _ := value.(map[string]any)
			clean := make(map[string]any, len(props))
			for name, prop := range props {
				if p, ok := prop.(map[string]any); ok {
					clean[name] = sanitizeSchema(p)
				} else {
					clean[name] = prop
				}
			}
			out[key] = clean
		case "items":
			if items, ok := value.(map[string]any); ok {
				out[key] = sanitizeSchema(items)
			} else {
				out[key] = value
			}
		case "required", "enum", "additionalProperties", "description", "type":
			out[key] = value
		case "minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "pattern", "format":
			// moved into the description below
		default:
			// Unknown keyword: drop it rather than send an invalid request.
		}
	}
	if desc := constraintDescription(schema); desc != "" {
		base, _ := out["description"].(string)
		if base != "" {
			out["description"] = base + " " + desc
		} else {
			out["description"] = desc
		}
	}
	return out
}

func constraintDescription(schema map[string]any) string {
	var parts []string
	if v, ok := numberValue(schema["minLength"]); ok {
		parts = append(parts, fmt.Sprintf("At least %s characters.", trimNum(v)))
	}
	if v, ok := numberValue(schema["maxLength"]); ok {
		parts = append(parts, fmt.Sprintf("At most %s characters.", trimNum(v)))
	}
	if v, ok := numberValue(schema["minItems"]); ok {
		parts = append(parts, fmt.Sprintf("At least %s items.", trimNum(v)))
	}
	if v, ok := numberValue(schema["maxItems"]); ok {
		parts = append(parts, fmt.Sprintf("At most %s items.", trimNum(v)))
	}
	if v, ok := numberValue(schema["minimum"]); ok {
		parts = append(parts, fmt.Sprintf("Minimum %s.", trimNum(v)))
	}
	if v, ok := numberValue(schema["maximum"]); ok {
		parts = append(parts, fmt.Sprintf("Maximum %s.", trimNum(v)))
	}
	if v, ok := schema["pattern"].(string); ok && v != "" {
		parts = append(parts, "Must match pattern "+v+".")
	}
	return strings.Join(parts, " ")
}

func trimNum(v float64) string {
	if v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%g", v)
}

// ValidateSchema enforces the narrow schema contract this client supports.
// Unsupported validation keywords are rejected rather than silently dropped.
func ValidateSchema(schema map[string]any) error {
	return validateSchemaNode(schema, "schema")
}

var allowedSchemaKeywords = map[string]bool{
	"type":                 true,
	"properties":           true,
	"required":             true,
	"additionalProperties": true,
	"items":                true,
	"enum":                 true,
	"description":          true,
	"minimum":              true,
	"maximum":              true,
	"minLength":            true,
	"maxLength":            true,
	"minItems":             true,
	"maxItems":             true,
	"pattern":              true,
}

func validateSchemaNode(node map[string]any, path string) error {
	if node == nil {
		return fmt.Errorf("llm: schema %s is not an object", path)
	}
	for key := range node {
		if !allowedSchemaKeywords[key] {
			return fmt.Errorf("llm: schema %s uses unsupported keyword %q", path, key)
		}
	}
	typ, ok := node["type"].(string)
	if !ok {
		return fmt.Errorf("llm: schema %s has no string type", path)
	}
	switch typ {
	case "object":
		props, ok := node["properties"].(map[string]any)
		if !ok {
			return fmt.Errorf("llm: object %s needs properties", path)
		}
		ap, ok := node["additionalProperties"].(bool)
		if !ok || ap {
			return fmt.Errorf("llm: object %s must be closed with additionalProperties: false", path)
		}
		req, ok := node["required"].([]any)
		if !ok {
			return fmt.Errorf("llm: object %s needs a required list", path)
		}
		required := make(map[string]bool, len(req))
		for _, r := range req {
			name, ok := r.(string)
			if !ok {
				return fmt.Errorf("llm: object %s required entries must be strings", path)
			}
			required[name] = true
			if _, exists := props[name]; !exists {
				return fmt.Errorf("llm: object %s requires unknown property %q", path, name)
			}
		}
		for name := range props {
			if !required[name] {
				return fmt.Errorf("llm: object %s property %q is not required", path, name)
			}
		}
		for name, prop := range props {
			p, ok := prop.(map[string]any)
			if !ok {
				return fmt.Errorf("llm: object %s property %q is not a schema", path, name)
			}
			if err := validateSchemaNode(p, path+".properties."+name); err != nil {
				return err
			}
		}
		if _, ok := node["items"]; ok {
			return fmt.Errorf("llm: object %s cannot have items", path)
		}
	case "array":
		items, ok := node["items"].(map[string]any)
		if !ok {
			return fmt.Errorf("llm: array %s needs items", path)
		}
		if err := validateSchemaNode(items, path+".items"); err != nil {
			return err
		}
		if _, ok := node["properties"]; ok {
			return fmt.Errorf("llm: array %s cannot have properties", path)
		}
		if _, ok := node["additionalProperties"]; ok {
			return fmt.Errorf("llm: array %s cannot have additionalProperties", path)
		}
		if _, ok := node["required"]; ok {
			return fmt.Errorf("llm: array %s cannot have required", path)
		}
	case "string", "integer", "number", "boolean":
		if _, ok := node["properties"]; ok {
			return fmt.Errorf("llm: %s %s cannot have properties", typ, path)
		}
		if _, ok := node["items"]; ok {
			return fmt.Errorf("llm: %s %s cannot have items", typ, path)
		}
		if _, ok := node["required"]; ok {
			return fmt.Errorf("llm: %s %s cannot have required", typ, path)
		}
		if _, ok := node["additionalProperties"]; ok {
			return fmt.Errorf("llm: %s %s cannot have additionalProperties", typ, path)
		}
	default:
		return fmt.Errorf("llm: schema %s has unsupported type %q", path, typ)
	}

	if err := validateConstraintTypes(node, typ, path); err != nil {
		return err
	}
	if enum, ok := node["enum"]; ok {
		values, ok := enum.([]any)
		if !ok || len(values) == 0 {
			return fmt.Errorf("llm: %s enum must be a non-empty array", path)
		}
		for i, v := range values {
			if !scalarEnumValue(v) {
				return fmt.Errorf("llm: %s enum[%d] must be a scalar", path, i)
			}
			if !valueMatchesType(v, typ) {
				return fmt.Errorf("llm: %s enum[%d] does not match type %s", path, i, typ)
			}
		}
	}
	if desc, ok := node["description"]; ok {
		if _, ok := desc.(string); !ok {
			return fmt.Errorf("llm: %s description must be a string", path)
		}
	}
	return nil
}

func validateConstraintTypes(node map[string]any, typ, path string) error {
	check := func(key string, allowed bool) error {
		if _, ok := node[key]; ok && !allowed {
			return fmt.Errorf("llm: %s keyword %q is not valid for type %s", path, key, typ)
		}
		return nil
	}
	if err := check("minLength", typ == "string"); err != nil {
		return err
	}
	if err := check("maxLength", typ == "string"); err != nil {
		return err
	}
	if err := check("pattern", typ == "string"); err != nil {
		return err
	}
	if err := check("minItems", typ == "array"); err != nil {
		return err
	}
	if err := check("maxItems", typ == "array"); err != nil {
		return err
	}
	if err := check("minimum", typ == "integer" || typ == "number"); err != nil {
		return err
	}
	if err := check("maximum", typ == "integer" || typ == "number"); err != nil {
		return err
	}
	for _, key := range []string{"minLength", "maxLength", "minItems", "maxItems"} {
		if v, ok := node[key]; ok {
			f, ok := numberValue(v)
			if !ok || f < 0 || f != math.Trunc(f) {
				return fmt.Errorf("llm: %s %s must be a non-negative integer", path, key)
			}
		}
	}
	for _, key := range []string{"minimum", "maximum"} {
		if v, ok := node[key]; ok {
			if _, ok := numberValue(v); !ok {
				return fmt.Errorf("llm: %s %s must be a number", path, key)
			}
		}
	}
	if lo, ok := numberValue(node["minLength"]); ok {
		if hi, ok := numberValue(node["maxLength"]); ok && hi < lo {
			return fmt.Errorf("llm: %s maxLength is below minLength", path)
		}
	}
	if lo, ok := numberValue(node["minItems"]); ok {
		if hi, ok := numberValue(node["maxItems"]); ok && hi < lo {
			return fmt.Errorf("llm: %s maxItems is below minItems", path)
		}
	}
	if lo, ok := numberValue(node["minimum"]); ok {
		if hi, ok := numberValue(node["maximum"]); ok && hi < lo {
			return fmt.Errorf("llm: %s maximum is below minimum", path)
		}
	}
	if v, ok := node["pattern"]; ok {
		pat, ok := v.(string)
		if !ok {
			return fmt.Errorf("llm: %s pattern must be a string", path)
		}
		if _, err := regexp.Compile(pat); err != nil {
			return fmt.Errorf("llm: %s pattern: %w", path, err)
		}
	}
	return nil
}

func scalarEnumValue(v any) bool {
	switch v.(type) {
	case string, bool, int, int64, float64, float32, json.Number:
		return true
	default:
		return false
	}
}

func valueMatchesType(v any, typ string) bool {
	switch typ {
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "integer":
		f, ok := numberValue(v)
		return ok && f == math.Trunc(f)
	case "number":
		_, ok := numberValue(v)
		return ok
	default:
		return false
	}
}

// ValidateValue checks a decoded JSON value against the original schema,
// including the constraints that were stripped from the wire. A violation is a
// *SchemaError.
func ValidateValue(schema map[string]any, value any) error {
	if schema == nil {
		return nil
	}
	if err := ValidateSchema(schema); err != nil {
		return err
	}
	return validateAt(schema, value, "")
}

func validateAt(schema map[string]any, value any, path string) error {
	if schema == nil {
		return nil
	}
	if enum, ok := schema["enum"].([]any); ok {
		if !enumContains(enum, value) {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("value %v is not in the enum", value)}
		}
	}
	typ, _ := schema["type"].(string)
	switch typ {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want object, got %T", value)}
		}
		if req, ok := schema["required"].([]any); ok {
			for _, r := range req {
				name, _ := r.(string)
				if _, present := obj[name]; !present {
					return &SchemaError{Path: join(path, name), Detail: "required field is missing"}
				}
			}
		}
		props, _ := schema["properties"].(map[string]any)
		if ap, ok := schema["additionalProperties"].(bool); ok && !ap {
			for key := range obj {
				if _, known := props[key]; !known {
					return &SchemaError{Path: join(path, key), Detail: "additional property is not allowed"}
				}
			}
		}
		for name, prop := range props {
			if v, ok := obj[name]; ok {
				if p, ok := prop.(map[string]any); ok {
					if err := validateAt(p, v, join(path, name)); err != nil {
						return err
					}
				}
			}
		}
	case "array":
		arr, ok := value.([]any)
		if !ok {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want array, got %T", value)}
		}
		if v, ok := numberValue(schema["minItems"]); ok && float64(len(arr)) < v {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want at least %s items, got %d", trimNum(v), len(arr))}
		}
		if v, ok := numberValue(schema["maxItems"]); ok && float64(len(arr)) > v {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want at most %s items, got %d", trimNum(v), len(arr))}
		}
		if items, ok := schema["items"].(map[string]any); ok {
			for i, v := range arr {
				if err := validateAt(items, v, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	case "string":
		s, ok := value.(string)
		if !ok {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want string, got %T", value)}
		}
		n := utf8.RuneCountInString(s)
		if v, ok := numberValue(schema["maxLength"]); ok && float64(n) > v {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want at most %s characters, got %d", trimNum(v), n)}
		}
		if v, ok := numberValue(schema["minLength"]); ok && float64(n) < v {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want at least %s characters, got %d", trimNum(v), n)}
		}
		if pattern, ok := schema["pattern"].(string); ok && pattern != "" {
			re, err := regexp.Compile(pattern)
			if err != nil {
				return &SchemaError{Path: path, Detail: "invalid schema pattern: " + err.Error()}
			}
			if !re.MatchString(s) {
				return &SchemaError{Path: path, Detail: fmt.Sprintf("value does not match pattern %s", pattern)}
			}
		}
	case "integer":
		f, ok := numberValue(value)
		if !ok || f != math.Trunc(f) {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want integer, got %T", value)}
		}
		if err := checkRange(schema, f, path); err != nil {
			return err
		}
	case "number":
		f, ok := numberValue(value)
		if !ok {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want number, got %T", value)}
		}
		if err := checkRange(schema, f, path); err != nil {
			return err
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return &SchemaError{Path: path, Detail: fmt.Sprintf("want boolean, got %T", value)}
		}
	}
	return nil
}

func checkRange(schema map[string]any, f float64, path string) error {
	if v, ok := numberValue(schema["minimum"]); ok && f < v {
		return &SchemaError{Path: path, Detail: fmt.Sprintf("%s is below the minimum %s", trimNum(f), trimNum(v))}
	}
	if v, ok := numberValue(schema["maximum"]); ok && f > v {
		return &SchemaError{Path: path, Detail: fmt.Sprintf("%s is above the maximum %s", trimNum(f), trimNum(v))}
	}
	return nil
}

func enumContains(enum []any, value any) bool {
	for _, e := range enum {
		if jsonEqual(e, value) {
			return true
		}
	}
	return false
}

func jsonEqual(a, b any) bool {
	af, aok := numberValue(a)
	bf, bok := numberValue(b)
	if aok && bok {
		return af == bf
	}
	return reflect.DeepEqual(a, b)
}

func numberValue(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
