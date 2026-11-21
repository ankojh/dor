package mockapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ankojh/dor/internal/decide/mockapi"
)

const body = `{"model":"typesafe/jev-1.14","state":{},"questions":{
 "n":{"type":"noul","criteria":{"true":"y","false":"n"}},
 "c":{"type":"choice","criteria":{"a":"A","b":"B","c":"C"}},
 "s":{"type":"score","criteria":["l0","l1","l2","l3"]}}}`

func post(t *testing.T, h http.Handler, payload string) (*http.Response, map[string]any) {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/api/alpha/decisions", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func clean() mockapi.Options {
	o := mockapi.DefaultOptions()
	o.Latency, o.RefusalEvery, o.MissingEvery, o.UnknownEvery = 0, 0, 0, 0
	return o
}

func TestAnswersFollowQuestionTypes(t *testing.T) {
	_, out := post(t, mockapi.NewHandler(clean()), body)
	if out["model"] != "typesafe/jev-1.14-20261014" {
		t.Errorf("served model = %v", out["model"])
	}
	answers := out["answers"].(map[string]any)
	n := answers["n"].(map[string]any)
	if n["type"] != "noul" || n["noul"].(float64) < 0 || n["noul"].(float64) > 1 {
		t.Errorf("noul = %v", n)
	}
	if n["noul"].(float64) == n["confidence"].(float64) {
		t.Errorf("noul probability and concentration must differ: %v", n)
	}
	c := answers["c"].(map[string]any)
	ch := c["choice"].(string)
	if ch != "a" && ch != "b" && ch != "c" {
		t.Errorf("choice = %v", c)
	}
	probs := c["probabilities"].(map[string]any)
	if len(probs) != 3 {
		t.Errorf("probabilities = %v", probs)
	}
	if probs[ch].(float64) == c["confidence"].(float64) {
		t.Errorf("selected probability and concentration must differ: %v", c)
	}
	s := answers["s"].(map[string]any)
	if sc := s["score"].(float64); sc < 0 || sc > 3 || len(s["probabilities"].(map[string]any)) != 4 {
		t.Errorf("score = %v", s)
	}
	usage := out["usage"].(map[string]any)
	if usage["cost"].(float64) <= 0 || usage["input_tokens"].(float64) <= 0 {
		t.Errorf("usage = %v", usage)
	}
}

func TestDeterministicPerBody(t *testing.T) {
	h := mockapi.NewHandler(clean())
	_, a := post(t, h, body)
	_, b := post(t, h, body)
	ja, _ := json.Marshal(a["answers"])
	jb, _ := json.Marshal(b["answers"])
	if !bytes.Equal(ja, jb) {
		t.Error("same body must give the same answers")
	}
}

func TestFailClosedArms(t *testing.T) {
	o := clean()
	o.RefusalEvery = 1 // (seed+i) % 1 == 0: every answer
	_, out := post(t, mockapi.NewHandler(o), body)
	for k, v := range out["answers"].(map[string]any) {
		if v.(map[string]any)["type"] != "refusal" {
			t.Errorf("%s = %v, want refusal", k, v)
		}
	}
	o = clean()
	o.MissingEvery = 1
	_, out = post(t, mockapi.NewHandler(o), body)
	if len(out["answers"].(map[string]any)) != 0 {
		t.Errorf("answers = %v, want none", out["answers"])
	}
	o = clean()
	o.UnknownEvery = 1
	_, out = post(t, mockapi.NewHandler(o), body)
	for k, v := range out["answers"].(map[string]any) {
		if v.(map[string]any)["type"] != "mystery" {
			t.Errorf("%s = %v, want an unknown type", k, v)
		}
	}
}

func TestBadJSONIsA400WithErrorBody(t *testing.T) {
	resp, out := post(t, mockapi.NewHandler(clean()), "{nope")
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	e := out["error"].(map[string]any)
	if e["code"].(float64) != 400 || e["message"] == "" {
		t.Errorf("error = %v", e)
	}
}
