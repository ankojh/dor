package main

import "testing"

func TestGatewayOnlyPipelineSelection(t *testing.T) {
	if got := pipelineSelection("gateway"); got != "" {
		t.Fatalf("gateway-only pipeline selection=%q, want empty agent list", got)
	}
	if !runsGateway("gateway") {
		t.Fatal("gateway-only selection must start gateway")
	}
	if runsPublisher("gateway") {
		t.Fatal("gateway-only selection must not start publisher")
	}
	if got := pipelineSelection("publisher,gateway, matcher"); got != "publisher,matcher" {
		t.Fatalf("pipelineSelection=%q", got)
	}
	if !runsGateway("publisher") || !runsPublisher("publisher") {
		t.Fatal("publisher selection must continue to include gateway")
	}
}
