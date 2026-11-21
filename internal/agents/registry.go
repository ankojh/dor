package agents

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Registry maps names to agents so --agents can select a subset.
type Registry struct {
	mu     sync.RWMutex
	agents map[string]Agent
	order  []string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{agents: make(map[string]Agent)}
}

// Register adds an agent. Names must be unique.
func (r *Registry) Register(a Agent) error {
	if a == nil {
		return fmt.Errorf("agents: cannot register a nil agent")
	}
	name := a.Name()
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("agents: cannot register an agent without a name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.agents[name]; ok {
		return fmt.Errorf("agents: %q is already registered", name)
	}
	r.agents[name] = a
	r.order = append(r.order, name)
	return nil
}

// Names returns registered names in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.order...)
}

// Select resolves all, none or a comma-separated list of agent names.
func (r *Registry) Select(selection string) ([]Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	selection = strings.TrimSpace(selection)
	if selection == "" || selection == "none" {
		return nil, nil
	}
	if selection == "all" {
		names := append([]string(nil), r.order...)
		sort.Strings(names)
		out := make([]Agent, 0, len(names))
		for _, name := range names {
			out = append(out, r.agents[name])
		}
		return out, nil
	}
	seen := make(map[string]bool)
	var out []Agent
	for _, raw := range strings.Split(selection, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if name == "all" {
			return nil, fmt.Errorf("agents: \"all\" cannot be combined with explicit names")
		}
		if seen[name] {
			continue
		}
		a, ok := r.agents[name]
		if !ok {
			return nil, fmt.Errorf("agents: unknown agent %q (known: %s)", name, strings.Join(r.order, ", "))
		}
		seen[name] = true
		out = append(out, a)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("agents: selection %q resolved to no agents", selection)
	}
	return out, nil
}
