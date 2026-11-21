// Command llm-smoke verifies prompt caching against the live Claude API.
//
// It makes two identical Haiku calls and prints usage.cache_read_input_tokens
// for the second. A non-zero read proves the cached prefix is being reused.
// Without ANTHROPIC_API_KEY it prints "skipped: no ANTHROPIC_API_KEY" and exits
// 0, so `make llm-smoke` is safe in CI and on a machine with no key.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ankojh/dor/config"
	"github.com/ankojh/dor/internal/budget"
	"github.com/ankojh/dor/internal/llm"
)

func main() {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		fmt.Println("skipped: no ANTHROPIC_API_KEY")
		return
	}
	models, err := config.LoadModels("config/models.yaml")
	if err != nil {
		fail(err)
	}
	budgetCfg, err := config.LoadBudget("config/budget.yaml")
	if err != nil {
		fail(err)
	}
	limiter := budget.NewMemory(budgetCfg)
	client, err := llm.NewFromConfig(models, key, "", limiter)
	if err != nil {
		fail(err)
	}
	job := llm.EnricherJob(llm.Article{
		Title:  "Flooding in Jakarta displaces thousands",
		Lede:   "Days of rain have flooded low-lying districts of the Indonesian capital, officials said.",
		Source: "BBC World",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	fmt.Println("two identical Haiku calls; the second should read the cached prefix")
	var second llm.Result
	for i := 1; i <= 2; i++ {
		res, err := client.Complete(ctx, job)
		if err != nil {
			fail(err)
		}
		second = res
		fmt.Printf("call %d: input=%d cache_read=%d cache_creation=%d output=%d\n",
			i, res.Usage.InputTokens, res.Usage.CacheReadInputTokens, res.Usage.CacheCreationInputTokens, res.Usage.OutputTokens)
	}
	fmt.Printf("cache_read_input_tokens (second): %d\n", second.Usage.CacheReadInputTokens)
	if second.Usage.CacheReadInputTokens > 0 {
		fmt.Println("OK: cached prefix reused")
	} else {
		fmt.Println("WARN: no cache read on the second call; the prefix may be below the model minimum or changed between calls")
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "llm-smoke:", err)
	os.Exit(1)
}
