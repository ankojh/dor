package decide

import "github.com/ankojh/dor/config"

func budgetConfigForTests() config.Budget {
	b := config.DefaultBudget()
	b.DailyUSD = 1000
	for k, v := range b.TokenBuckets {
		v.Capacity = 1e12
		b.TokenBuckets[k] = v
	}
	return b
}
