## Route History Table (Long-Term Routing Memory)

Separate from live pheromones, add a `route_history` table that never deletes:

```sql
route_history (
    channel_id TEXT PRIMARY KEY,
    first_seen REAL,
    last_success REAL,
    last_failure REAL,
    total_successes INTEGER,
    total_failures INTEGER,
    best_fee_ppm INTEGER,
    last_fee_ppm INTEGER
)
```

**Rationale**: Live pheromones drive real-time fee decisions and should evaporate aggressively. But two signals are lost today:
1. "This route worked before but went quiet" — recovery signal after outages/rebalances
2. "This route has never worked" — negative knowledge (don't bother trying)

A persistent history table lets the advisor and planner query long-term routing memory without ghost-influencing live fee decisions. Needs LRU eviction to avoid noise from long-closed channels.

**Related**: Pheromone persistence was added in commit 12b3eab.
