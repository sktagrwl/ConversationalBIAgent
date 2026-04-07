# Dataset Rules

These are structural properties of the data that every part of the system must respect — not dataset-specific quirks, but fundamental constraints that affect how queries must be written.

## Rule 1: Raw Tables Are Too Large to Scan

Raw CSV tables are not suitable for direct analytics queries. The largest raw table alone has 32M+ rows. Scanning them will be slow, memory-intensive, and unnecessary.

**Always use derived tables for analytics.** They are pre-aggregated, indexed, and purpose-built.

| Derived Table | Replaces | Use For |
|---|---|---|
| `order_products` | prior + train CSVs | Any product-order relationship analysis |
| `product_metrics` | order_products scan | Per-product stats (counts, reorder rate, cart position) |
| `order_metrics` | order_products scan | Per-order stats (basket size, reorder ratio) |
| `user_metrics` | order_metrics scan | Per-user aggregates |
| `department_metrics` | product_metrics scan | Per-department aggregates |

The agent's system prompt annotates raw tables as `RAW — avoid` and derived tables as `DERIVED — PREFER THIS` to enforce this at the LLM level.

## Rule 2: The eval_set Partition

The raw data splits rows across three ML evaluation partitions via an `eval_set` column:

| Partition | Description | Has basket data? |
|---|---|---|
| `prior` | All historical orders per user | Yes |
| `train` | Most recent order per user (ML label) | Yes |
| `test` | Most recent order for held-out users | **No** |

**Critical rules:**

- `order_products` unifies `prior` + `train`. It intentionally excludes `test` (no basket contents).
- **Never write `WHERE eval_set = 'prior'`** — this silently discards valid `train` rows and understates all metrics.
- Any filter on `eval_set` in a query is almost certainly a bug.
- There is no need to think about `eval_set` when querying derived tables — the partition is already resolved.

## Rule 4: No Absolute Timestamps — Temporal Reasoning Is Relative

There are **no calendar dates** in this dataset. Do not generate date/month/year filters.

| What exists | What it means |
|---|---|
| `order_number` | Sequential per-user order index (1, 2, 3…) |
| `days_since_prior_order` | Days elapsed since previous order (NULL for first order) |

**Correct patterns:**
- Purchase frequency → `AVG(days_since_prior_order)` (already in `user_metrics.avg_days_between_orders`)
- User timeline → `SUM(days_since_prior_order) OVER (PARTITION BY user_id ORDER BY order_number)`
- Recency → `MAX(order_number)` per user, not a date comparison
- Sequence analysis → use `order_number` as the time axis

**Forbidden patterns:**
- `WHERE order_date > ...` — column does not exist
- `GROUP BY month/year` — no calendar fields
- Any assumption that `days_since_prior_order` maps to a real date

### NULL semantics for days_since_prior_order

NULL occurs once per user: on `order_number = 1`. It means "no prior order" — not 0 days.

| Case | Pattern | Note |
|---|---|---|
| Average interval | `AVG(days_since_prior_order)` or `WHERE days_since_prior_order IS NOT NULL` | AVG auto-excludes NULLs; explicit filter also fine |
| Distribution | `WHERE days_since_prior_order IS NOT NULL` before `GROUP BY` | Always exclude first orders |
| Timeline SUM | `COALESCE(days_since_prior_order, 0)` inside `SUM OVER (...)` | First order = day 0 baseline; already in `fact_orders.days_since_first_order` |
| Reorder-only | `WHERE order_number > 1` | Removes first orders entirely |

`COALESCE(days_since_prior_order, 0)` inside `AVG()` or any aggregate is always a bug — it biases interval metrics toward 0.

## Rule 3: CSV/Parquet → Table Name Mapping

`data/<name>.csv` automatically gets converted to a `<name>.parquet` file, which becomes the `<name>` table in DuckDB on startup.

Deleting `data/warehouse.duckdb` and the generated `.parquet` files forces full re-materialization from CSVs on next app start. Do this when your underlying CSVs change.
