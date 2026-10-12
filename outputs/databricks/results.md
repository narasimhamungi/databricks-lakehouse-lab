### Drop-by-drop loads

| Drop | Bronze rows added (yfinance) | Silver yfinance inserted / updated | Silver Tiingo inserted / updated | dim_security new / changed / closed | Quarantined rows | Gates failed | Parity |
|---|---|---|---|---|---|---|---|
| 2026-09-04 | 698,665 | 698,665 / 0 | 78,852 / 0 | 503 / 0 / 0 | 1 | 0 | pending |
| 2026-09-08 | 370 | 370 / 0 | 42 / 0 | - | 0 | 0 | pending |
| 2026-09-09 | 740 | 370 / 0 | 42 / 0 | - | 0 | 0 | match |
| 2026-10-07 | 0 | 0 / 0 | 0 / 0 | 6 / 0 / 6 | 0 | 0 | match |

Run `503362447812204` with no new drop: 0 rows inserted and 0 updated across 14 table writes; 0 gates failed; parity match.

Run `591174523851659` with no new drop: 0 rows inserted and 0 updated across 14 table writes; 0 gates failed; parity match.

### Reconciliation (full snapshot)

| | Port (Spark) | Original (pandas) |
|---|---|---|
| compared | 63,575 | 63,575 |
| flagged | 1,262 | 1,262 |
| yfinance_only | 635,830 | 635,830 |
| tiingo_only | 15,361 | 15,361 |

Parity: **match**.

### Delta Lake evidence

- Time travel: row count by version of silver yfinance {'v0': 0, 'v1': 698665, 'v2': 698665, 'v3': 699035, 'v4': 699035, 'v5': 699405, 'v6': 699405, 'v7': 699405, 'v8': 699405, 'v9': 699405, 'v10': 699405, 'v11': 699405, 'v12': 699405, 'v13': 699405}.
- Schema enforcement:
  - extra column without mergeSchema: rejected.
  - malformed string into double: rejected.
  - numeric string into double: accepted (stored 3.25).
- Schema evolution: columns after mergeSchema append ['id', 'price', 'note'].
- OPTIMIZE on lakehouse_lab.lakehouse_bronze.yfinance_prices: 1 files -> 1. Query seconds before [1.732, 0.957, 0.869], after [0.915, 0.932, 0.81] (three runs each; first runs include cache warm-up).
- Bronze history: 19 commits, 0 of them automatic compactions (auto=true).
  - OPTIMIZE v9 at 2026-10-10 13:18:07 by workspace-owner (job: [dev narasimhamungi] lakehouse-delta-evidence): removed 3 files, added 1.
- Predictive optimization on the bronze schema: ENABLE (inherited from METASTORE metastore_aws_us_east_2).
- OPTIMIZE with auto-compaction off (8 single-row appends): 8 files -> 1.
- Silver yfinance OPTIMIZE v13 at 2026-10-10 15:07:08 by job Predictive Optimization Job-576b31d6-bf7b-4e2a-bf5d-b2e4484e2c05.
- VACUUM: {'retention': 'default (7 days)', 'files_eligible': 0, 'note': 'files replaced by OPTIMIZE are kept until the retention window passes'}.

### Declarative pipeline expectations

| Dataset | Expectation | Passed | Failed |
|---|---|---|---|
| dq_tiingo_expectations | adj_close_not_null | 78,978 | 0 |
| dq_tiingo_expectations | adj_close_positive | 78,978 | 0 |
| dq_tiingo_expectations | close_not_null | 78,978 | 0 |
| dq_tiingo_expectations | close_positive | 78,978 | 0 |
| dq_tiingo_expectations | date_not_null | 78,978 | 0 |
| dq_tiingo_expectations | high_ge_close | 78,978 | 0 |
| dq_tiingo_expectations | high_ge_low | 78,978 | 0 |
| dq_tiingo_expectations | high_ge_open | 78,978 | 0 |
| dq_tiingo_expectations | low_le_close | 78,978 | 0 |
| dq_tiingo_expectations | low_le_open | 78,978 | 0 |
| dq_tiingo_expectations | open_positive | 78,978 | 0 |
| dq_tiingo_expectations | ticker_not_null | 78,978 | 0 |
| dq_tiingo_expectations | volume_non_negative | 78,978 | 0 |
| dq_yfinance_expectations | adj_close_not_null | 699,775 | 0 |
| dq_yfinance_expectations | adj_close_positive | 699,775 | 0 |
| dq_yfinance_expectations | close_not_null | 699,775 | 0 |
| dq_yfinance_expectations | close_positive | 699,775 | 0 |
| dq_yfinance_expectations | date_not_null | 699,775 | 0 |
| dq_yfinance_expectations | high_ge_close | 699,775 | 0 |
| dq_yfinance_expectations | high_ge_low | 699,775 | 0 |
| dq_yfinance_expectations | high_ge_open | 699,775 | 0 |
| dq_yfinance_expectations | low_le_close | 699,775 | 0 |
| dq_yfinance_expectations | low_le_open | 699,774 | 1 |
| dq_yfinance_expectations | open_positive | 699,775 | 0 |
| dq_yfinance_expectations | ticker_not_null | 699,775 | 0 |
| dq_yfinance_expectations | volume_non_negative | 699,775 | 0 |

Rows dropped by expect_or_drop: dq_tiingo_expectations 0, dq_yfinance_expectations 0.
