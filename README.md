# 🎵 Spotify User Platform — Auth Analytics Pipeline

> A production-style Analytics Engineering project modelling IAM authentication events to measure user friction, track login conversion, and surface data quality issues — built with **dbt + DuckDB + Python**.

This project mirrors the core work of Spotify's **User Platform** team: building robust data infrastructure around identity and access management (IAM) to help product and engineering teams understand where users experience friction, accurately measure auth conversions, and continuously improve the Spotify onboarding experience.

---

## 📐 Architecture

```
Raw Data (DuckDB)
    │
    ▼
┌─────────────────────────────────────┐
│  STAGING LAYER  (views)             │
│  stg_auth_events  stg_users         │
│  • Type casting  • Renaming         │
│  • Null guards   • Derived flags    │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  INTERMEDIATE LAYER  (ephemeral)    │
│  int_user_sessions                  │  ← Session reconstruction from event stream
│  int_auth_funnel                    │  ← Per-user, per-day funnel steps
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│  MARTS LAYER  (tables)              │
│  fct_daily_auth_metrics             │  ← BI dashboard source
│  user_friction_scores               │  ← User-level friction scoring
└─────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Tool | Purpose |
|------|---------|
| **dbt-core** | Data modelling, testing, documentation |
| **DuckDB** | Local analytical warehouse (BigQuery-compatible SQL) |
| **Python** | Synthetic data generation, analysis scripts |
| **dbt singular tests** | Custom data quality assertions |
| **dbt schema tests** | Column-level uniqueness, not-null, accepted-values |

> In a production Spotify environment, DuckDB → **BigQuery + GCS**, with dbt running on **Airflow/Cloud Composer** or **dbt Cloud**.

---

## 📂 Project Structure

```
spotify-user-platform-analytics/
│
├── data_ingestion/
│   └── generate_data.py          # Synthetic IAM event + user data generator
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_auth_events.sql   # Cleaned auth event stream
│   │   │   ├── stg_users.sql         # Cleaned user dimension
│   │   │   └── schema.yml            # Source + staging tests
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_user_sessions.sql   # Session reconstruction
│   │   │   └── int_auth_funnel.sql     # Step-by-step funnel
│   │   │
│   │   └── marts/
│   │       ├── fct_daily_auth_metrics.sql   # Daily auth KPIs
│   │       ├── user_friction_scores.sql     # Per-user friction scoring
│   │       └── schema.yml                   # Mart tests
│   │
│   └── tests/
│       ├── assert_no_future_events.sql          # No future-dated events
│       └── assert_conversion_rate_not_zero.sql  # Outlier detection
│
├── analysis/
│   └── auth_insights.py          # CLI report: friction, trends, segments
│
└── requirements.txt
```

---

## 🧪 Synthetic Data — Where Does the Data Come From?

This project uses **synthetic data generation** — all data is fabricated by `generate_data.py` using Python's `random` library. No real user data is used at any point.

This is a standard technique in data engineering for building and testing pipelines without needing access to production systems. Here's how it works:

| Library | Role |
|---------|------|
| `random` | The core engine — picks countries, devices, event outcomes using weighted probabilities that mirror realistic distributions (e.g. US = 30%, mobile = 58%) |
| `uuid` | Generates unique IDs like `usr_3f9a21bc04d1` to simulate real user and event identifiers |
| `datetime` + `timedelta` | Builds realistic timestamps spread across 30 days so the data behaves like a real time series |
| `duckdb` | Stores the fabricated records into `spotify_iam.duckdb` so dbt can query them like a real warehouse |

**Example:** a "login attempt" event isn't fetched from an API — it's created like this:

```python
random.choices(["login_success", "login_failure_bad_password", "mfa_triggered"],
               [0.72, 0.18, 0.10])[0]
```

This means 72% of attempts succeed, 18% fail with a bad password, and 10% trigger MFA — numbers chosen to produce a realistic-looking dataset.

The result: **500 users and ~30,000 auth events** that behave like real IAM data, allowing the full dbt pipeline, data quality tests, and friction scoring model to be developed and validated end-to-end.

---

## 🚀 Quick Start

### 1. Install dependencies
```bash
pip install duckdb dbt-core dbt-duckdb
```

### 2. Generate synthetic data
```bash
python data_ingestion/generate_data.py
# Loads 500 users and ~30,000 auth events into spotify_iam.duckdb
```

### 3. Run dbt models
```bash
cd dbt_project
dbt run --profiles-dir .
```

### 4. Run data quality tests
```bash
dbt test --profiles-dir .
```

### 5. View analysis
```bash
cd ..
python analysis/auth_insights.py
```

---

## 📊 Key Metrics Produced

### `fct_daily_auth_metrics`
Daily auth health sliced by country × device × subscription type:

| Metric | Description |
|--------|-------------|
| `login_conversion_rate` | % of login attempts that succeeded |
| `mfa_pass_rate` | % of MFA challenges that were completed |
| `avg_errors_per_session` | Mean error events per session |
| `password_reset_sessions` | Sessions that required a password reset |
| `mfa_blocked_sessions` | Sessions that failed at the MFA step |

### `user_friction_scores`
Per-user friction scoring (0–100):

| Score Range | Segment | Action |
|-------------|---------|--------|
| 60–100 | `high_friction` | Priority UX investigation |
| 20–59 | `medium_friction` | Monitor; A/B test improvements |
| 0–19 | `low_friction` | Baseline / control group |

**Friction score formula:**
```
friction_score = min(100,
    failure_rate          × 40   -- dominant signal: % of sessions that failed
  + mfa_blocked_count     ×  5   -- each MFA block adds minor friction
  + password_reset_count  × 10   -- resets indicate full lockout, weighted higher
  + mfa_penalty_if_blocked    5  -- flat penalty if MFA-enabled user was blocked
  + unverified_email_penalty  5  -- flat penalty for unverified email address
)
```

The `min(100, ...)` caps the score so all users sit on a clean 0–100 scale regardless of extreme values.

---

## 🧪 Data Quality Tests

The project ships **24 dbt tests** across 3 layers:

| Layer | Tests |
|-------|-------|
| Staging | `unique`, `not_null` on PKs; `accepted_values` on event_type and device_category |
| Marts | `not_null` on key columns; `accepted_values` on friction_segment |
| Singular | No future-dated events; conversion rate never below 10% over 7-day window |

---

## 🔄 Extending to Production (BigQuery)

To migrate from DuckDB to BigQuery, update `profiles.yml`:

```yaml
spotify_prod:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: spotify-user-platform
      dataset: analytics
      location: EU
      threads: 8
```

All SQL is BigQuery-compatible (standard SQL, no DuckDB-specific functions used beyond casting).

---

## 💡 Design Decisions

**Why ephemeral intermediate models?**  
Session reconstruction and funnel logic are complex enough to warrant their own layer for readability, but are only ever queried downstream — materialising them as tables would waste storage at Spotify's data volumes.

**Why a friction _score_ rather than just failure_rate?**  
A single metric loses nuance. A user with 3 MFA blocks and 0 password resets has a different problem than one with 0 MFA blocks and 3 resets. The composite score surfaces both patterns at different weights.

**Why DuckDB locally instead of BigQuery emulator?**  
DuckDB supports the same SQL dialect, runs fully in-process with no infrastructure, and is the fastest way to iterate on dbt models during development — mirroring how Spotify engineers develop locally before promoting to BigQuery.

**Why synthetic data?**  
Building pipelines against real production data requires access, compliance approvals, and risks exposing PII. Synthetic data generation allows the full pipeline to be developed, tested, and shared publicly without any of those constraints — while still producing statistically realistic results.

---

## 👩‍💻 Author

Built by Mina as a portfolio project to demonstrate analytics engineering skills relevant to Spotify's User Platform team.

Skills demonstrated: **dbt modelling (staging → intermediate → marts)** · **IAM/identity domain data** · **Synthetic data generation** · **Data quality testing** · **Funnel analysis & conversion metrics** · **Python data pipeline engineering** · **SQL (BigQuery-compatible)**