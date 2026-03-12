"""
Spotify User Platform - Auth Analytics Report
Queries the dbt marts directly from DuckDB and surfaces key insights.

Run after: python data_ingestion/generate_data.py && cd dbt_project && dbt run
"""

import duckdb
import sys

DB_PATH = "spotify_iam.duckdb"


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def run_analysis():
    con = duckdb.connect(DB_PATH, read_only=True)

    # ── 1. Overall auth health ───────────────────────────────────────────────
    section("📊 Overall Auth Health")
    result = con.execute("""
        select
            count(distinct metric_date)                  as days_covered,
            sum(users_attempted_login)                   as total_login_attempts,
            sum(successful_logins)                       as total_successes,
            round(avg(login_conversion_rate) * 100, 2)  as avg_conversion_pct,
            round(avg(mfa_pass_rate) * 100, 2)          as avg_mfa_pass_pct,
            sum(password_reset_sessions)                 as total_password_resets,
            sum(mfa_blocked_sessions)                    as total_mfa_blocks
        from main.fct_daily_auth_metrics
    """).fetchone()

    labels = ["Days", "Login Attempts", "Successes",
              "Avg Conversion %", "Avg MFA Pass %",
              "Password Resets", "MFA Blocks"]
    for label, val in zip(labels, result):
        print(f"  {label:<22}: {val:>12}")

    # ── 2. Conversion by subscription tier ──────────────────────────────────
    section("🎵 Login Conversion Rate by Subscription Type")
    rows = con.execute("""
        select
            subscription_type,
            round(avg(login_conversion_rate) * 100, 2)  as conversion_pct,
            sum(users_attempted_login)                   as attempts
        from main.fct_daily_auth_metrics
        where subscription_type is not null
        group by 1
        order by conversion_pct desc
    """).fetchall()
    print(f"  {'Subscription':<30} {'Conv %':>8} {'Attempts':>12}")
    for r in rows:
        print(f"  {r[0]:<30} {r[1]:>8} {r[2]:>12,}")

    # ── 3. Friction by device ────────────────────────────────────────────────
    section("📱 Auth Friction by Device Category")
    rows = con.execute("""
        select
            device_category,
            round(avg(login_conversion_rate) * 100, 2)  as conversion_pct,
            round(avg(avg_errors_per_session), 3)        as avg_errors_per_session,
            sum(mfa_blocked_sessions)                    as mfa_blocks
        from main.fct_daily_auth_metrics
        where device_category is not null
        group by 1
        order by conversion_pct asc
    """).fetchall()
    print(f"  {'Device':<15} {'Conv %':>8} {'Avg Errors':>12} {'MFA Blocks':>12}")
    for r in rows:
        print(f"  {r[0]:<15} {r[1]:>8} {r[2]:>12} {r[3]:>12,}")

    # ── 4. Friction segments ─────────────────────────────────────────────────
    section("🔥 User Friction Segments")
    rows = con.execute("""
        select
            friction_segment,
            count(*)                            as user_count,
            round(avg(friction_score), 1)       as avg_score,
            round(avg(failure_rate) * 100, 2)   as avg_failure_rate_pct,
            round(avg(password_reset_count), 2) as avg_resets
        from main.user_friction_scores
        group by 1
        order by avg_score desc
    """).fetchall()
    print(f"  {'Segment':<22} {'Users':>8} {'Avg Score':>10} {'Fail%':>8} {'Resets':>8}")
    for r in rows:
        print(f"  {r[0]:<22} {r[1]:>8,} {r[2]:>10} {r[3]:>8} {r[4]:>8}")

    # ── 5. Top friction countries ────────────────────────────────────────────
    section("🌍 Top 5 Countries by Auth Failure Rate")
    rows = con.execute("""
        select
            country_code,
            round(
                sum(sessions_with_failure)::float / nullif(sum(total_sessions), 0) * 100,
                2
            )                            as failure_rate_pct,
            sum(total_sessions)          as total_sessions
        from main.fct_daily_auth_metrics
        where country_code is not null
        group by 1
        order by failure_rate_pct desc
        limit 5
    """).fetchall()
    print(f"  {'Country':<10} {'Failure %':>12} {'Sessions':>12}")
    for r in rows:
        print(f"  {r[0]:<10} {r[1]:>12.2f} {r[2]:>12,}")

    # ── 6. Daily conversion trend ────────────────────────────────────────────
    section("📈 Daily Conversion Rate Trend (first 7 days)")
    rows = con.execute("""
        select
            metric_date,
            round(avg(login_conversion_rate) * 100, 2) as conversion_pct,
            sum(users_attempted_login)                  as attempts
        from main.fct_daily_auth_metrics
        group by 1
        order by 1
        limit 7
    """).fetchall()
    print(f"  {'Date':<12} {'Conv %':>8} {'Attempts':>12}")
    for r in rows:
        bar = "█" * int((r[1] or 0) / 2)
        print(f"  {str(r[0])[:10]:<12} {r[1]:>7}% {r[2]:>10,}  {bar}")
    con.close()
    print(f"\n✅ Analysis complete.\n")


if __name__ == "__main__":
    try:
        run_analysis()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure you've run: python data_ingestion/generate_data.py && cd dbt_project && dbt run")
        sys.exit(1)