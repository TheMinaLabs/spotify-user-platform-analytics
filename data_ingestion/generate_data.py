"""
Synthetic IAM Event Data Generator for Spotify User Platform Analytics
Generates realistic auth events, user records, and device sessions.
"""

import random
import uuid
import json
import os
from datetime import datetime, timedelta
import duckdb

random.seed(42)

# ── Config ──────────────────────────────────────────────────────────────────
NUM_USERS        = 500
DAYS_OF_DATA     = 30
EVENTS_PER_DAY   = 1_000
OUTPUT_DB        = "spotify_iam.duckdb"

COUNTRIES        = ["US", "BR", "GB", "DE", "SE", "MX", "IN", "FR", "AU", "CA"]
COUNTRY_WEIGHTS  = [0.30, 0.15, 0.10, 0.08, 0.05, 0.07, 0.09, 0.06, 0.05, 0.05]
DEVICE_TYPES     = ["mobile_ios", "mobile_android", "desktop_windows", "desktop_mac", "web_browser", "smart_tv"]
DEVICE_WEIGHTS   = [0.30, 0.28, 0.15, 0.12, 0.10, 0.05]
SUBSCRIPTION     = ["free", "premium_individual", "premium_duo", "premium_family"]
SUB_WEIGHTS      = [0.55, 0.30, 0.08, 0.07]
AGE_GROUPS       = ["18-24", "25-34", "35-44", "45-54", "55+"]
AGE_WEIGHTS      = [0.25, 0.35, 0.22, 0.11, 0.07]

AUTH_FLOW = {
    # event_type: (next_possible_events with weights)
    "login_attempt":            [("login_success", 0.72), ("login_failure_bad_password", 0.18), ("login_failure_account_locked", 0.05), ("mfa_triggered", 0.05)],
    "mfa_triggered":            [("mfa_success", 0.82), ("mfa_failure", 0.12), ("mfa_timeout", 0.06)],
    "mfa_success":              [("login_success", 1.0)],
    "login_success":            [("token_issued", 1.0)],
    "token_issued":             [("session_active", 0.95), ("token_refresh", 0.05)],
    "token_refresh":            [("session_active", 0.97), ("session_expired", 0.03)],
    "session_active":           [("logout", 0.40), ("session_expired", 0.35), ("token_refresh", 0.25)],
    "password_reset_requested": [("password_reset_email_sent", 0.95), ("password_reset_failed", 0.05)],
    "password_reset_email_sent":[("password_reset_completed", 0.65), ("password_reset_expired", 0.35)],
}

ERROR_CODES = {
    "login_failure_bad_password":  "AUTH_001",
    "login_failure_account_locked":"AUTH_002",
    "mfa_failure":                 "MFA_001",
    "mfa_timeout":                 "MFA_002",
    "token_refresh":               None,
    "session_expired":             "SESSION_001",
    "password_reset_failed":       "PWD_001",
    "password_reset_expired":      "PWD_002",
}

# ── Generators ───────────────────────────────────────────────────────────────

def make_users(n: int) -> list[dict]:
    users = []
    base_date = datetime(2024, 1, 1)
    for i in range(n):
        created_at = base_date + timedelta(days=random.randint(0, 365))
        users.append({
            "user_id":           f"usr_{uuid.uuid4().hex[:12]}",
            "created_at":        created_at.isoformat(),
            "country":           random.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
            "subscription_type": random.choices(SUBSCRIPTION, SUB_WEIGHTS)[0],
            "age_group":         random.choices(AGE_GROUPS, AGE_WEIGHTS)[0],
            "email_verified":    random.choices([True, False], [0.92, 0.08])[0],
            "mfa_enabled":       random.choices([True, False], [0.35, 0.65])[0],
        })
    return users


def make_auth_events(users: list[dict], days: int, events_per_day: int) -> list[dict]:
    events = []
    base_date = datetime(2024, 6, 1)
    user_ids = [u["user_id"] for u in users]
    user_map  = {u["user_id"]: u for u in users}

    for day_offset in range(days):
        day = base_date + timedelta(days=day_offset)
        # ~15 % of days are weekends with lower volume
        volume = int(events_per_day * (0.65 if day.weekday() >= 5 else 1.0))
        # small growth trend
        volume = int(volume * (1 + day_offset * 0.001))

        for _ in range(volume):
            user_id     = random.choice(user_ids)
            user        = user_map[user_id]
            device_type = random.choices(DEVICE_TYPES, DEVICE_WEIGHTS)[0]
            session_id  = f"sess_{uuid.uuid4().hex[:16]}"
            ts          = day + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            # Decide entry point
            entry = random.choices(
                ["login_attempt", "token_refresh", "password_reset_requested"],
                [0.70, 0.25, 0.05]
            )[0]

            current = entry
            step     = 0
            while current and step < 8:
                events.append({
                    "event_id":      f"evt_{uuid.uuid4().hex[:16]}",
                    "session_id":    session_id,
                    "user_id":       user_id,
                    "event_type":    current,
                    "device_type":   device_type,
                    "country":       user["country"],
                    "subscription_type": user["subscription_type"],
                    "mfa_enabled":   user["mfa_enabled"],
                    "error_code":    ERROR_CODES.get(current),
                    "event_ts":      (ts + timedelta(seconds=step * random.randint(2, 30))).isoformat(),
                })
                nexts = AUTH_FLOW.get(current, [])
                if not nexts:
                    break
                nxt_events, nxt_weights = zip(*nexts)
                current = random.choices(nxt_events, nxt_weights)[0]
                step   += 1

    return events


# ── Load to DuckDB ────────────────────────────────────────────────────────────

def load_to_duckdb(users, events, db_path):
    con = duckdb.connect(db_path)

    con.execute("DROP TABLE IF EXISTS raw_users")
    con.execute("DROP TABLE IF EXISTS raw_auth_events")

    con.execute("""
        CREATE TABLE raw_users (
            user_id          VARCHAR,
            created_at       TIMESTAMP,
            country          VARCHAR,
            subscription_type VARCHAR,
            age_group        VARCHAR,
            email_verified   BOOLEAN,
            mfa_enabled      BOOLEAN
        )
    """)

    con.execute("""
        CREATE TABLE raw_auth_events (
            event_id         VARCHAR,
            session_id       VARCHAR,
            user_id          VARCHAR,
            event_type       VARCHAR,
            device_type      VARCHAR,
            country          VARCHAR,
            subscription_type VARCHAR,
            mfa_enabled      BOOLEAN,
            error_code       VARCHAR,
            event_ts         TIMESTAMP
        )
    """)

    # Batch insert
    user_rows = [(
        u["user_id"], u["created_at"], u["country"],
        u["subscription_type"], u["age_group"],
        u["email_verified"], u["mfa_enabled"]
    ) for u in users]

    event_rows = [(
        e["event_id"], e["session_id"], e["user_id"], e["event_type"],
        e["device_type"], e["country"], e["subscription_type"],
        e["mfa_enabled"], e["error_code"], e["event_ts"]
    ) for e in events]

    con.executemany("INSERT INTO raw_users VALUES (?,?,?,?,?,?,?)", user_rows)
    con.executemany("INSERT INTO raw_auth_events VALUES (?,?,?,?,?,?,?,?,?,?)", event_rows)

    user_count  = con.execute("SELECT COUNT(*) FROM raw_users").fetchone()[0]
    event_count = con.execute("SELECT COUNT(*) FROM raw_auth_events").fetchone()[0]
    print(f"✅ Loaded {user_count:,} users and {event_count:,} auth events into {db_path}")
    con.close()


if __name__ == "__main__":
    print("🎵 Generating Spotify IAM synthetic data...")
    users  = make_users(NUM_USERS)
    events = make_auth_events(users, DAYS_OF_DATA, EVENTS_PER_DAY)
    load_to_duckdb(users, events, OUTPUT_DB)
    print("Done! Run `dbt run` next.")
