import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="SmartSafe Co-Pilot Dashboard",
    layout="wide"
)

# =========================================================
# AUTO REFRESH
# =========================================================
REFRESH_MS = 2000
st_autorefresh(interval=REFRESH_MS, key="datarefresh")

# =========================================================
# CONFIG
# =========================================================
LINE_CONFIG = {
    "Line 1": {
        "name": "Sensor Assembly",
        "description": "Temperature / Pressure / Oxygen Sensor Assembly Line"
    },
    "Line 2": {
        "name": "ECU Production",
        "description": "Electronic Control Unit and PCB Assembly Line"
    },
    "Line 3": {
        "name": "Fuel Injector",
        "description": "Fuel System / Injector Precision Manufacturing Line"
    },
    "Line 4": {
        "name": "EV Components",
        "description": "Battery / Motor / Power Electronics Assembly Line"
    }
}

MAX_SESSION_HISTORY = 100
MAX_SESSION_ALERTS = 20
DB_PATH = Path("smartsafe_history.db")

RANGE_OPTIONS = {
    "7 Days": 7,
    "30 Days": 30,
    "4 Months": 120,
    "8 Months": 240,
    "1 Year": 365
}

# =========================================================
# DATABASE
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        created_date TEXT NOT NULL,
        created_hour TEXT NOT NULL,
        line_key TEXT NOT NULL,
        process_name TEXT NOT NULL,
        helmet TEXT NOT NULL,
        distance REAL NOT NULL,
        vibration REAL NOT NULL,
        temperature REAL NOT NULL,
        risk INTEGER NOT NULL,
        status TEXT NOT NULL,
        action TEXT NOT NULL,
        reasons TEXT NOT NULL,
        solutions TEXT NOT NULL,
        is_demo INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_history_line_time
    ON history(line_key, created_at)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_history_time
    ON history(created_at)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        line_key TEXT NOT NULL,
        risk INTEGER NOT NULL,
        status TEXT NOT NULL,
        reasons TEXT NOT NULL,
        action TEXT NOT NULL,
        is_demo INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_alert_line_time
    ON alert_log(line_key, created_at)
    """)

    conn.commit()
    conn.close()


init_db()

# =========================================================
# HELPERS
# =========================================================
def clamp_risk(value):
    try:
        value = int(round(float(value)))
    except Exception:
        return 0
    return max(0, min(value, 100))


def progress_value_from_risk(risk):
    return clamp_risk(risk) / 100.0


def safe_append_limited(items, value, max_len):
    items.append(value)
    if len(items) > max_len:
        del items[:-max_len]


def decision_logic(risk):
    risk = clamp_risk(risk)
    if risk > 80:
        return "HIGH RISK", "STOP MACHINE"
    if risk > 50:
        return "WARNING", "CHECK SYSTEM"
    return "SAFE", "NORMAL OPERATION"


def render_status_box(status):
    if status == "SAFE":
        st.success(status)
    elif status == "WARNING":
        st.warning(status)
    else:
        st.error(status)


def render_live_alert(line_key, status, reasons):
    if status == "HIGH RISK":
        st.error(f"🚨 {line_key}: HIGH RISK - {', '.join(reasons)}")
    elif status == "WARNING":
        st.warning(f"⚠️ {line_key}: WARNING - {', '.join(reasons)}")
    else:
        st.success(f"✅ {line_key}: SAFE - No active critical risk")


def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return datetime.now().strftime("%Y-%m-%d")


def current_hour_str():
    return datetime.now().strftime("%H:00")


def cutoff_datetime(days):
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def safe_percent(numerator, denominator):
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def format_percent(value):
    return f"{value:.1f}%"


# =========================================================
# INCIDENT ANALYTICS
# =========================================================
def add_transition_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.sort_values(["line_key", "created_at"]).copy()
    out["prev_status"] = out.groupby("line_key")["status"].shift(1)
    out["is_warning_incident"] = (
        (out["status"] == "WARNING") &
        (out["prev_status"] != "WARNING")
    )
    out["is_high_incident"] = (
        (out["status"] == "HIGH RISK") &
        (out["prev_status"] != "HIGH RISK")
    )
    out["is_any_incident"] = out["is_warning_incident"] | out["is_high_incident"]
    return out


def count_status_incidents(df: pd.DataFrame, target_status: str) -> int:
    if df.empty:
        return 0

    working = add_transition_columns(df)
    if target_status == "WARNING":
        return int(working["is_warning_incident"].sum())
    if target_status == "HIGH RISK":
        return int(working["is_high_incident"].sum())
    return 0


def build_incident_summary_by_line(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    working = add_transition_columns(df)
    rows = []

    for line_key in working["line_key"].unique():
        line_df = working[working["line_key"] == line_key].copy()

        total_records = len(line_df)
        warning_records = int((line_df["status"] == "WARNING").sum())
        high_records = int((line_df["status"] == "HIGH RISK").sum())
        warning_incidents = int(line_df["is_warning_incident"].sum())
        high_incidents = int(line_df["is_high_incident"].sum())

        rows.append({
            "Line": line_key,
            "Avg Risk": round(float(line_df["risk"].mean()), 1),
            "Warning Incidents": warning_incidents,
            "High Risk Incidents": high_incidents,
            "Warning Records": warning_records,
            "High Risk Records": high_records,
            "Warning Rate (%)": round(safe_percent(warning_records, total_records), 1),
            "High Risk Rate (%)": round(safe_percent(high_records, total_records), 1),
            "Total Records": total_records
        })

    return pd.DataFrame(rows).sort_values(
        ["Avg Risk", "High Risk Incidents", "Warning Incidents"],
        ascending=[False, False, False]
    )


def build_incident_timeline(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    working = add_transition_columns(df)
    incident_df = working[working["is_any_incident"]].copy()

    if incident_df.empty:
        return pd.DataFrame()

    incident_df["created_date_only"] = incident_df["created_at"].dt.date
    summary = (
        incident_df.groupby(["created_date_only", "line_key"], as_index=False)
        .agg(
            warning_incidents=("is_warning_incident", "sum"),
            high_risk_incidents=("is_high_incident", "sum")
        )
    )
    return summary


def get_peak_hour(df: pd.DataFrame) -> str:
    if df.empty:
        return "-"

    working = df.copy()
    working["hour"] = working["created_at"].dt.hour
    working["is_alert_record"] = working["status"].isin(["WARNING", "HIGH RISK"]).astype(int)

    hourly = working.groupby("hour", as_index=False)["is_alert_record"].sum()
    if hourly.empty or int(hourly["is_alert_record"].max()) == 0:
        return "-"

    peak_row = hourly.sort_values(["is_alert_record", "hour"], ascending=[False, True]).iloc[0]
    return f"{int(peak_row['hour']):02d}:00"


def get_most_risky_line(df: pd.DataFrame) -> str:
    if df.empty:
        return "-"

    summary = (
        df.groupby("line_key", as_index=False)
        .agg(avg_risk=("risk", "mean"))
        .sort_values("avg_risk", ascending=False)
    )
    if summary.empty:
        return "-"
    return str(summary.iloc[0]["line_key"])


# =========================================================
# DATA GENERATION
# =========================================================
def generate_random_data_by_line(line_key):
    if line_key == "Line 1":
        return {
            "helmet": random.choice([True, False]),
            "distance": random.randint(15, 80),
            "vibration": random.randint(10, 60),
            "temperature": random.randint(25, 45)
        }
    if line_key == "Line 2":
        return {
            "helmet": random.choice([True, False]),
            "distance": random.randint(20, 90),
            "vibration": random.randint(5, 40),
            "temperature": random.randint(30, 65)
        }
    if line_key == "Line 3":
        return {
            "helmet": random.choice([True, False]),
            "distance": random.randint(10, 70),
            "vibration": random.randint(25, 90),
            "temperature": random.randint(28, 55)
        }
    return {
        "helmet": random.choice([True, False]),
        "distance": random.randint(15, 85),
        "vibration": random.randint(10, 75),
        "temperature": random.randint(35, 80)
    }


def calculate_risk_by_line(d, line_key):
    risk = 0
    reasons = []

    if not d["helmet"]:
        risk += 40 if line_key == "Line 4" else 30
        reasons.append("No helmet detected")

    if d["distance"] < 30:
        risk += 45 if line_key == "Line 3" else 40
        reasons.append("Worker too close to machine")

    if d["vibration"] > 70:
        risk += 45 if line_key == "Line 3" else 35
        reasons.append("High machine vibration")

    if d["temperature"] > 60:
        if line_key in ["Line 2", "Line 4"]:
            risk += 35
            reasons.append("High operating temperature")
        elif d["temperature"] > 70:
            risk += 20
            reasons.append("High operating temperature")

    return clamp_risk(risk), reasons


def ai_solution_by_line(reasons, line_key):
    solutions = []

    if "No helmet detected" in reasons:
        solutions.append("Ensure the worker wears a helmet before entering the work area.")
        if line_key == "Line 1":
            solutions.append("Add a PPE checkpoint before the sensor assembly area.")
        elif line_key == "Line 4":
            solutions.append("Apply stricter PPE control in the EV / High Voltage area.")
        else:
            solutions.append("Install an automatic PPE detection system.")

    if "Worker too close to machine" in reasons:
        if line_key == "Line 3":
            solutions.append("Increase the safe distance from the high-speed injector machine.")
        else:
            solutions.append("Increase the safe distance between worker and machine.")
        solutions.append("Clearly define and mark the safe zone.")

    if "High machine vibration" in reasons:
        if line_key == "Line 3":
            solutions.append("Inspect the precision machining unit and fixture immediately.")
        else:
            solutions.append("Inspect abnormal machine vibration immediately.")
        solutions.append("Stop the machine for inspection.")
        solutions.append("Schedule preventive maintenance.")

    if "High operating temperature" in reasons:
        if line_key == "Line 2":
            solutions.append("Inspect the cooling system in the ECU/PCB line.")
            solutions.append("Control ambient temperature in the electronics production area.")
        elif line_key == "Line 4":
            solutions.append("Inspect high temperature in the EV Components area immediately.")
            solutions.append("Separate high-heat zones and improve ventilation.")
        else:
            solutions.append("Inspect equipment operating temperature.")

    if not solutions:
        solutions.append("System is within normal condition. Continue monitoring.")

    return solutions


def ai_pattern_recommendation(df_line: pd.DataFrame, line_key: str):
    if df_line.empty:
        return "No sufficient historical data."

    msg = []
    high_count = int((df_line["status"] == "HIGH RISK").sum())
    warning_count = int((df_line["status"] == "WARNING").sum())
    avg_risk = float(df_line["risk"].mean())

    if high_count >= 10:
        msg.append("Repeated HIGH RISK detected. Perform root cause analysis and assign ownership.")
    if warning_count >= 20:
        msg.append("High WARNING accumulation detected. Start preventive review by shift.")
    if avg_risk >= 60:
        msg.append("Average risk is relatively high. Increase inspection frequency.")

    helmet_no_rate = (df_line["helmet"] == "NO").mean() if len(df_line) else 0
    if helmet_no_rate >= 0.25:
        msg.append("High non-PPE rate detected. Add a PPE checkpoint before entry.")

    if (df_line["vibration"] > 70).mean() >= 0.20:
        msg.append("Repeated high vibration detected. Schedule preventive maintenance.")

    if (df_line["temperature"] > 60).mean() >= 0.20:
        msg.append("Repeated high temperature detected. Check cooling and ventilation systems.")

    if not msg:
        return f"{line_key}: Historical trend remains controllable. Continue monitoring."

    return f"{line_key}: " + " | ".join(msg[:3])


# =========================================================
# DB WRITE / READ
# =========================================================
def insert_history_record(record: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO history (
            created_at, created_date, created_hour, line_key, process_name,
            helmet, distance, vibration, temperature, risk, status, action,
            reasons, solutions, is_demo
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        record["created_at"],
        record["created_date"],
        record["created_hour"],
        record["line_key"],
        record["process_name"],
        record["helmet"],
        record["distance"],
        record["vibration"],
        record["temperature"],
        record["risk"],
        record["status"],
        record["action"],
        record["reasons"],
        record["solutions"],
        record["is_demo"]
    ))
    conn.commit()
    conn.close()


def insert_alert_record(record: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO alert_log (
            created_at, line_key, risk, status, reasons, action, is_demo
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        record["created_at"],
        record["line_key"],
        record["risk"],
        record["status"],
        record["reasons"],
        record["action"],
        record["is_demo"]
    ))
    conn.commit()
    conn.close()


def read_history(days: int, line_key: str | None = None):
    conn = get_conn()
    cutoff = cutoff_datetime(days)
    if line_key and line_key != "All Lines":
        query = """
            SELECT * FROM history
            WHERE created_at >= ? AND line_key = ?
            ORDER BY created_at ASC
        """
        df = pd.read_sql_query(query, conn, params=(cutoff, line_key))
    else:
        query = """
            SELECT * FROM history
            WHERE created_at >= ?
            ORDER BY created_at ASC
        """
        df = pd.read_sql_query(query, conn, params=(cutoff,))
    conn.close()
    return df


def read_recent_alerts(line_key: str, limit: int = 5):
    conn = get_conn()
    query = """
        SELECT created_at, risk, status, reasons, action
        FROM alert_log
        WHERE line_key = ?
        ORDER BY created_at DESC
        LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(line_key, limit))
    conn.close()
    return df


def read_total_counts():
    conn = get_conn()
    cur = conn.cursor()
    total_history = cur.execute("SELECT COUNT(*) FROM history").fetchone()[0]
    total_alerts = cur.execute("SELECT COUNT(*) FROM alert_log").fetchone()[0]
    conn.close()
    return total_history, total_alerts


def delete_older_than(days: int):
    cutoff = cutoff_datetime(days)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM history WHERE created_at < ?", (cutoff,))
    cur.execute("DELETE FROM alert_log WHERE created_at < ?", (cutoff,))
    conn.commit()
    deleted = conn.total_changes
    conn.close()
    return deleted


# =========================================================
# SESSION STATE
# =========================================================
if "line_history" not in st.session_state:
    st.session_state.line_history = {line: [] for line in LINE_CONFIG.keys()}

if "line_alerts" not in st.session_state:
    st.session_state.line_alerts = {line: [] for line in LINE_CONFIG.keys()}

# =========================================================
# SIDEBAR
# =========================================================
st.sidebar.title("Control Panel")

history_range_label = st.sidebar.selectbox(
    "Historical Range",
    list(RANGE_OPTIONS.keys()),
    index=0
)
history_days = RANGE_OPTIONS[history_range_label]

selected_history_line = st.sidebar.selectbox(
    "Historical Line Filter",
    ["All Lines"] + list(LINE_CONFIG.keys()),
    index=0
)

st.sidebar.markdown("---")
retention_days = st.sidebar.selectbox(
    "Data Retention",
    [30, 90, 180, 365, 730],
    index=3
)

if st.sidebar.button("Clean Old Data"):
    deleted_rows = delete_older_than(retention_days)
    st.sidebar.success(f"Deleted {deleted_rows} old rows")

# =========================================================
# GENERATE CURRENT DATA + SAVE TO DB
# =========================================================
current_line_data = {}

for line_key in LINE_CONFIG.keys():
    d = generate_random_data_by_line(line_key)
    risk, reasons = calculate_risk_by_line(d, line_key)
    solutions = ai_solution_by_line(reasons, line_key)

    risk = clamp_risk(risk)
    status, action = decision_logic(risk)
    ts = now_iso()

    record = {
        "created_at": ts,
        "created_date": today_str(),
        "created_hour": current_hour_str(),
        "line_key": line_key,
        "process_name": LINE_CONFIG[line_key]["name"],
        "helmet": "YES" if d["helmet"] else "NO",
        "distance": d["distance"],
        "vibration": d["vibration"],
        "temperature": d["temperature"],
        "risk": risk,
        "status": status,
        "action": action,
        "reasons": ", ".join(reasons) if reasons else "No active risk detected",
        "solutions": " | ".join(solutions),
        "is_demo": 0
    }

    insert_history_record(record)

    session_record = {
        "time": ts,
        "helmet": record["helmet"],
        "distance": record["distance"],
        "vibration": record["vibration"],
        "temperature": record["temperature"],
        "risk": record["risk"],
        "status": record["status"],
        "action": record["action"],
        "reasons": record["reasons"],
        "solutions": record["solutions"]
    }
    safe_append_limited(st.session_state.line_history[line_key], session_record, MAX_SESSION_HISTORY)

    if status in ["WARNING", "HIGH RISK"]:
        new_alert = {
            "created_at": ts,
            "line_key": line_key,
            "risk": risk,
            "status": status,
            "reasons": record["reasons"],
            "action": action,
            "is_demo": 0
        }

        alert_history = st.session_state.line_alerts[line_key]
        should_append = (
            len(alert_history) == 0
            or alert_history[-1]["reasons"] != new_alert["reasons"]
            or alert_history[-1]["status"] != new_alert["status"]
        )
        if should_append:
            safe_append_limited(alert_history, new_alert, MAX_SESSION_ALERTS)
            insert_alert_record(new_alert)

    current_line_data[line_key] = {
        "data": d,
        "risk": risk,
        "reasons": reasons,
        "status": status,
        "action": action,
        "solutions": solutions
    }

# =========================================================
# HISTORICAL LOAD
# =========================================================
hist_df = read_history(history_days, selected_history_line)
historical_source_label = "SQLite Historical Data"

if not hist_df.empty:
    hist_df["created_at"] = pd.to_datetime(hist_df["created_at"])
    hist_df["risk"] = hist_df["risk"].apply(clamp_risk)

hist_transition_df = add_transition_columns(hist_df) if not hist_df.empty else pd.DataFrame()

# =========================================================
# HEADER
# =========================================================
st.title("SmartSafe Co-Pilot Dashboard")
st.caption("DENSO-style production safety monitoring across 4 lines with SQLite historical storage")

total_history, total_alerts = read_total_counts()
h1, h2, h3, h4 = st.columns(4)
h1.metric("Total History Rows", f"{total_history:,}")
h2.metric("Total Alert Rows", f"{total_alerts:,}")
h3.metric("Selected Range", history_range_label)
h4.metric("Historical Source", historical_source_label)

# =========================================================
# OVERVIEW
# =========================================================
st.subheader("Overview")
overview_cols = st.columns(4, gap="medium")

for i, line_key in enumerate(LINE_CONFIG.keys()):
    with overview_cols[i]:
        line_info = LINE_CONFIG[line_key]
        line_now = current_line_data[line_key]
        d = line_now["data"]

        with st.container(border=True):
            left, right = st.columns([2, 1])
            with left:
                st.markdown(f"**{line_key}**")
                st.write(line_info["name"])
            with right:
                st.metric("Risk", line_now["risk"])

            st.caption(line_info["description"])

            c1, c2 = st.columns(2)
            c1.write(f"Helmet: {'YES' if d['helmet'] else 'NO'}")
            c2.write(f"Temp: {d['temperature']} °C")

            render_status_box(line_now["status"])

# =========================================================
# TABS
# =========================================================
tab_names = ["Overview All"] + list(LINE_CONFIG.keys()) + ["Historical Analytics"]
tabs = st.tabs(tab_names)

# =========================================================
# TAB 0 - OVERVIEW ALL
# =========================================================
with tabs[0]:
    st.subheader("All Production Lines Summary")

    summary_rows = []
    for line_key, line_info in LINE_CONFIG.items():
        line_now = current_line_data[line_key]
        d = line_now["data"]
        summary_rows.append({
            "Line": line_key,
            "Process": line_info["name"],
            "Helmet": "YES" if d["helmet"] else "NO",
            "Distance (cm)": d["distance"],
            "Vibration": d["vibration"],
            "Temperature (°C)": d["temperature"],
            "Risk": line_now["risk"],
            "Status": line_now["status"],
            "Action": line_now["action"]
        })

    summary_df = pd.DataFrame(summary_rows)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.subheader("Risk Comparison")
    risk_compare = pd.DataFrame({
        "Line": list(LINE_CONFIG.keys()),
        "Risk Score": [clamp_risk(current_line_data[line]["risk"]) for line in LINE_CONFIG.keys()]
    }).set_index("Line")
    st.line_chart(risk_compare, use_container_width=True)

# =========================================================
# LINE TABS
# =========================================================
for idx, line_key in enumerate(LINE_CONFIG.keys(), start=1):
    with tabs[idx]:
        line_info = LINE_CONFIG[line_key]
        line_now = current_line_data[line_key]
        d = line_now["data"]
        risk = clamp_risk(line_now["risk"])
        reasons = line_now["reasons"]
        status = line_now["status"]
        action = line_now["action"]
        solutions = line_now["solutions"]

        st.subheader(f"{line_key} - {line_info['name']}")
        st.caption(line_info["description"])

        col1, col2, col3 = st.columns(3, gap="large")

        with col1:
            with st.container(border=True):
                st.markdown("### Worker Status")
                st.metric("Helmet", "YES" if d["helmet"] else "NO")
                st.metric("Distance", f"{d['distance']} cm")

        with col2:
            with st.container(border=True):
                st.markdown("### Machine Status")
                st.metric("Vibration", d["vibration"])
                st.metric("Temperature", f"{d['temperature']} °C")

        with col3:
            with st.container(border=True):
                st.markdown("### Risk Analysis")
                st.metric("Risk Score", risk)
                render_status_box(status)
                st.progress(progress_value_from_risk(risk))

        st.subheader("Live Alert")
        render_live_alert(line_key, status, reasons)

        left, right = st.columns([1, 1.15], gap="large")

        with left:
            with st.container(border=True):
                st.markdown("### AI Decision Support")
                st.write(f"Recommended Action: **{action}**")

                st.markdown("### Explainable AI")
                if reasons:
                    for r in reasons:
                        st.write(f"- {r}")
                else:
                    st.write("- No active risk detected")

        with right:
            with st.container(border=True):
                st.markdown("### AI Recommended Fix")
                for s in solutions:
                    st.info(s)

        with st.container(border=True):
            st.markdown("### Session Risk Trend")
            df = pd.DataFrame(st.session_state.line_history[line_key])

            if "risk" in df.columns and not df.empty:
                df["risk"] = df["risk"].apply(clamp_risk)
                st.line_chart(df[["risk"]], use_container_width=True)
            else:
                st.info("No session chart data yet.")

        with st.container(border=True):
            st.markdown(f"### Historical Trend ({history_range_label})")
            line_hist_df = hist_df[hist_df["line_key"] == line_key].copy() if not hist_df.empty else pd.DataFrame()

            if not line_hist_df.empty:
                trend_line = (
                    line_hist_df
                    .assign(created_date_only=line_hist_df["created_at"].dt.date)
                    .groupby("created_date_only", as_index=False)
                    .agg(
                        avg_risk=("risk", "mean"),
                        max_risk=("risk", "max")
                    )
                    .set_index("created_date_only")
                )
                st.line_chart(trend_line[["avg_risk", "max_risk"]], use_container_width=True)
            else:
                st.info("No historical chart data in the selected range.")

        with st.container(border=True):
            st.markdown("### Recent Alerts")
            recent_alerts_df = read_recent_alerts(line_key, limit=5)
            if not recent_alerts_df.empty:
                for _, alert in recent_alerts_df.iterrows():
                    safe_risk = clamp_risk(alert["risk"])
                    msg = (
                        f"[{alert['created_at']}] {alert['status']} | "
                        f"Score {safe_risk} | {alert['reasons']} | "
                        f"Action: {alert['action']}"
                    )
                    if alert["status"] == "HIGH RISK":
                        st.error(msg)
                    else:
                        st.warning(msg)
            else:
                st.info("No alert history yet.")

# =========================================================
# HISTORICAL ANALYTICS TAB
# =========================================================
with tabs[-1]:
    st.subheader("Historical Analytics")
    st.caption(
        f"Selected Range: {history_range_label} | "
        f"Line: {selected_history_line} | "
        f"Source: {historical_source_label}"
    )

    if hist_df.empty:
        st.warning("No historical data found in the selected range.")
    else:
        total_records = len(hist_df)
        avg_risk = round(float(hist_df["risk"].mean()), 1)
        warning_records = int((hist_df["status"] == "WARNING").sum())
        high_risk_records = int((hist_df["status"] == "HIGH RISK").sum())
        warning_incidents = count_status_incidents(hist_df, "WARNING")
        high_risk_incidents = count_status_incidents(hist_df, "HIGH RISK")
        warning_rate = safe_percent(warning_records, total_records)
        high_risk_rate = safe_percent(high_risk_records, total_records)
        most_risky_line = get_most_risky_line(hist_df)
        peak_hour = get_peak_hour(hist_df)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Records", f"{total_records:,}")
        m2.metric("Avg Risk", f"{avg_risk:.1f}")
        m3.metric("Warning Incidents", f"{warning_incidents:,}")
        m4.metric("High Risk Incidents", f"{high_risk_incidents:,}")

        c1, c2, c3, c4 = st.columns(4)
        c1.caption(f"Raw rows collected in {history_range_label.lower()}")
        c2.caption("Average risk score across all selected records")
        c3.caption(f"{warning_records:,} warning records found")
        c4.caption(f"{high_risk_records:,} high-risk records found")

        st.markdown("")

        i1, i2, i3, i4 = st.columns(4)
        i1.metric("Warning Rate", format_percent(warning_rate))
        i2.metric("High Risk Rate", format_percent(high_risk_rate))
        i3.metric("Most Risky Line", most_risky_line)
        i4.metric("Peak Hour", peak_hour)

        d1, d2, d3, d4 = st.columns(4)
        d1.caption("Percentage of records in WARNING state")
        d2.caption("Percentage of records in HIGH RISK state")
        d3.caption("Line with highest average risk")
        d4.caption("Hour with the highest alert concentration")

        st.markdown("---")

        st.markdown("### Historical Trend")
        trend_df = (
            hist_df
            .assign(created_date_only=hist_df["created_at"].dt.date)
            .groupby(["created_date_only", "line_key"], as_index=False)
            .agg(
                avg_risk=("risk", "mean"),
                max_risk=("risk", "max"),
                warning_records=("status", lambda s: int((s == "WARNING").sum())),
                high_risk_records=("status", lambda s: int((s == "HIGH RISK").sum()))
            )
        )

        if selected_history_line == "All Lines":
            pivot_avg = trend_df.pivot(index="created_date_only", columns="line_key", values="avg_risk")
            st.line_chart(pivot_avg, use_container_width=True)
        else:
            one_line = trend_df[trend_df["line_key"] == selected_history_line].set_index("created_date_only")
            st.line_chart(one_line[["avg_risk", "max_risk"]], use_container_width=True)

        a1, a2 = st.columns(2)

        with a1:
            st.markdown("### Risk by Line")
            risk_line_df = (
                hist_df.groupby("line_key", as_index=False)
                .agg(
                    avg_risk=("risk", "mean"),
                    max_risk=("risk", "max"),
                    records=("risk", "count")
                )
                .sort_values("avg_risk", ascending=False)
                .set_index("line_key")
            )
            st.bar_chart(risk_line_df[["avg_risk", "max_risk"]], use_container_width=True)

        with a2:
            st.markdown("### Alert Distribution (Records)")
            alert_dist_df = (
                hist_df[hist_df["status"].isin(["WARNING", "HIGH RISK"])]
                .groupby(["line_key", "status"], as_index=False)
                .size()
            )
            if not alert_dist_df.empty:
                alert_pivot = alert_dist_df.pivot(index="line_key", columns="status", values="size").fillna(0)
                st.bar_chart(alert_pivot, use_container_width=True)
            else:
                st.info("No WARNING/HIGH RISK records in the selected range.")

        b1, b2 = st.columns(2)

        with b1:
            st.markdown("### Incident Timeline")
            incident_timeline_df = build_incident_timeline(hist_df)

            if not incident_timeline_df.empty:
                if selected_history_line == "All Lines":
                    timeline_view = (
                        incident_timeline_df.groupby("created_date_only", as_index=False)
                        .agg(
                            warning_incidents=("warning_incidents", "sum"),
                            high_risk_incidents=("high_risk_incidents", "sum")
                        )
                        .set_index("created_date_only")
                    )
                else:
                    timeline_view = (
                        incident_timeline_df[incident_timeline_df["line_key"] == selected_history_line]
                        .groupby("created_date_only", as_index=False)
                        .agg(
                            warning_incidents=("warning_incidents", "sum"),
                            high_risk_incidents=("high_risk_incidents", "sum")
                        )
                        .set_index("created_date_only")
                    )

                st.line_chart(timeline_view, use_container_width=True)
            else:
                st.info("No incident transitions found in the selected range.")

        with b2:
            st.markdown("### Incident Summary by Line")
            incident_summary_df = build_incident_summary_by_line(hist_df)
            if not incident_summary_df.empty:
                st.dataframe(incident_summary_df, use_container_width=True, hide_index=True)
            else:
                st.info("No incident summary available.")

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("### Top Reasons")
            reason_series = (
                hist_df["reasons"]
                .str.split(", ")
                .explode()
                .dropna()
            )
            reason_df = reason_series.value_counts().reset_index()
            reason_df.columns = ["Reason", "Count"]
            st.dataframe(reason_df.head(10), use_container_width=True, hide_index=True)

        with c2:
            st.markdown("### Hourly Hotspot")
            hourly_df = hist_df.copy()
            hourly_df["hour"] = hourly_df["created_at"].dt.hour
            hourly_summary = (
                hourly_df.groupby("hour", as_index=False)
                .agg(
                    avg_risk=("risk", "mean"),
                    warning_records=("status", lambda s: int((s == "WARNING").sum())),
                    high_risk_records=("status", lambda s: int((s == "HIGH RISK").sum()))
                )
                .set_index("hour")
            )
            st.line_chart(hourly_summary, use_container_width=True)

        st.markdown("### Safety Score by Line")
        safety_df = hist_df.groupby("line_key", as_index=False).agg(
            avg_risk=("risk", "mean"),
            helmet_yes_rate=("helmet", lambda s: round((s == "YES").mean() * 100, 2)),
            high_risk_count=("status", lambda s: int((s == "HIGH RISK").sum())),
            warning_count=("status", lambda s: int((s == "WARNING").sum()))
        )
        safety_df["safety_score"] = (
            100
            - safety_df["avg_risk"]
            - (safety_df["high_risk_count"] * 0.5)
            - (safety_df["warning_count"] * 0.2)
            + (safety_df["helmet_yes_rate"] * 0.1)
        ).clip(lower=0, upper=100).round(1)

        st.dataframe(
            safety_df.sort_values("safety_score", ascending=False),
            use_container_width=True,
            hide_index=True
        )

        st.markdown("### Pattern-based AI Insight")
        insight_lines = []
        for lk in hist_df["line_key"].unique():
            df_line = hist_df[hist_df["line_key"] == lk].copy()
            insight_lines.append(ai_pattern_recommendation(df_line, lk))
        for msg in insight_lines:
            st.info(msg)

        st.markdown("### Historical Event Table")

        table_view_mode = st.radio(
            "Table View",
            ["Warning + High Risk Only", "All Records"],
            horizontal=True
        )

        if table_view_mode == "Warning + High Risk Only":
            display_df = hist_df[hist_df["status"].isin(["WARNING", "HIGH RISK"])].copy()
        else:
            display_df = hist_df.copy()

        if not display_df.empty:
            display_df["created_at"] = display_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

            display_columns = [
                "created_at",
                "line_key",
                "process_name",
                "risk",
                "status",
                "action",
                "reasons",
                "helmet",
                "distance",
                "vibration",
                "temperature"
            ]

            display_df = display_df[display_columns].sort_values(
                by=["created_at", "risk"],
                ascending=[False, False]
            )

            st.dataframe(display_df, use_container_width=True, hide_index=True)

            csv_data = to_csv_bytes(display_df)
            st.download_button(
                "Download Filtered CSV",
                data=csv_data,
                file_name=f"smartsafe_events_{selected_history_line.replace(' ', '_').lower()}_{history_days}d.csv",
                mime="text/csv"
            )
        else:
            st.info("No records found for the selected filter.")
