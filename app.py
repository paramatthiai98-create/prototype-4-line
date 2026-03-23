import random
from datetime import datetime

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="SmartSafe DENSO Production Dashboard",
    layout="wide"
)

# รีเฟรชทุก 2 วินาที
st_autorefresh(interval=2000, key="datarefresh")

# -------------------------
# CONFIG
# -------------------------
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

# -------------------------
# HELPERS
# -------------------------
def generate_data_by_line(line_key: str):
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


def calculate_risk_by_line(d: dict, line_key: str):
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

    return min(risk, 100), reasons


def decision_logic(risk: int):
    if risk > 80:
        return "HIGH RISK", "STOP MACHINE"
    if risk > 50:
        return "WARNING", "CHECK SYSTEM"
    return "SAFE", "NORMAL OPERATION"


def ai_solution_by_line(reasons: list[str], line_key: str):
    solutions = []

    if "No helmet detected" in reasons:
        solutions.append("ให้พนักงานสวมหมวกนิรภัยก่อนเข้าพื้นที่ปฏิบัติงาน")
        if line_key == "Line 1":
            solutions.append("เพิ่มจุดตรวจ PPE ก่อนเข้าพื้นที่ประกอบเซนเซอร์")
        elif line_key == "Line 4":
            solutions.append("เพิ่มมาตรการ PPE เข้มงวดในพื้นที่ EV / High Voltage")
        else:
            solutions.append("ติดตั้งระบบตรวจจับ PPE อัตโนมัติ")

    if "Worker too close to machine" in reasons:
        if line_key == "Line 3":
            solutions.append("เพิ่มระยะปลอดภัยจากเครื่องจักรความเร็วสูงในไลน์หัวฉีด")
        else:
            solutions.append("เพิ่มระยะปลอดภัยระหว่างคนงานกับเครื่องจักร")
        solutions.append("กำหนดเขต safe zone ให้ชัดเจน")

    if "High machine vibration" in reasons:
        if line_key == "Line 3":
            solutions.append("ตรวจสอบเครื่องจักร precision machining และ fixture ทันที")
        else:
            solutions.append("ตรวจสอบการสั่นสะเทือนของเครื่องจักรทันที")
        solutions.append("หยุดเครื่องเพื่อตรวจเช็กความผิดปกติ")
        solutions.append("วางแผนบำรุงรักษาเชิงป้องกัน")

    if "High operating temperature" in reasons:
        if line_key == "Line 2":
            solutions.append("ตรวจสอบระบบระบายความร้อนใน ECU/PCB line")
            solutions.append("ควบคุมอุณหภูมิพื้นที่ผลิตอิเล็กทรอนิกส์")
        elif line_key == "Line 4":
            solutions.append("ตรวจสอบอุณหภูมิในพื้นที่ EV Components ทันที")
            solutions.append("แยกพื้นที่ความร้อนสูงและเพิ่มระบบระบายอากาศ")
        else:
            solutions.append("ตรวจสอบอุณหภูมิการทำงานของอุปกรณ์")

    if not solutions:
        solutions.append("ระบบอยู่ในเกณฑ์ปกติ ให้ติดตามต่อเนื่อง")

    return solutions


def render_status(status: str):
    if status == "SAFE":
        st.success(status)
    elif status == "WARNING":
        st.warning(status)
    else:
        st.error(status)


def render_live_alert(line_key: str, status: str, reasons: list[str]):
    if status == "HIGH RISK":
        st.error(f"🚨 {line_key}: HIGH RISK - {', '.join(reasons)}")
    elif status == "WARNING":
        st.warning(f"⚠️ {line_key}: WARNING - {', '.join(reasons)}")
    else:
        st.success(f"✅ {line_key}: SAFE - No active critical risk")


# -------------------------
# SESSION STATE
# -------------------------
if "line_history" not in st.session_state:
    st.session_state.line_history = {line: [] for line in LINE_CONFIG.keys()}

if "line_alerts" not in st.session_state:
    st.session_state.line_alerts = {line: [] for line in LINE_CONFIG.keys()}

# -------------------------
# GENERATE CURRENT DATA
# -------------------------
current_line_data = {}

for line_key in LINE_CONFIG.keys():
    d = generate_data_by_line(line_key)
    risk, reasons = calculate_risk_by_line(d, line_key)
    status, action = decision_logic(risk)
    solutions = ai_solution_by_line(reasons, line_key)

    record = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "helmet": "YES" if d["helmet"] else "NO",
        "distance": d["distance"],
        "vibration": d["vibration"],
        "temperature": d["temperature"],
        "risk": risk,
        "status": status,
        "action": action,
        "reasons": ", ".join(reasons) if reasons else "No active risk detected",
        "solutions": " | ".join(solutions)
    }

    st.session_state.line_history[line_key].append(record)
    if len(st.session_state.line_history[line_key]) > 100:
        st.session_state.line_history[line_key] = st.session_state.line_history[line_key][-100:]

    if status in ["WARNING", "HIGH RISK"]:
        new_alert = {
            "time": record["time"],
            "risk": record["risk"],
            "status": record["status"],
            "reasons": record["reasons"],
            "action": record["action"]
        }

        alert_history = st.session_state.line_alerts[line_key]
        if (
            len(alert_history) == 0
            or alert_history[-1]["reasons"] != new_alert["reasons"]
            or alert_history[-1]["status"] != new_alert["status"]
        ):
            alert_history.append(new_alert)

    if len(st.session_state.line_alerts[line_key]) > 20:
        st.session_state.line_alerts[line_key] = st.session_state.line_alerts[line_key][-20:]

    current_line_data[line_key] = {
        "data": d,
        "risk": risk,
        "reasons": reasons,
        "status": status,
        "action": action,
        "solutions": solutions
    }

# -------------------------
# HEADER
# -------------------------
st.title("SmartSafe Co-Pilot Dashboard")
st.caption("DENSO-style production safety monitoring across 4 lines")

# -------------------------
# OVERVIEW
# -------------------------
st.subheader("Overview")

overview_cols = st.columns(4, gap="medium")

for i, line_key in enumerate(LINE_CONFIG.keys()):
    with overview_cols[i]:
        line_info = LINE_CONFIG[line_key]
        line_now = current_line_data[line_key]
        d = line_now["data"]

        with st.container(border=True):
            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown(f"**{line_key}**")
                st.write(line_info["name"])
            with c2:
                st.metric("Risk", line_now["risk"])

            st.caption(line_info["description"])

            f1, f2 = st.columns(2)
            f1.write(f"Helmet: {'YES' if d['helmet'] else 'NO'}")
            f2.write(f"Temp: {d['temperature']} °C")

            render_status(line_now["status"])

# -------------------------
# TABS
# -------------------------
tab_names = ["Overview All"] + list(LINE_CONFIG.keys())
tabs = st.tabs(tab_names)

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
        "Risk Score": [current_line_data[line]["risk"] for line in LINE_CONFIG.keys()]
    }).set_index("Line")
    st.line_chart(risk_compare, use_container_width=True)

for idx, line_key in enumerate(LINE_CONFIG.keys(), start=1):
    with tabs[idx]:
        line_info = LINE_CONFIG[line_key]
        line_now = current_line_data[line_key]
        d = line_now["data"]
        risk = line_now["risk"]
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
                render_status(status)
                st.progress(risk / 100)

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
            st.markdown("### Risk Trend")
            df = pd.DataFrame(st.session_state.line_history[line_key])
            st.line_chart(df[["risk"]], use_container_width=True)

        with st.container(border=True):
            st.markdown("### Recent Alerts")
            if st.session_state.line_alerts[line_key]:
                for alert in reversed(st.session_state.line_alerts[line_key][-5:]):
                    msg = (
                        f"[{alert['time']}] {alert['status']} | "
                        f"Score {alert['risk']} | {alert['reasons']} | "
                        f"Action: {alert['action']}"
                    )
                    if alert["status"] == "HIGH RISK":
                        st.error(msg)
                    else:
                        st.warning(msg)
            else:
                st.info("ยังไม่มีประวัติการแจ้งเตือน")
