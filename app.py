# app.py
import streamlit as st
from PIL import Image
import numpy as np
import pandas as pd
import sqlite3
import io
import hashlib
import uuid
import time
from datetime import datetime

DB_PATH = "registry.db"

# ---------- DB helpers ----------
def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_uuid TEXT,
        created_at TEXT,
        project_name TEXT,
        data_type TEXT,
        credits INTEGER,
        mrv_status TEXT,
        mrv_msg TEXT,
        tx_hash TEXT
    )
    """)
    conn.commit()
    return conn

def save_record(conn, project_name, data_type, credits, mrv_status, mrv_msg, tx_hash=None):
    record_uuid = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat()
    c = conn.cursor()
    c.execute("""
        INSERT INTO registry (record_uuid, created_at, project_name, data_type, credits, mrv_status, mrv_msg, tx_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (record_uuid, created_at, project_name, data_type, credits, mrv_status, mrv_msg, tx_hash))
    conn.commit()
    return record_uuid

def fetch_registry(conn):
    return pd.read_sql_query("SELECT * FROM registry ORDER BY id DESC", conn)

# ---------- MRV checks ----------
def anomaly_detection_image(img: Image.Image):
    # Basic: grayscale variance check
    arr = np.array(img.convert("L"), dtype=np.float32)
    variance = float(arr.var())
    # simplistic rule: very low variance -> suspicious
    if variance < 50:
        return False, f"Low pixel variance ({variance:.1f}) — could be manipulated/too uniform."
    # check extremely small images
    w, h = img.size
    if w*h < 10000:
        return False, f"Small image ({w}x{h}) — may be insufficient resolution."
    return True, f"Image passes basic checks (variance={variance:.1f})."

def anomaly_detection_iot(df: pd.DataFrame):
    # expect a column named 'value'
    if 'value' not in df.columns:
        return False, "CSV missing required 'value' column."
    vals = df['value'].dropna().astype(float)
    if len(vals) < 3:
        return False, "Not enough IoT readings."
    mean = vals.mean()
    std = vals.std()
    # z-score check
    zscores = (vals - mean) / (std if std>0 else 1)
    anomalies = (abs(zscores) > 3).sum()
    if anomalies > max(1, len(vals)//10):
        return False, f"{anomalies} statistical anomalies detected in sensor readings."
    # repeated values or flatline
    if vals.nunique() <= 2:
        return False, "Suspiciously flat sensor data (too few unique values)."
    return True, f"IoT readings look OK (mean={mean:.2f}, std={std:.2f}, anomalies={anomalies})."

# ---------- Credit calculations ----------
def calculate_credits_image(img: Image.Image):
    # demo formula: area-based proxy
    w, h = img.size
    base = (w * h) / 200000.0   # choose divisor to keep values small
    credits = max(1, int(base * 10))  # deterministic integer
    return credits

def calculate_credits_iot(df: pd.DataFrame):
    # demo: average value * duration factor
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            duration_hours = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()/3600.0
            duration_hours = max(duration_hours, 1.0)
        except:
            duration_hours = 1.0
    else:
        duration_hours = 1.0
    avg_val = float(df['value'].mean())
    credits = max(1, int((avg_val * duration_hours) / 10.0))
    return credits

# ---------- Simulated blockchain tx hash ----------
def simulate_tx_hash(record_uuid):
    payload = f"{record_uuid}|{time.time()}"
    return hashlib.sha256(payload.encode()).hexdigest()

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Blue Carbon MRV MVP", layout="wide")
st.title("🌱 Blue Carbon MRV + Registry — MVP (Streamlit)")

conn = init_db()

with st.sidebar:
    st.header("Upload & Info")
    project_name = st.text_input("Project name", value="Demo Mangrove Project")
    uploader = st.text_input("Uploader name (optional)")

col1, col2 = st.columns([1,2])
with col1:
    st.subheader("Upload proof")
    uploaded_file = st.file_uploader("Upload image (jpg/png) or IoT CSV", type=['png','jpg','jpeg','csv'])
    run_btn = st.button("Verify & Calculate")

with col2:
    st.subheader("Registry")
    df_registry = fetch_registry(conn)
    st.dataframe(df_registry)

    csv = df_registry.to_csv(index=False).encode('utf-8')
    st.download_button(label="Download registry CSV", data=csv, file_name="registry.csv", mime='text/csv')

# Process upload
if run_btn:
    if not uploaded_file:
        st.warning("Please upload a file first.")
    else:
        filename = uploaded_file.name.lower()
        if filename.endswith('.csv'):
            # IoT CSV path
            st.info("Processing IoT CSV...")
            try:
                df = pd.read_csv(uploaded_file)
            except Exception as e:
                st.error(f"Failed to parse CSV: {e}")
                st.stop()
            ok, msg = anomaly_detection_iot(df)
            st.write(msg)
            if ok:
                credits = calculate_credits_iot(df)
                tx = None
                st.success(f"Verified ✔ — estimated credits: {credits} tCO₂e")
                # save record (no tx yet)
                rec_uuid = save_record(conn, project_name, "iot_csv", credits, "verified", msg, tx)
                st.write("Saved record id:", rec_uuid)
            else:
                credits = 0
                rec_uuid = save_record(conn, project_name, "iot_csv", credits, "rejected", msg, None)
                st.error("Data rejected — record saved with status 'rejected'.")
        else:
            # image path
            try:
                img = Image.open(uploaded_file).convert("RGB")
            except Exception as e:
                st.error(f"Could not open image: {e}")
                st.stop()
            st.image(img, caption="Uploaded image", use_column_width=True)
            ok, msg = anomaly_detection_image(img)
            st.write(msg)
            if ok:
                credits = calculate_credits_image(img)
                rec_uuid = save_record(conn, project_name, "image", credits, "verified", msg, None)
                st.success(f"Verified ✔ — estimated credits: {credits} tCO₂e")
                st.write("Saved record id:", rec_uuid)
            else:
                credits = 0
                rec_uuid = save_record(conn, project_name, "image", credits, "rejected", msg, None)
                st.error("Data rejected — record saved with status 'rejected'.")

    # refresh registry table
    # st.experimental_rerun()
    # st.rerun()


# Button: Issue simulated blockchain tx for the most recent unissued verified record
st.markdown("---")
st.subheader("Issue on ledger (simulated)")
if st.button("Issue simulated tx for latest verified & unissued record"):
    c = conn.cursor()
    c.execute("SELECT id, record_uuid FROM registry WHERE mrv_status='verified' AND (tx_hash IS NULL OR tx_hash='') ORDER BY id DESC LIMIT 1")
    row = c.fetchone()
    if not row:
        st.info("No verified unissued record found.")
    else:
        rec_id, rec_uuid = row
        tx_hash = simulate_tx_hash(rec_uuid)
        c.execute("UPDATE registry SET tx_hash=? WHERE id=?", (tx_hash, rec_id))
        conn.commit()
        st.success(f"Simulated tx issued: {tx_hash}")

# Show final registry again
st.markdown("---")
st.subheader("Latest registry entries")
st.dataframe(fetch_registry(conn))
