from __future__ import annotations
import sqlite3
import datetime
import io
import uuid
import hashlib
import sys
import os
from typing import Tuple, List
try:
    import numpy as np
    from PIL import Image
except Exception:
    raise RuntimeError("This module requires numpy and pillow. Install with: pip install numpy pillow")
HAS_STREAMLIT = True
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None
    HAS_STREAMLIT = False
DB_FILE = os.environ.get("BLUECARBON_DB", "bluecarbon.db")

def get_conn(path: str = DB_FILE) -> sqlite3.Connection:
    return sqlite3.connect(path, check_same_thread=False)

def init_db(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        name TEXT,
        location TEXT,
        description TEXT,
        created_at TEXT,
        tx_hash TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS credits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        project_id INTEGER,
        data_type TEXT,
        credits REAL,
        status TEXT,
        notes TEXT,
        timestamp TEXT,
        tx_hash TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS complaints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        project_id INTEGER,
        complaint TEXT,
        status TEXT,
        created_at TEXT,
        tx_hash TEXT
    )
    """)
    conn.commit()

def simulate_tx_hash(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()

def add_project(conn: sqlite3.Connection, name: str, location: str, description: str) -> Tuple[str, str]:
    uid = str(uuid.uuid4())
    created_at = datetime.datetime.utcnow().isoformat()
    tx = simulate_tx_hash(uid + created_at)
    c = conn.cursor()
    c.execute(
        "INSERT INTO projects (uuid, name, location, description, created_at, tx_hash) VALUES (?, ?, ?, ?, ?, ?)",
        (uid, name, location, description, created_at, tx),
    )
    conn.commit()
    return uid, tx

def calculate_carbon_credits(image_bytes: bytes) -> Tuple[float, float]:
    img = Image.open(io.BytesIO(image_bytes)).convert("L")
    arr = np.array(img, dtype=np.float32)
    variance = float(np.var(arr))
    credits = round(variance / 100.0, 2)
    return credits, variance

def add_credit(conn: sqlite3.Connection, project_id: int, data_type: str, credits: float, status: str, notes: str) -> Tuple[str, str]:
    uid = str(uuid.uuid4())
    ts = datetime.datetime.utcnow().isoformat()
    tx = simulate_tx_hash(uid + str(credits) + ts)
    c = conn.cursor()
    c.execute(
        "INSERT INTO credits (uuid, project_id, data_type, credits, status, notes, timestamp, tx_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (uid, project_id, data_type, credits, status, notes, ts, tx),
    )
    conn.commit()
    return uid, tx

def add_complaint(conn: sqlite3.Connection, project_id: int, complaint: str) -> Tuple[str, str]:
    uid = str(uuid.uuid4())
    ts = datetime.datetime.utcnow().isoformat()
    tx = simulate_tx_hash(uid + complaint + ts)
    c = conn.cursor()
    c.execute(
        "INSERT INTO complaints (uuid, project_id, complaint, status, created_at, tx_hash) VALUES (?, ?, ?, ?, ?, ?)",
        (uid, project_id, complaint, "pending", ts, tx),
    )
    conn.commit()
    return uid, tx

def list_projects(conn: sqlite3.Connection) -> List[tuple]:
    c = conn.cursor()
    return c.execute("SELECT id, uuid, name, location, description, created_at, tx_hash FROM projects ORDER BY id DESC").fetchall()

def list_credits(conn: sqlite3.Connection) -> List[tuple]:
    c = conn.cursor()
    return c.execute("SELECT id, uuid, project_id, data_type, credits, status, notes, timestamp, tx_hash FROM credits ORDER BY id DESC").fetchall()

def list_complaints(conn: sqlite3.Connection) -> List[tuple]:
    c = conn.cursor()
    return c.execute("SELECT id, uuid, project_id, complaint, status, created_at, tx_hash FROM complaints ORDER BY id DESC").fetchall()

if HAS_STREAMLIT:
    def run_streamlit_app(db_path: str = DB_FILE):
        conn = get_conn(db_path)
        init_db(conn)
        st.set_page_config(page_title="Blue Carbon MVP", layout="wide")
        st.title("🌍 Blue Carbon Registry – MVP Prototype")
        menu = ["Project Registry", "Upload Data & Verify", "Carbon Credit Registry", "Complaint Registry"]
        choice = st.sidebar.radio("Navigate", menu)
        if choice == "Project Registry":
            st.header("📌 Register a New Project")
            with st.form("project_form"):
                name = st.text_input("Project Name")
                location = st.text_input("Location")
                desc = st.text_area("Description")
                submit = st.form_submit_button("Register Project")
                if submit and name:
                    uid, tx = add_project(conn, name, location, desc)
                    st.success(f"✅ Project '{name}' registered (tx: {tx[:10]}...)")
            st.subheader("Registered Projects")
            projects = list_projects(conn)
            for p in projects:
                st.write(f"**ID {p[0]} | {p[2]} ({p[3]})** - {p[4]} | Created: {p[5]} | tx: {p[6][:10]}...")
        elif choice == "Upload Data & Verify":
            st.header("📤 Upload Proof / Data")
            projects = list_projects(conn)
            if not projects:
                st.warning("⚠️ No projects registered yet. Please register a project first.")
            else:
                project_map = {p[0]: p for p in projects}
                selected = st.selectbox("Select Project", options=[p[0] for p in projects], format_func=lambda x: f"{project_map[x][2]} (ID {x})")
                uploaded_file = st.file_uploader("Upload Image (Proof)", type=["jpg", "png", "jpeg"])
                if uploaded_file is not None:
                    image_bytes = uploaded_file.read()
                    credits, variance = calculate_carbon_credits(image_bytes)
                    st.image(image_bytes, caption="Uploaded Image", use_column_width=True)
                    st.write(f"🧮 Variance: {variance:.2f}")
                    st.write(f"🌱 Estimated Carbon Credits: {credits}")
                    uid, tx = add_credit(conn, selected, "image", credits, "verified", f"variance={variance:.2f}")
                    st.success(f"✅ Data verified & credits issued (simulated tx: {tx[:10]}...)")
        elif choice == "Carbon Credit Registry":
            st.header("📊 Carbon Credit Ledger (Simulated Blockchain)")
            credits = list_credits(conn)
            if credits:
                for cr in credits:
                    st.write(f"ID {cr[0]} | Project {cr[2]} | Type: {cr[3]} | Credits: {cr[4]} | Status: {cr[5]} | Notes: {cr[6]} | ts: {cr[7]} | tx: {cr[8][:10]}...")
            else:
                st.info("No credits issued yet.")
        elif choice == "Complaint Registry":
            st.header("🛑 Complaint Ledger (Simulated Blockchain)")
            projects = list_projects(conn)
            if projects:
                project_map = {p[0]: p for p in projects}
                with st.form("complaint_form"):
                    selected = st.selectbox("Select Project", options=[p[0] for p in projects], format_func=lambda x: f"{project_map[x][2]} (ID {x})")
                    complaint_text = st.text_area("Enter Complaint")
                    submit = st.form_submit_button("Submit Complaint")
                    if submit and complaint_text:
                        uid, tx = add_complaint(conn, selected, complaint_text)
                        st.success(f"✅ Complaint logged (tx {tx[:10]}...)")
            complaints = list_complaints(conn)
            if complaints:
                for cm in complaints:
                    st.write(f"ID {cm[0]} | Project {cm[2]} | Complaint: {cm[3]} | Status: {cm[4]} | {cm[5]} | tx: {cm[6][:10]}...")
            else:
                st.info("No complaints logged yet.")
    if __name__ == "__main__":
        run_streamlit_app()
else:
    import unittest
    class TestBlueCarbonMVP(unittest.TestCase):
        def setUp(self):
            self.conn = sqlite3.connect(":memory:")
            init_db(self.conn)
        def tearDown(self):
            self.conn.close()
        def test_db_tables_created(self):
            c = self.conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {r[0] for r in c.fetchall()}
            self.assertIn('projects', tables)
            self.assertIn('credits', tables)
            self.assertIn('complaints', tables)
        def test_add_project_and_retrieve(self):
            uid, tx = add_project(self.conn, 'Test Project', '0,0', 'desc')
            c = self.conn.cursor()
            c.execute("SELECT uuid, name, tx_hash FROM projects WHERE uuid=?", (uid,))
            row = c.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[1], 'Test Project')
            self.assertEqual(row[0], uid)
            self.assertTrue(len(row[2]) >= 10)
        def test_calculate_carbon_credits_nonzero(self):
            arr = np.random.randint(0, 256, size=(64, 64), dtype=np.uint8)
            img = Image.fromarray(arr, mode='L')
            bio = io.BytesIO()
            img.save(bio, format='PNG')
            img_bytes = bio.getvalue()
            credits, variance = calculate_carbon_credits(img_bytes)
            self.assertGreater(variance, 0)
            self.assertGreaterEqual(credits, 0)
        def test_add_credit_and_complaint(self):
            uid, tx = add_project(self.conn, 'P2', 'loc', 'd')
            c = self.conn.cursor()
            c.execute("SELECT id FROM projects WHERE uuid=?", (uid,))
            pid = c.fetchone()[0]
            cu, ctx = add_credit(self.conn, pid, 'image', 12.34, 'verified', 'notes')
            self.assertIsNotNone(cu)
            c.execute("SELECT credits, tx_hash FROM credits WHERE uuid=?", (cu,))
            crow = c.fetchone()
            self.assertAlmostEqual(crow[0], 12.34)
            self.assertTrue(len(crow[1]) >= 10)
            compu, comptx = add_complaint(self.conn, pid, 'This is a complaint')
            c.execute("SELECT complaint, tx_hash FROM complaints WHERE uuid=?", (compu,))
            comprow = c.fetchone()
            self.assertEqual(comprow[0], 'This is a complaint')
            self.assertTrue(len(comprow[1]) >= 10)
    def cli_demo():
        print("Running CLI demo because Streamlit is not available in this environment.")
        conn = get_conn(':memory:')
        init_db(conn)
        print("Created in-memory DB and initialized tables.")
        uid, tx = add_project(conn, 'Demo Mangrove', '10.0,20.0', 'Demo project for Blue Carbon')
        print(f"Added project: uuid={uid} tx_hash={tx[:12]}...")
        arr = np.tile(np.linspace(0, 255, 128, dtype=np.uint8), (128, 1))
        img = Image.fromarray(arr, mode='L')
        bio = io.BytesIO()
        img.save(bio, format='PNG')
        img_bytes = bio.getvalue()
        credits, variance = calculate_carbon_credits(img_bytes)
        print(f"Demo image variance={variance:.2f}, estimated credits={credits}")
        c = conn.cursor()
        c.execute("SELECT id FROM projects WHERE uuid=?", (uid,))
        pid = c.fetchone()[0]
        cu, ctx = add_credit(conn, pid, 'image', credits, 'verified', f'variance={variance:.2f}')
        print(f"Inserted credit entry uuid={cu}, tx_hash={ctx[:12]}...")
        compu, comptx = add_complaint(conn, pid, 'Sample complaint for demo')
        print(f"Inserted complaint uuid={compu}, tx_hash={comptx[:12]}...")
        print("Listing projects:")
        for p in list_projects(conn):
            print(p)
        print("Listing credits:")
        for cr in list_credits(conn):
            print(cr)
        print("Listing complaints:")
        for cm in list_complaints(conn):
            print(cm)
    if __name__ == '__main__':
        print("Streamlit is not available. Running unit tests...")
        import unittest as _unittest
        _unittest.main(exit=False)
        print('\nUnit tests completed. Running CLI demo...\n')
        cli_demo()
