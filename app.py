"""
FAMS Module Backend - Sections 1-3
Covers: Overview, Module Details (CFAMS Setup), Execution Procedure (CFAMO Operations)
Stack: Flask + SQLite (swap to Oracle 10g in production per tech spec)
"""

from flask import Flask, request, jsonify, session, send_from_directory
from flask_cors import CORS
import sqlite3, os, uuid
from datetime import datetime, date
from functools import wraps

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "fams-dev-secret-2025")
CORS(app, supports_credentials=True)

DB_PATH = "fams.db"

# ─────────────────────────────────────────────
# DB INIT
# ─────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
        -- SECTION 2: CFAMS Setup Tables

        -- CARCMD: Custom Asset Reference Code Maintenance
        CREATE TABLE IF NOT EXISTS carcmd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference_type TEXT NOT NULL,
            reference_code TEXT NOT NULL,
            category TEXT NOT NULL,
            sub_category TEXT NOT NULL,
            scale TEXT NOT NULL,
            status TEXT DEFAULT 'ACTIVE',        -- ACTIVE / DELETED
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            UNIQUE(reference_type, reference_code)
        );

        -- CDSM: Designation Scale Mapping
        CREATE TABLE IF NOT EXISTS cdsm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            designation TEXT NOT NULL,
            scale TEXT NOT NULL,
            status TEXT DEFAULT 'ACTIVE',
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(designation, scale)
        );

        -- CSTOM / CSOTM: Sol Office Type Mapping
        CREATE TABLE IF NOT EXISTS csotm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_type TEXT NOT NULL,
            sol_id TEXT NOT NULL,
            status TEXT DEFAULT 'ACTIVE',
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(office_type, sol_id)
        );

        -- COTLM: Office Type Limit Mapping
        CREATE TABLE IF NOT EXISTS cotlm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_type TEXT NOT NULL,
            limit_amount REAL NOT NULL,
            limit_start_date TEXT NOT NULL,
            limit_end_date TEXT NOT NULL,
            status TEXT DEFAULT 'ACTIVE',
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(office_type, limit_start_date)
        );

        -- CCESM: Expense Type Sol Mapping
        CREATE TABLE IF NOT EXISTS ccesm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            expense_type TEXT NOT NULL,
            sol_id TEXT NOT NULL,
            depreciation_account TEXT NOT NULL,
            status TEXT DEFAULT 'ACTIVE',
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(expense_type, sol_id)
        );

        -- SECTION 3: CFAMO Operations Tables

        -- Asset Requests (AQ)
        CREATE TABLE IF NOT EXISTS asset_request (
            request_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            sub_category TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            description TEXT,
            quantity INTEGER NOT NULL DEFAULT 1,
            estimated_cost REAL,
            sol_id TEXT,
            status TEXT DEFAULT 'PENDING',       -- PENDING / APPROVED / REJECTED / PROCURED / TRANSFERRED / DISPOSED / SURRENDERED
            created_by TEXT NOT NULL,
            approved_by TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT
        );

        -- Asset Procurement (AP)
        CREATE TABLE IF NOT EXISTS asset_procurement (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            vendor TEXT NOT NULL,
            purchase_date TEXT NOT NULL,
            purchase_amount REAL NOT NULL,
            account_debit TEXT NOT NULL,
            account_credit TEXT NOT NULL,
            transaction_ref TEXT,
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES asset_request(request_id)
        );

        -- Asset Transfer (AT)
        CREATE TABLE IF NOT EXISTS asset_transfer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            sender_branch TEXT NOT NULL,
            receiver_branch TEXT NOT NULL,
            units INTEGER NOT NULL,
            total_asset_value REAL,
            net_asset_value REAL,
            transfer_date TEXT NOT NULL,
            transaction_ref TEXT,
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES asset_request(request_id)
        );

        -- Asset Disposal (AW)
        CREATE TABLE IF NOT EXISTS asset_disposal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            sale_amount REAL NOT NULL,
            disposal_type TEXT NOT NULL,       -- SALE / WRITEOFF / AUCTION
            tran_mode TEXT NOT NULL,           -- CASH / TRANSFER
            disposal_amount REAL NOT NULL,
            accumulated_depreciation REAL,
            gain_loss REAL,
            disposal_date TEXT NOT NULL,
            transaction_ref TEXT,
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES asset_request(request_id)
        );

        -- Asset Repair (AR)
        CREATE TABLE IF NOT EXISTS asset_repair (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            repair_amount REAL NOT NULL,
            repair_description TEXT,
            repair_date TEXT NOT NULL,
            transaction_ref TEXT,
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES asset_request(request_id)
        );

        -- Asset Revaluation (AL)
        CREATE TABLE IF NOT EXISTS asset_revaluation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            revaluation_amount REAL NOT NULL,
            net_asset_value REAL NOT NULL,
            revaluation_date TEXT NOT NULL,
            transaction_ref TEXT,
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES asset_request(request_id)
        );

        -- Asset Surrender (AS)
        CREATE TABLE IF NOT EXISTS asset_surrender (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            receiver_branch TEXT NOT NULL,
            units INTEGER NOT NULL,
            total_asset_value REAL,
            net_asset_value REAL,
            surrender_date TEXT NOT NULL,
            transaction_ref TEXT,
            verified_by TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES asset_request(request_id)
        );

        -- Users (minimal auth for maker-checker)
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT DEFAULT 'USER'            -- USER / TELLER / ADMIN
        );

        -- Seed two users for maker-checker demo
        INSERT OR IGNORE INTO users VALUES ('u1','maker_user','pass123','USER');
        INSERT OR IGNORE INTO users VALUES ('u2','checker_user','pass123','USER');
        INSERT OR IGNORE INTO users VALUES ('u3','teller_user','pass123','TELLER');
        """)

init_db()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def now():
    return datetime.now().isoformat(timespec='seconds')

def gen_request_id(category, sub_category):
    prefix = (category[:2] + sub_category[:2]).upper()
    uid = str(uuid.uuid4())[:8].upper()
    return f"REQ-{prefix}-{uid}"

def err(msg, code=400):
    return jsonify({"success": False, "error": msg}), code

def ok(data=None, msg="Success"):
    return jsonify({"success": True, "message": msg, "data": data})

def require_login(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return err("Not logged in", 401)
        return f(*args, **kwargs)
    return wrapper

# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────

@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE username=? AND password=?",
            (data.get("username"), data.get("password"))
        ).fetchone()
    if not user:
        return err("Invalid credentials", 401)
    session["user_id"] = user["user_id"]
    session["username"] = user["username"]
    session["role"] = user["role"]
    return ok({"username": user["username"], "role": user["role"]})

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return ok(msg="Logged out")

@app.route("/api/me")
def me():
    if "user_id" not in session:
        return err("Not logged in", 401)
    return ok({"username": session["username"], "role": session["role"]})

# ─────────────────────────────────────────────
# SECTION 2: CFAMS SETUP OPERATIONS
# ─────────────────────────────────────────────

# --- CARCMD ---
@app.route("/api/cfams/carcmd", methods=["GET"])
@require_login
def carcmd_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM carcmd WHERE status='ACTIVE'").fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfams/carcmd", methods=["POST"])
@require_login
def carcmd_add():
    d = request.json
    ref_type = d.get("reference_type","").strip()
    ref_code = d.get("reference_code","").strip()
    if not all([ref_type, ref_code, d.get("category"), d.get("sub_category"), d.get("scale")]):
        return err("All fields are required")
    with get_db() as conn:
        existing = conn.execute(
            "SELECT * FROM carcmd WHERE reference_type=? AND reference_code=?",
            (ref_type, ref_code)
        ).fetchone()
        if existing:
            return err("Record already exists for this reference type and code combination")
        conn.execute(
            "INSERT INTO carcmd (reference_type,reference_code,category,sub_category,scale,created_by,created_at) VALUES (?,?,?,?,?,?,?)",
            (ref_type, ref_code, d["category"], d["sub_category"], d["scale"], session["username"], now())
        )
    return ok(msg="CARCMD record added. Pending verification.")

@app.route("/api/cfams/carcmd/<int:rec_id>/verify", methods=["POST"])
@require_login
def carcmd_verify(rec_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM carcmd WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["created_by"] == session["username"]:
            return err("Same user cannot verify the record")
        if rec["verified_by"]:
            return err("Record already verified")
        conn.execute("UPDATE carcmd SET verified_by=?, updated_at=? WHERE id=?",
                     (session["username"], now(), rec_id))
    return ok(msg="CARCMD record verified")

@app.route("/api/cfams/carcmd/<int:rec_id>", methods=["PUT"])
@require_login
def carcmd_modify(rec_id):
    d = request.json
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM carcmd WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["created_by"] == session["username"] and rec["verified_by"]:
            return err("Different user must modify a verified record")
        conn.execute(
            "UPDATE carcmd SET category=?,sub_category=?,scale=?,updated_at=?,verified_by=NULL WHERE id=?",
            (d.get("category", rec["category"]), d.get("sub_category", rec["sub_category"]),
             d.get("scale", rec["scale"]), now(), rec_id)
        )
    return ok(msg="CARCMD record modified. Re-verification required.")

@app.route("/api/cfams/carcmd/<int:rec_id>", methods=["DELETE"])
@require_login
def carcmd_delete(rec_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM carcmd WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["status"] == "DELETED":
            return err("Record already deleted")
        conn.execute("UPDATE carcmd SET status='DELETED',updated_at=? WHERE id=?", (now(), rec_id))
    return ok(msg="CARCMD record deleted")

# --- CDSM: Designation Scale Mapping ---
@app.route("/api/cfams/cdsm", methods=["GET"])
@require_login
def cdsm_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM cdsm WHERE status='ACTIVE'").fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfams/cdsm", methods=["POST"])
@require_login
def cdsm_add():
    d = request.json
    if not all([d.get("designation"), d.get("scale")]):
        return err("Designation and scale are required")
    with get_db() as conn:
        existing = conn.execute("SELECT id FROM cdsm WHERE designation=? AND scale=?",
                                (d["designation"], d["scale"])).fetchone()
        if existing:
            return err("Record already exists for this designation and scale")
        conn.execute("INSERT INTO cdsm (designation,scale,created_by,created_at) VALUES (?,?,?,?)",
                     (d["designation"], d["scale"], session["username"], now()))
    return ok(msg="CDSM record added")

@app.route("/api/cfams/cdsm/<int:rec_id>/verify", methods=["POST"])
@require_login
def cdsm_verify(rec_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM cdsm WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["created_by"] == session["username"]:
            return err("Same user cannot verify the record")
        conn.execute("UPDATE cdsm SET verified_by=? WHERE id=?", (session["username"], rec_id))
    return ok(msg="CDSM record verified")

# --- CSOTM: Sol Office Type Mapping ---
@app.route("/api/cfams/csotm", methods=["GET"])
@require_login
def csotm_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM csotm WHERE status='ACTIVE'").fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfams/csotm", methods=["POST"])
@require_login
def csotm_add():
    d = request.json
    if not all([d.get("office_type"), d.get("sol_id")]):
        return err("Office type and Sol ID are required")
    with get_db() as conn:
        existing = conn.execute("SELECT id FROM csotm WHERE office_type=? AND sol_id=?",
                                (d["office_type"], d["sol_id"])).fetchone()
        if existing:
            return err("Record already exists for this office type and Sol ID")
        conn.execute("INSERT INTO csotm (office_type,sol_id,created_by,created_at) VALUES (?,?,?,?)",
                     (d["office_type"], d["sol_id"], session["username"], now()))
    return ok(msg="CSOTM record added")

@app.route("/api/cfams/csotm/<int:rec_id>/verify", methods=["POST"])
@require_login
def csotm_verify(rec_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM csotm WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["created_by"] == session["username"]:
            return err("Same user cannot verify the record")
        conn.execute("UPDATE csotm SET verified_by=? WHERE id=?", (session["username"], rec_id))
    return ok(msg="CSOTM record verified")

# --- COTLM: Office Type Limit Mapping ---
@app.route("/api/cfams/cotlm", methods=["GET"])
@require_login
def cotlm_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM cotlm WHERE status='ACTIVE'").fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfams/cotlm", methods=["POST"])
@require_login
def cotlm_add():
    d = request.json
    if not all([d.get("office_type"), d.get("limit_amount"), d.get("limit_start_date"), d.get("limit_end_date")]):
        return err("All fields are required")
    if d["limit_end_date"] < d["limit_start_date"]:
        return err("Limit end date cannot be less than limit start date")
    with get_db() as conn:
        existing = conn.execute("SELECT id FROM cotlm WHERE office_type=? AND limit_start_date=?",
                                (d["office_type"], d["limit_start_date"])).fetchone()
        if existing:
            return err("Record already exists for this office type and start date")
        conn.execute(
            "INSERT INTO cotlm (office_type,limit_amount,limit_start_date,limit_end_date,created_by,created_at) VALUES (?,?,?,?,?,?)",
            (d["office_type"], d["limit_amount"], d["limit_start_date"], d["limit_end_date"], session["username"], now())
        )
    return ok(msg="COTLM record added")

@app.route("/api/cfams/cotlm/<int:rec_id>/verify", methods=["POST"])
@require_login
def cotlm_verify(rec_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM cotlm WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["created_by"] == session["username"]:
            return err("Same user cannot verify the record")
        conn.execute("UPDATE cotlm SET verified_by=? WHERE id=?", (session["username"], rec_id))
    return ok(msg="COTLM record verified")

# --- CCESM: Expense Type Sol Mapping ---
VALID_DEPR_ACCOUNTS = ["ACC001", "ACC002", "ACC003", "ACC004", "ACC005"]  # configurable

@app.route("/api/cfams/ccesm", methods=["GET"])
@require_login
def ccesm_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM ccesm WHERE status='ACTIVE'").fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfams/ccesm", methods=["POST"])
@require_login
def ccesm_add():
    d = request.json
    if not all([d.get("expense_type"), d.get("sol_id"), d.get("depreciation_account")]):
        return err("All fields are required")
    if d["depreciation_account"] not in VALID_DEPR_ACCOUNTS:
        return err(f"Depreciation account number is invalid. Valid accounts: {VALID_DEPR_ACCOUNTS}")
    with get_db() as conn:
        existing = conn.execute("SELECT id FROM ccesm WHERE expense_type=? AND sol_id=?",
                                (d["expense_type"], d["sol_id"])).fetchone()
        if existing:
            return err("Record already exists for this expense type and Sol ID")
        conn.execute(
            "INSERT INTO ccesm (expense_type,sol_id,depreciation_account,created_by,created_at) VALUES (?,?,?,?,?)",
            (d["expense_type"], d["sol_id"], d["depreciation_account"], session["username"], now())
        )
    return ok(msg="CCESM record added")

@app.route("/api/cfams/ccesm/<int:rec_id>/verify", methods=["POST"])
@require_login
def ccesm_verify(rec_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM ccesm WHERE id=?", (rec_id,)).fetchone()
        if not rec:
            return err("Record not found", 404)
        if rec["created_by"] == session["username"]:
            return err("Same user cannot verify the record")
        conn.execute("UPDATE ccesm SET verified_by=? WHERE id=?", (session["username"], rec_id))
    return ok(msg="CCESM record verified")

# ─────────────────────────────────────────────
# SECTION 3: CFAMO OPERATIONS
# ─────────────────────────────────────────────

# Helper: get categories from carcmd setup
@app.route("/api/cfamo/categories", methods=["GET"])
@require_login
def get_categories():
    with get_db() as conn:
        rows = conn.execute("SELECT DISTINCT category FROM carcmd WHERE status='ACTIVE'").fetchall()
    return ok([r["category"] for r in rows])

@app.route("/api/cfamo/subcategories/<category>", methods=["GET"])
@require_login
def get_subcategories(category):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT sub_category FROM carcmd WHERE category=? AND status='ACTIVE'", (category,)
        ).fetchall()
    return ok([r["sub_category"] for r in rows])

# AQ - Asset Request
@app.route("/api/cfamo/request", methods=["POST"])
@require_login
def asset_request():
    d = request.json
    required = ["category", "sub_category", "asset_name", "quantity"]
    for f in required:
        if not d.get(f):
            return err(f"Field '{f}' is required")

    with get_db() as conn:
        # Validate category/sub_category exists in CARCMD setup
        valid = conn.execute(
            "SELECT id FROM carcmd WHERE category=? AND sub_category=? AND status='ACTIVE'",
            (d["category"], d["sub_category"])
        ).fetchone()
        if not valid:
            return err("Invalid category/sub-category. Not found in CARCMD setup.")

        req_id = gen_request_id(d["category"], d["sub_category"])
        conn.execute(
            """INSERT INTO asset_request
               (request_id,category,sub_category,asset_name,description,quantity,estimated_cost,sol_id,created_by,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (req_id, d["category"], d["sub_category"], d["asset_name"],
             d.get("description",""), d["quantity"], d.get("estimated_cost"),
             d.get("sol_id"), session["username"], now())
        )
    return ok({"request_id": req_id}, msg=f"Asset request created. Request ID: {req_id}")

# Get requests by category/subcategory (for searcher in AA, AM, etc.)
@app.route("/api/cfamo/requests", methods=["GET"])
@require_login
def list_requests():
    category = request.args.get("category")
    sub_category = request.args.get("sub_category")
    status = request.args.get("status")
    q = "SELECT * FROM asset_request WHERE 1=1"
    params = []
    if category:
        q += " AND category=?"; params.append(category)
    if sub_category:
        q += " AND sub_category=?"; params.append(sub_category)
    if status:
        q += " AND status=?"; params.append(status)
    with get_db() as conn:
        rows = conn.execute(q, params).fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfamo/requests/<request_id>", methods=["GET"])
@require_login
def get_request(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (request_id,)).fetchone()
    if not rec:
        return err("Request not found", 404)
    return ok(dict(rec))

# AA - Asset Approval/Reject
@app.route("/api/cfamo/approve", methods=["POST"])
@require_login
def asset_approve():
    d = request.json
    req_id = d.get("request_id")
    action = d.get("action")  # APPROVED or REJECTED
    if action not in ["APPROVED", "REJECTED"]:
        return err("Action must be APPROVED or REJECTED")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must approve/reject the request (maker-checker rule)")
        if rec["status"] not in ["PENDING", "MODIFIED"]:
            return err(f"Request cannot be approved in current status: {rec['status']}")
        conn.execute(
            "UPDATE asset_request SET status=?,approved_by=?,updated_at=? WHERE request_id=?",
            (action, session["username"], now(), req_id)
        )
    return ok(msg=f"Asset request {action.lower()}")

# AM - Asset Modify
@app.route("/api/cfamo/request/<request_id>", methods=["PUT"])
@require_login
def asset_modify(request_id):
    d = request.json
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (request_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["status"] == "PROCURED":
            return err("Asset already procured. Modification not allowed.")
        if rec["status"] not in ["PENDING", "MODIFIED", "APPROVED"]:
            return err(f"Cannot modify request in status: {rec['status']}")
        conn.execute(
            """UPDATE asset_request SET asset_name=?,description=?,quantity=?,estimated_cost=?,
               status='MODIFIED',updated_at=? WHERE request_id=?""",
            (d.get("asset_name", rec["asset_name"]), d.get("description", rec["description"]),
             d.get("quantity", rec["quantity"]), d.get("estimated_cost", rec["estimated_cost"]),
             now(), request_id)
        )
    return ok(msg="Asset request modified. Re-approval required.")

# AD - Asset Delete
@app.route("/api/cfamo/request/<request_id>", methods=["DELETE"])
@require_login
def asset_delete(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (request_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["status"] == "PROCURED":
            return err("Asset already procured. Deletion not allowed.")
        conn.execute(
            "UPDATE asset_request SET status='DELETED',updated_at=? WHERE request_id=?",
            (now(), request_id)
        )
    return ok(msg="Asset request deleted")

# AP - Asset Procurement
@app.route("/api/cfamo/procure", methods=["POST"])
@require_login
def asset_procure():
    d = request.json
    req_id = d.get("request_id")
    required = ["request_id", "vendor", "purchase_date", "purchase_amount", "account_debit", "account_credit"]
    for f in required:
        if not d.get(f):
            return err(f"Field '{f}' is required")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["status"] != "APPROVED":
            return err("Asset must be approved before procurement")
        tref = f"TXN-{uuid.uuid4().hex[:10].upper()}"
        conn.execute(
            """INSERT INTO asset_procurement
               (request_id,vendor,purchase_date,purchase_amount,account_debit,account_credit,transaction_ref,created_by,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (req_id, d["vendor"], d["purchase_date"], d["purchase_amount"],
             d["account_debit"], d["account_credit"], tref, session["username"], now())
        )
        conn.execute("UPDATE asset_request SET status='PROCURED',updated_at=? WHERE request_id=?", (now(), req_id))
    return ok({"transaction_ref": tref}, msg="Asset procured. Transaction created. Pending verify.")

@app.route("/api/cfamo/procure/<request_id>/verify", methods=["POST"])
@require_login
def procure_verify(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_procurement WHERE request_id=? AND verified_by IS NULL",
                           (request_id,)).fetchone()
        if not rec:
            return err("Procurement record not found or already verified", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must verify (maker-checker rule)")
        conn.execute("UPDATE asset_procurement SET verified_by=? WHERE id=?", (session["username"], rec["id"]))
    return ok(msg="Procurement verified. Transaction confirmed.")

# AT - Asset Transfer
@app.route("/api/cfamo/transfer", methods=["POST"])
@require_login
def asset_transfer():
    d = request.json
    req_id = d.get("request_id")
    required = ["request_id", "receiver_branch", "units"]
    for f in required:
        if not d.get(f):
            return err(f"Field '{f}' is required")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["status"] != "PROCURED":
            return err("Asset must be procured before transfer")

        # Validate: 3-field combo (category, sub_category, sol_id) must exist in CCESM
        ccesm_check = conn.execute(
            """SELECT id FROM ccesm
               WHERE expense_type=? AND sol_id=? AND status='ACTIVE'""",
            (rec["sub_category"], d["receiver_branch"])
        ).fetchone()
        if not ccesm_check:
            return err("Transfer failed: category/sub-category/receiver-branch combination not set up in CCESM")

        # Validate receiver branch exists in CSOTM
        branch_check = conn.execute("SELECT id FROM csotm WHERE sol_id=? AND status='ACTIVE'",
                                    (d["receiver_branch"],)).fetchone()
        if not branch_check:
            return err("Receiver branch is invalid. Not found in Sol Office Type Mapping.")

        total_val = float(rec["estimated_cost"] or 0) * int(d["units"])
        net_val = total_val * 0.85  # simplified; hook into real depreciation calc
        tref = f"TXN-{uuid.uuid4().hex[:10].upper()}"

        conn.execute(
            """INSERT INTO asset_transfer
               (request_id,sender_branch,receiver_branch,units,total_asset_value,net_asset_value,transfer_date,transaction_ref,created_by,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (req_id, rec["sol_id"] or "HO", d["receiver_branch"], d["units"],
             total_val, net_val, d.get("transfer_date", now()[:10]), tref, session["username"], now())
        )
        conn.execute("UPDATE asset_request SET status='TRANSFERRED',updated_at=? WHERE request_id=?", (now(), req_id))
    return ok({"transaction_ref": tref, "total_asset_value": total_val, "net_asset_value": net_val},
              msg="Asset transfer recorded. Pending verify.")

@app.route("/api/cfamo/transfer/<request_id>/verify", methods=["POST"])
@require_login
def transfer_verify(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_transfer WHERE request_id=? AND verified_by IS NULL",
                           (request_id,)).fetchone()
        if not rec:
            return err("Transfer record not found or already verified", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must verify")
        conn.execute("UPDATE asset_transfer SET verified_by=? WHERE id=?", (session["username"], rec["id"]))
    return ok(msg="Transfer verified")

# AW - Asset Disposal
@app.route("/api/cfamo/dispose", methods=["POST"])
@require_login
def asset_dispose():
    d = request.json
    req_id = d.get("request_id")
    required = ["request_id", "sale_amount", "disposal_type", "tran_mode", "disposal_amount"]
    for f in required:
        if not d.get(f):
            return err(f"Field '{f}' is required")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["status"] != "PROCURED":
            return err("Asset must be procured before disposal")
        gain_loss = float(d["sale_amount"]) - float(d["disposal_amount"])
        tref = f"TXN-{uuid.uuid4().hex[:10].upper()}"
        conn.execute(
            """INSERT INTO asset_disposal
               (request_id,sale_amount,disposal_type,tran_mode,disposal_amount,gain_loss,disposal_date,transaction_ref,created_by,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (req_id, d["sale_amount"], d["disposal_type"], d["tran_mode"], d["disposal_amount"],
             gain_loss, d.get("disposal_date", now()[:10]), tref, session["username"], now())
        )
        conn.execute("UPDATE asset_request SET status='DISPOSED',updated_at=? WHERE request_id=?", (now(), req_id))
    return ok({"transaction_ref": tref, "gain_loss": gain_loss}, msg="Asset disposal recorded. Pending verify.")

@app.route("/api/cfamo/dispose/<request_id>/verify", methods=["POST"])
@require_login
def dispose_verify(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_disposal WHERE request_id=? AND verified_by IS NULL",
                           (request_id,)).fetchone()
        if not rec:
            return err("Disposal record not found or already verified", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must verify")
        if rec["tran_mode"] == "CASH" and session["role"] != "TELLER":
            return err("Cash mode disposal can only be verified by a Teller")
        conn.execute("UPDATE asset_disposal SET verified_by=? WHERE id=?", (session["username"], rec["id"]))
    return ok(msg="Disposal verified")

# AR - Asset Repair
@app.route("/api/cfamo/repair", methods=["POST"])
@require_login
def asset_repair():
    d = request.json
    req_id = d.get("request_id")
    if not all([req_id, d.get("repair_amount")]):
        return err("request_id and repair_amount are required")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        tref = f"TXN-{uuid.uuid4().hex[:10].upper()}"
        conn.execute(
            """INSERT INTO asset_repair
               (request_id,repair_amount,repair_description,repair_date,transaction_ref,created_by,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (req_id, d["repair_amount"], d.get("repair_description",""),
             d.get("repair_date", now()[:10]), tref, session["username"], now())
        )
    return ok({"transaction_ref": tref}, msg="Repair recorded. Pending verify.")

@app.route("/api/cfamo/repair/<request_id>/verify", methods=["POST"])
@require_login
def repair_verify(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_repair WHERE request_id=? AND verified_by IS NULL",
                           (request_id,)).fetchone()
        if not rec:
            return err("Repair record not found or already verified", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must verify")
        conn.execute("UPDATE asset_repair SET verified_by=? WHERE id=?", (session["username"], rec["id"]))
    return ok(msg="Repair verified")

# AL - Asset Revaluation
@app.route("/api/cfamo/revalue", methods=["POST"])
@require_login
def asset_revalue():
    d = request.json
    req_id = d.get("request_id")
    if not all([req_id, d.get("revaluation_amount"), d.get("net_asset_value")]):
        return err("request_id, revaluation_amount and net_asset_value are required")
    if float(d["revaluation_amount"]) < float(d["net_asset_value"]):
        return err("Revaluation amount cannot be less than net asset value")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        tref = f"TXN-{uuid.uuid4().hex[:10].upper()}"
        conn.execute(
            """INSERT INTO asset_revaluation
               (request_id,revaluation_amount,net_asset_value,revaluation_date,transaction_ref,created_by,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (req_id, d["revaluation_amount"], d["net_asset_value"],
             d.get("revaluation_date", now()[:10]), tref, session["username"], now())
        )
    return ok({"transaction_ref": tref}, msg="Revaluation recorded. Pending verify.")

@app.route("/api/cfamo/revalue/<request_id>/verify", methods=["POST"])
@require_login
def revalue_verify(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_revaluation WHERE request_id=? AND verified_by IS NULL",
                           (request_id,)).fetchone()
        if not rec:
            return err("Revaluation record not found or already verified", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must verify")
        conn.execute("UPDATE asset_revaluation SET verified_by=? WHERE id=?", (session["username"], rec["id"]))
    return ok(msg="Revaluation verified")

# AS - Asset Surrender
@app.route("/api/cfamo/surrender", methods=["POST"])
@require_login
def asset_surrender():
    d = request.json
    req_id = d.get("request_id")
    if not all([req_id, d.get("receiver_branch"), d.get("units")]):
        return err("request_id, receiver_branch and units are required")
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_request WHERE request_id=?", (req_id,)).fetchone()
        if not rec:
            return err("Request not found", 404)
        if rec["status"] != "PROCURED":
            return err("Asset must be procured before surrender")
        total_val = float(rec["estimated_cost"] or 0) * int(d["units"])
        net_val = total_val * 0.80
        tref = f"TXN-{uuid.uuid4().hex[:10].upper()}"
        conn.execute(
            """INSERT INTO asset_surrender
               (request_id,receiver_branch,units,total_asset_value,net_asset_value,surrender_date,transaction_ref,created_by,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (req_id, d["receiver_branch"], d["units"], total_val, net_val,
             d.get("surrender_date", now()[:10]), tref, session["username"], now())
        )
        conn.execute("UPDATE asset_request SET status='SURRENDERED',updated_at=? WHERE request_id=?", (now(), req_id))
    return ok({"transaction_ref": tref, "total_asset_value": total_val, "net_asset_value": net_val},
              msg="Surrender recorded. Pending verify.")

@app.route("/api/cfamo/surrender/<request_id>/verify", methods=["POST"])
@require_login
def surrender_verify(request_id):
    with get_db() as conn:
        rec = conn.execute("SELECT * FROM asset_surrender WHERE request_id=? AND verified_by IS NULL",
                           (request_id,)).fetchone()
        if not rec:
            return err("Surrender record not found or already verified", 404)
        if rec["created_by"] == session["username"]:
            return err("Different user must verify")
        conn.execute("UPDATE asset_surrender SET verified_by=? WHERE id=?", (session["username"], rec["id"]))
    return ok(msg="Surrender verified")

# ─────────────────────────────────────────────
# DASHBOARD STATS
# ─────────────────────────────────────────────
@app.route("/api/dashboard/stats", methods=["GET"])
@require_login
def dashboard_stats():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as c FROM asset_request").fetchone()["c"]
        pending = conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='PENDING'").fetchone()["c"]
        approved = conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='APPROVED'").fetchone()["c"]
        procured = conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='PROCURED'").fetchone()["c"]
        disposed = conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='DISPOSED'").fetchone()["c"]
        setup_count = conn.execute("SELECT COUNT(*) as c FROM carcmd WHERE status='ACTIVE'").fetchone()["c"]
    return ok({
        "total_requests": total,
        "pending": pending,
        "approved": approved,
        "procured": procured,
        "disposed": disposed,
        "setup_records": setup_count
    })

# ─────────────────────────────────────────────
# SERVE FRONTEND
# ─────────────────────────────────────────────

@app.route("/")
def index():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base_dir, "index.html")

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    import webbrowser, threading
    port = int(os.environ.get("PORT", 5000))

    def open_browser():
        import time; time.sleep(1)
        webbrowser.open(f"http://localhost:{port}")

    threading.Thread(target=open_browser, daemon=True).start()
    print(f"\n{'='*50}")
    print(f"  FAMS is running at: http://localhost:{port}")
    print(f"  Press Ctrl+C to stop")
    print(f"{'='*50}\n")
    app.run(debug=False, port=port, use_reloader=False)
