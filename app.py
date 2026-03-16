"""
FAMS Module Backend
Roles: ADMIN -> CFAMS | USER/TELLER -> CFAMO
"""
from flask import Flask, request, jsonify, session, send_from_directory
from flask_cors import CORS
import sqlite3, os, uuid
from datetime import datetime
from functools import wraps

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(BASE_DIR,"static"), template_folder=BASE_DIR)
app.secret_key = os.environ.get("SECRET_KEY", "fams-dev-secret-2025")
CORS(app, supports_credentials=True)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fams.db")
VALID_DEPR_ACCOUNTS = ["ACC001","ACC002","ACC003","ACC004","ACC005"]

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def today(): return datetime.now().strftime("%Y-%m-%d")
def gen_rid(cat,sub): return f"REQ-{(cat[:2]+sub[:2]).upper()}-{uuid.uuid4().hex[:8].upper()}"
def err(msg,code=400): return jsonify({"success":False,"error":msg}),code
def ok(data=None,msg="Success"): return jsonify({"success":True,"message":msg,"data":data})

def init_db():
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY,username TEXT UNIQUE NOT NULL,password TEXT NOT NULL,role TEXT DEFAULT 'USER');
        INSERT OR IGNORE INTO users VALUES('u1','maker_user','pass123','USER');
        INSERT OR IGNORE INTO users VALUES('u2','checker_user','pass123','USER');
        INSERT OR IGNORE INTO users VALUES('u3','teller_user','pass123','TELLER');
        INSERT OR IGNORE INTO users VALUES('u4','admin_user','admin123','ADMIN');

        CREATE TABLE IF NOT EXISTS carcmd(id INTEGER PRIMARY KEY AUTOINCREMENT,reference_type TEXT NOT NULL,reference_code TEXT NOT NULL,category TEXT NOT NULL,sub_category TEXT NOT NULL,scale TEXT NOT NULL,status TEXT DEFAULT 'ACTIVE',verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT,UNIQUE(reference_type,reference_code));
        CREATE TABLE IF NOT EXISTS cdsm(id INTEGER PRIMARY KEY AUTOINCREMENT,designation TEXT NOT NULL,scale TEXT NOT NULL,status TEXT DEFAULT 'ACTIVE',verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(designation,scale));
        CREATE TABLE IF NOT EXISTS csotm(id INTEGER PRIMARY KEY AUTOINCREMENT,office_type TEXT NOT NULL,sol_id TEXT NOT NULL,status TEXT DEFAULT 'ACTIVE',verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(office_type,sol_id));
        CREATE TABLE IF NOT EXISTS cotlm(id INTEGER PRIMARY KEY AUTOINCREMENT,office_type TEXT NOT NULL,limit_amount REAL NOT NULL,limit_start_date TEXT NOT NULL,limit_end_date TEXT NOT NULL,status TEXT DEFAULT 'ACTIVE',verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(office_type,limit_start_date));
        CREATE TABLE IF NOT EXISTS ccesm(id INTEGER PRIMARY KEY AUTOINCREMENT,category_code TEXT NOT NULL,sub_category_code TEXT NOT NULL,sol_id TEXT NOT NULL,expense_type TEXT NOT NULL,depreciation_account TEXT NOT NULL,status TEXT DEFAULT 'ACTIVE',verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(category_code,sub_category_code,sol_id));

        CREATE TABLE IF NOT EXISTS asset_request(request_id TEXT PRIMARY KEY,category TEXT NOT NULL,sub_category TEXT NOT NULL,asset_name TEXT NOT NULL,description TEXT,quantity INTEGER DEFAULT 1,purchase_value REAL,sol_id TEXT,status TEXT DEFAULT 'PENDING',created_by TEXT NOT NULL,approved_by TEXT,created_at TEXT NOT NULL,updated_at TEXT);
        CREATE TABLE IF NOT EXISTS asset_procurement(id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL,vendor_name TEXT,purchase_date TEXT,purchase_amount REAL,account_debit TEXT,account_credit TEXT,transaction_ref TEXT,verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS asset_transfer(id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL,sender_branch TEXT,receiver_branch TEXT NOT NULL,units INTEGER,total_asset_value REAL,net_asset_value REAL,transfer_date TEXT,transaction_ref TEXT,verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS asset_disposal(id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL,sale_amount REAL,disposal_type TEXT,tran_mode TEXT,disposal_amount REAL,gain_loss REAL,disposal_date TEXT,transaction_ref TEXT,verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS asset_repair(id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL,repair_amount REAL,repair_description TEXT,repair_date TEXT,transaction_ref TEXT,verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS asset_revaluation(id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL,revaluation_amount REAL,net_asset_value REAL,revaluation_date TEXT,transaction_ref TEXT,verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS asset_surrender(id INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT NOT NULL,receiver_branch TEXT NOT NULL,units INTEGER,total_asset_value REAL,net_asset_value REAL,surrender_date TEXT,transaction_ref TEXT,verified_by TEXT,created_by TEXT NOT NULL,created_at TEXT NOT NULL);
        """)

init_db()

def require_login(f):
    @wraps(f)
    def w(*a,**k):
        if "user_id" not in session: return err("Not logged in",401)
        return f(*a,**k)
    return w

def require_admin(f):
    @wraps(f)
    def w(*a,**k):
        if "user_id" not in session: return err("Not logged in",401)
        if session.get("role")!="ADMIN": return err("Access denied: Admin only",403)
        return f(*a,**k)
    return w

def require_user(f):
    @wraps(f)
    def w(*a,**k):
        if "user_id" not in session: return err("Not logged in",401)
        if session.get("role") not in ("USER","TELLER"): return err("Access denied",403)
        return f(*a,**k)
    return w

@app.route("/")
def index():
    base = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base, "index.html")

@app.route("/api/login",methods=["POST"])
def login():
    d=request.json
    with get_db() as conn:
        u=conn.execute("SELECT * FROM users WHERE username=? AND password=?",(d.get("username"),d.get("password"))).fetchone()
    if not u: return err("Invalid credentials",401)
    session["user_id"]=u["user_id"]; session["username"]=u["username"]; session["role"]=u["role"]
    return ok({"username":u["username"],"role":u["role"]})

@app.route("/api/logout",methods=["POST"])
def logout(): session.clear(); return ok(msg="Logged out")

@app.route("/api/me")
def me():
    if "user_id" not in session: return err("Not logged in",401)
    return ok({"username":session["username"],"role":session["role"]})

@app.route("/api/dashboard/stats")
@require_login
def stats():
    role=session.get("role")
    with get_db() as conn:
        if role=="ADMIN":
            return ok({"carcmd":conn.execute("SELECT COUNT(*) as c FROM carcmd WHERE status='ACTIVE'").fetchone()["c"],
                       "cdsm":conn.execute("SELECT COUNT(*) as c FROM cdsm WHERE status='ACTIVE'").fetchone()["c"],
                       "csotm":conn.execute("SELECT COUNT(*) as c FROM csotm WHERE status='ACTIVE'").fetchone()["c"],
                       "cotlm":conn.execute("SELECT COUNT(*) as c FROM cotlm WHERE status='ACTIVE'").fetchone()["c"],
                       "ccesm":conn.execute("SELECT COUNT(*) as c FROM ccesm WHERE status='ACTIVE'").fetchone()["c"]})
        else:
            return ok({"total":conn.execute("SELECT COUNT(*) as c FROM asset_request").fetchone()["c"],
                       "pending":conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='PENDING'").fetchone()["c"],
                       "approved":conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='APPROVED'").fetchone()["c"],
                       "procured":conn.execute("SELECT COUNT(*) as c FROM asset_request WHERE status='PROCURED'").fetchone()["c"]})

# ── CFAMS helpers ──
def cfams_list(table, extra=""):
    with get_db() as conn:
        rows=conn.execute(f"SELECT * FROM {table} WHERE status='ACTIVE'{extra}").fetchall()
    return ok([dict(r) for r in rows])

def cfams_verify(table, rid):
    with get_db() as conn:
        r=conn.execute(f"SELECT * FROM {table} WHERE id=?",(rid,)).fetchone()
        if not r: return err("Record not found",404)
        if r["created_by"]==session["username"]: return err("Same user cannot verify the record")
        if r["verified_by"]: return err("Record already verified")
        conn.execute(f"UPDATE {table} SET verified_by=? WHERE id=?",(session["username"],rid))
    return ok(msg="Record verified successfully")

# ── CARCMD ──
@app.route("/api/cfams/carcmd",methods=["GET"])
@require_admin
def carcmd_list(): return cfams_list("carcmd")

@app.route("/api/cfams/carcmd",methods=["POST"])
@require_admin
def carcmd_add():
    d=request.json
    if not all([d.get("reference_type"),d.get("reference_code"),d.get("category"),d.get("sub_category"),d.get("scale")]): return err("All fields are required")
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO carcmd(reference_type,reference_code,category,sub_category,scale,created_by,created_at) VALUES(?,?,?,?,?,?,?)",(d["reference_type"],d["reference_code"],d["category"],d["sub_category"],d["scale"],session["username"],now()))
        return ok(msg="Record added successfully. Pending verification.")
    except sqlite3.IntegrityError: return err("Record already exists for this reference type and reference code combination")

@app.route("/api/cfams/carcmd/<int:rid>/verify",methods=["POST"])
@require_admin
def carcmd_verify(rid): return cfams_verify("carcmd",rid)

@app.route("/api/cfams/carcmd/<int:rid>",methods=["PUT"])
@require_admin
def carcmd_modify(rid):
    d=request.json
    with get_db() as conn:
        r=conn.execute("SELECT * FROM carcmd WHERE id=?",(rid,)).fetchone()
        if not r: return err("Record not found",404)
        if r["status"]=="DELETED": return err("Record does not exist")
        conn.execute("UPDATE carcmd SET category=?,sub_category=?,scale=?,verified_by=NULL,updated_at=? WHERE id=?",(d.get("category",r["category"]),d.get("sub_category",r["sub_category"]),d.get("scale",r["scale"]),now(),rid))
    return ok(msg="Record modified. Re-verification required.")

@app.route("/api/cfams/carcmd/<int:rid>",methods=["DELETE"])
@require_admin
def carcmd_delete(rid):
    with get_db() as conn:
        r=conn.execute("SELECT * FROM carcmd WHERE id=?",(rid,)).fetchone()
        if not r: return err("Record not found",404)
        if r["status"]=="DELETED": return err("Record already deleted")
        conn.execute("UPDATE carcmd SET status='DELETED',updated_at=? WHERE id=?",(now(),rid))
    return ok(msg="Record deleted")

@app.route("/api/cfams/carcmd/<int:rid>/undelete",methods=["POST"])
@require_admin
def carcmd_undelete(rid):
    with get_db() as conn:
        r=conn.execute("SELECT * FROM carcmd WHERE id=?",(rid,)).fetchone()
        if not r: return err("Not found",404)
        if r["status"]=="ACTIVE": return err("Record already active")
        conn.execute("UPDATE carcmd SET status='ACTIVE',updated_at=? WHERE id=?",(now(),rid))
    return ok(msg="Record undeleted")

# ── CDSM ──
@app.route("/api/cfams/cdsm",methods=["GET"])
@require_admin
def cdsm_list(): return cfams_list("cdsm")

@app.route("/api/cfams/cdsm",methods=["POST"])
@require_admin
def cdsm_add():
    d=request.json
    if not all([d.get("designation"),d.get("scale")]): return err("All fields required")
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO cdsm(designation,scale,created_by,created_at) VALUES(?,?,?,?)",(d["designation"],d["scale"],session["username"],now()))
        return ok(msg="CDSM record added")
    except sqlite3.IntegrityError: return err("Designation and scale combination already exists")

@app.route("/api/cfams/cdsm/<int:rid>/verify",methods=["POST"])
@require_admin
def cdsm_verify(rid): return cfams_verify("cdsm",rid)

# ── CSOTM ──
@app.route("/api/cfams/csotm",methods=["GET"])
@require_admin
def csotm_list(): return cfams_list("csotm")

@app.route("/api/cfams/csotm",methods=["POST"])
@require_admin
def csotm_add():
    d=request.json
    if not all([d.get("sol_id"),d.get("office_type")]): return err("Sol ID and Office Type required")
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO csotm(office_type,sol_id,created_by,created_at) VALUES(?,?,?,?)",(d["office_type"],d["sol_id"],session["username"],now()))
        return ok(msg="CSOTM record added")
    except sqlite3.IntegrityError: return err("Office type for this Sol ID already exists")

@app.route("/api/cfams/csotm/<int:rid>/verify",methods=["POST"])
@require_admin
def csotm_verify(rid): return cfams_verify("csotm",rid)

# ── COTLM ──
@app.route("/api/cfams/cotlm",methods=["GET"])
@require_admin
def cotlm_list(): return cfams_list("cotlm")

@app.route("/api/cfams/cotlm",methods=["POST"])
@require_admin
def cotlm_add():
    d=request.json
    if not all([d.get("office_type"),d.get("limit_amount"),d.get("limit_start_date"),d.get("limit_end_date")]): return err("All fields required")
    if d["limit_end_date"]<d["limit_start_date"]: return err("Limit end date must always be greater than limit start date")
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO cotlm(office_type,limit_amount,limit_start_date,limit_end_date,created_by,created_at) VALUES(?,?,?,?,?,?)",(d["office_type"],d["limit_amount"],d["limit_start_date"],d["limit_end_date"],session["username"],now()))
        return ok(msg="COTLM record added")
    except sqlite3.IntegrityError: return err("Record already exists for this office type and start date")

@app.route("/api/cfams/cotlm/<int:rid>/verify",methods=["POST"])
@require_admin
def cotlm_verify(rid): return cfams_verify("cotlm",rid)

# ── CCESM ──
@app.route("/api/cfams/ccesm",methods=["GET"])
@require_admin
def ccesm_list(): return cfams_list("ccesm")

@app.route("/api/cfams/ccesm",methods=["POST"])
@require_admin
def ccesm_add():
    d=request.json
    if not all([d.get("category_code"),d.get("sub_category_code"),d.get("sol_id"),d.get("expense_type"),d.get("depreciation_account")]): return err("All fields required")
    if d["depreciation_account"] not in VALID_DEPR_ACCOUNTS: return err("Invalid Depreciation Account Number")
    try:
        with get_db() as conn:
            conn.execute("INSERT INTO ccesm(category_code,sub_category_code,sol_id,expense_type,depreciation_account,created_by,created_at) VALUES(?,?,?,?,?,?,?)",(d["category_code"],d["sub_category_code"],d["sol_id"],d["expense_type"],d["depreciation_account"],session["username"],now()))
        return ok(msg="CCESM record added")
    except sqlite3.IntegrityError: return err("Record already exists for this combination")

@app.route("/api/cfams/ccesm/<int:rid>/verify",methods=["POST"])
@require_admin
def ccesm_verify(rid): return cfams_verify("ccesm",rid)

# ══ CFAMO ══════════════════════════════════════════════════════

@app.route("/api/cfamo/requests",methods=["GET"])
@require_user
def list_requests():
    cat=request.args.get("category"); sub=request.args.get("sub_category")
    rid=request.args.get("request_id"); sts=request.args.get("status")
    q="SELECT * FROM asset_request WHERE 1=1"; p=[]
    if cat: q+=" AND category=?"; p.append(cat)
    if sub: q+=" AND sub_category=?"; p.append(sub)
    if rid: q+=" AND request_id=?"; p.append(rid)
    if sts: q+=" AND status=?"; p.append(sts)
    with get_db() as conn: rows=conn.execute(q,p).fetchall()
    return ok([dict(r) for r in rows])

@app.route("/api/cfamo/requests/<rid>",methods=["GET"])
@require_user
def get_request(rid):
    with get_db() as conn: r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
    if not r: return err("Request not found",404)
    return ok(dict(r))

# AQ
@app.route("/api/cfamo/aq",methods=["POST"])
@require_user
def aq():
    d=request.json
    if not all([d.get("category"),d.get("sub_category"),d.get("asset_name")]): return err("Category, Sub Category and Asset Name required")
    with get_db() as conn:
        if not conn.execute("SELECT id FROM carcmd WHERE category=? AND sub_category=? AND status='ACTIVE'",(d["category"],d["sub_category"])).fetchone():
            return err("Invalid category/sub-category — not found in CARCMD setup")
        rid=gen_rid(d["category"],d["sub_category"])
        conn.execute("INSERT INTO asset_request(request_id,category,sub_category,asset_name,description,quantity,purchase_value,sol_id,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (rid,d["category"],d["sub_category"],d["asset_name"],d.get("description",""),d.get("quantity",1),d.get("purchase_value"),d.get("sol_id"),session["username"],now()))
    return ok({"request_id":rid},msg=f"Asset request Id successfully created. Request ID: {rid}")

# AA
@app.route("/api/cfamo/aa",methods=["POST"])
@require_user
def aa():
    d=request.json; action=d.get("action")
    if action not in ("APPROVED","REJECTED"): return err("Action must be APPROVED or REJECTED")
    with get_db() as conn:
        r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(d.get("request_id"),)).fetchone()
        if not r: return err("Request not found",404)
        if r["created_by"]==session["username"]: return err("Same user cannot verify the record")
        if r["status"] not in ("PENDING","MODIFIED"): return err(f"Cannot action — current status: {r['status']}")
        conn.execute("UPDATE asset_request SET status=?,approved_by=?,updated_at=? WHERE request_id=?",(action,session["username"],now(),d["request_id"]))
    return ok(msg=f"Asset request Id successfully {action.lower()}")

# AM
@app.route("/api/cfamo/am",methods=["POST"])
@require_user
def am():
    d=request.json; rid=d.get("request_id")
    with get_db() as conn:
        r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
        if not r: return err("Request not found",404)
        if r["status"]=="PROCURED": return err("Asset is already purchased")
        if r["status"] not in ("PENDING","MODIFIED","APPROVED"): return err(f"Cannot modify in status: {r['status']}")
        conn.execute("UPDATE asset_request SET asset_name=?,quantity=?,purchase_value=?,status='MODIFIED',updated_at=? WHERE request_id=?",
            (d.get("asset_name",r["asset_name"]),d.get("quantity",r["quantity"]),d.get("purchase_value",r["purchase_value"]),now(),rid))
    return ok(msg="Asset request Id successfully modified")

# AD
@app.route("/api/cfamo/ad",methods=["POST"])
@require_user
def ad():
    d=request.json; rid=d.get("request_id")
    with get_db() as conn:
        r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
        if not r: return err("Request not found",404)
        if r["status"]=="PROCURED": return err("Asset already purchased. Cannot delete.")
        conn.execute("UPDATE asset_request SET status='DELETED',updated_at=? WHERE request_id=?",(now(),rid))
    return ok(msg="Asset request deleted successfully")

# AP
@app.route("/api/cfamo/ap",methods=["POST"])
@require_user
def ap():
    d=request.json; fn=d.get("function_code","A"); rid=d.get("request_id")
    if not rid: return err("Request ID required")
    if fn=="V":
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_procurement WHERE request_id=? AND verified_by IS NULL",(rid,)).fetchone()
            if not r: return err("Not found or already verified")
            if r["created_by"]==session["username"]: return err("Different user must verify")
            conn.execute("UPDATE asset_procurement SET verified_by=? WHERE id=?",(session["username"],r["id"]))
        return ok(msg="Asset request Id procurement successful")
    else:
        if not d.get("purchase_amount"): return err("Purchase amount required")
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
            if not r: return err("Request not found",404)
            if r["status"]!="APPROVED": return err(f"Asset must be APPROVED. Current: {r['status']}")
            tref=f"TXN-{uuid.uuid4().hex[:10].upper()}"
            conn.execute("INSERT INTO asset_procurement(request_id,vendor_name,purchase_date,purchase_amount,account_debit,account_credit,transaction_ref,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (rid,d.get("vendor_name"),d.get("purchase_date",today()),d["purchase_amount"],d.get("account_debit"),d.get("account_credit"),tref,session["username"],now()))
            conn.execute("UPDATE asset_request SET status='PROCURED',updated_at=? WHERE request_id=?",(now(),rid))
        return ok({"transaction_ref":tref},msg=f"Asset request Id procurement is added. TXN: {tref}")

# AT
@app.route("/api/cfamo/at",methods=["POST"])
@require_user
def at():
    d=request.json; fn=d.get("function_code","A"); rid=d.get("request_id")
    if not rid: return err("Request ID required")
    if fn=="V":
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_transfer WHERE request_id=? AND verified_by IS NULL",(rid,)).fetchone()
            if not r: return err("Not found or already verified")
            if r["created_by"]==session["username"]: return err("Different user must verify")
            conn.execute("UPDATE asset_transfer SET verified_by=? WHERE id=?",(session["username"],r["id"]))
        return ok(msg="Asset transfer successfully verified")
    else:
        if not all([d.get("receiver_branch"),d.get("units")]): return err("Receiver branch and units required")
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
            if not r: return err("Request not found",404)
            if r["status"]!="PROCURED": return err(f"Must be PROCURED. Current: {r['status']}")
            if not conn.execute("SELECT id FROM csotm WHERE sol_id=? AND status='ACTIVE'",(d["receiver_branch"],)).fetchone():
                return err("Receiver branch is invalid")
            if not conn.execute("SELECT id FROM ccesm WHERE category_code=? AND sub_category_code=? AND sol_id=? AND status='ACTIVE'",(r["category"],r["sub_category"],d["receiver_branch"])).fetchone():
                return err("Receiver branch, category and sub-category combination not set up in CCSESM")
            tv=(r["purchase_value"] or 0)*int(d["units"]); nv=tv*0.85
            tref=f"TXN-{uuid.uuid4().hex[:10].upper()}"
            conn.execute("INSERT INTO asset_transfer(request_id,sender_branch,receiver_branch,units,total_asset_value,net_asset_value,transfer_date,transaction_ref,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (rid,r["sol_id"] or "HO",d["receiver_branch"],d["units"],tv,nv,d.get("transfer_date",today()),tref,session["username"],now()))
            conn.execute("UPDATE asset_request SET status='TRANSFERRED',updated_at=? WHERE request_id=?",(now(),rid))
        return ok({"transaction_ref":tref,"total_asset_value":tv,"net_asset_value":nv},msg="Asset transfer added successfully")

# AW
@app.route("/api/cfamo/aw",methods=["POST"])
@require_user
def aw():
    d=request.json; fn=d.get("function_code","A"); rid=d.get("request_id")
    if not rid: return err("Request ID required")
    if fn=="V":
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_disposal WHERE request_id=? AND verified_by IS NULL",(rid,)).fetchone()
            if not r: return err("Not found or already verified")
            if r["created_by"]==session["username"]: return err("Different user must verify")
            if r["tran_mode"]=="C-Cash" and session["role"]!="TELLER": return err("Only Teller can verify the record in Cash Mode")
            conn.execute("UPDATE asset_disposal SET verified_by=? WHERE id=?",(session["username"],r["id"]))
        return ok(msg="Disposal verified successfully")
    else:
        if not all([d.get("sale_amount"),d.get("disposal_type"),d.get("tran_mode"),d.get("disposal_amount")]): return err("All fields required")
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
            if not r: return err("Request not found",404)
            if r["status"]!="PROCURED": return err(f"Must be PROCURED. Current: {r['status']}")
            gl=float(d["sale_amount"])-float(d["disposal_amount"])
            tref=f"TXN-{uuid.uuid4().hex[:10].upper()}"
            conn.execute("INSERT INTO asset_disposal(request_id,sale_amount,disposal_type,tran_mode,disposal_amount,gain_loss,disposal_date,transaction_ref,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (rid,d["sale_amount"],d["disposal_type"],d["tran_mode"],d["disposal_amount"],gl,d.get("disposal_date",today()),tref,session["username"],now()))
            conn.execute("UPDATE asset_request SET status='DISPOSED',updated_at=? WHERE request_id=?",(now(),rid))
        return ok({"transaction_ref":tref,"gain_loss":gl},msg="Disposal details added successfully")

# AR
@app.route("/api/cfamo/ar",methods=["POST"])
@require_user
def ar():
    d=request.json; fn=d.get("function_code","A"); rid=d.get("request_id")
    if not rid: return err("Request ID required")
    if fn=="V":
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_repair WHERE request_id=? AND verified_by IS NULL",(rid,)).fetchone()
            if not r: return err("Not found or already verified")
            if r["created_by"]==session["username"]: return err("Different user must verify")
            conn.execute("UPDATE asset_repair SET verified_by=? WHERE id=?",(session["username"],r["id"]))
        return ok(msg="Repair details verified successfully")
    else:
        if not d.get("repair_amount"): return err("Repair amount required")
        with get_db() as conn:
            if not conn.execute("SELECT id FROM asset_request WHERE request_id=?",(rid,)).fetchone(): return err("Request not found",404)
            tref=f"TXN-{uuid.uuid4().hex[:10].upper()}"
            conn.execute("INSERT INTO asset_repair(request_id,repair_amount,repair_description,repair_date,transaction_ref,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (rid,d["repair_amount"],d.get("repair_description",""),d.get("repair_date",today()),tref,session["username"],now()))
        return ok({"transaction_ref":tref},msg="Repair details added successfully")

# AL
@app.route("/api/cfamo/al",methods=["POST"])
@require_user
def al():
    d=request.json; fn=d.get("function_code","A"); rid=d.get("request_id")
    if not rid: return err("Request ID required")
    if fn=="V":
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_revaluation WHERE request_id=? AND verified_by IS NULL",(rid,)).fetchone()
            if not r: return err("Not found or already verified")
            if r["created_by"]==session["username"]: return err("Different user must verify")
            conn.execute("UPDATE asset_revaluation SET verified_by=? WHERE id=?",(session["username"],r["id"]))
        return ok(msg="Revaluation details verified successfully")
    else:
        if not all([d.get("revaluation_amount"),d.get("net_asset_value")]): return err("All fields required")
        if float(d["revaluation_amount"])<float(d["net_asset_value"]): return err("Revaluation amount should be greater than current net asset value")
        with get_db() as conn:
            if not conn.execute("SELECT id FROM asset_request WHERE request_id=?",(rid,)).fetchone(): return err("Request not found",404)
            tref=f"TXN-{uuid.uuid4().hex[:10].upper()}"
            conn.execute("INSERT INTO asset_revaluation(request_id,revaluation_amount,net_asset_value,revaluation_date,transaction_ref,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (rid,d["revaluation_amount"],d["net_asset_value"],d.get("revaluation_date",today()),tref,session["username"],now()))
        return ok({"transaction_ref":tref},msg="Revaluation details added successfully")

# AS
@app.route("/api/cfamo/as",methods=["POST"])
@require_user
def as_op():
    d=request.json; fn=d.get("function_code","A"); rid=d.get("request_id")
    if not rid: return err("Request ID required")
    if fn=="V":
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_surrender WHERE request_id=? AND verified_by IS NULL",(rid,)).fetchone()
            if not r: return err("Not found or already verified")
            if r["created_by"]==session["username"]: return err("Different user must verify")
            conn.execute("UPDATE asset_surrender SET verified_by=? WHERE id=?",(session["username"],r["id"]))
        return ok(msg="Asset surrender successfully verified")
    else:
        if not all([d.get("receiver_branch"),d.get("units")]): return err("Receiver branch and units required")
        with get_db() as conn:
            r=conn.execute("SELECT * FROM asset_request WHERE request_id=?",(rid,)).fetchone()
            if not r: return err("Request not found",404)
            if r["status"]!="PROCURED": return err(f"Must be PROCURED. Current: {r['status']}")
            tv=(r["purchase_value"] or 0)*int(d["units"]); nv=tv*0.80
            tref=f"TXN-{uuid.uuid4().hex[:10].upper()}"
            conn.execute("INSERT INTO asset_surrender(request_id,receiver_branch,units,total_asset_value,net_asset_value,surrender_date,transaction_ref,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (rid,d["receiver_branch"],d["units"],tv,nv,d.get("surrender_date",today()),tref,session["username"],now()))
            conn.execute("UPDATE asset_request SET status='SURRENDERED',updated_at=? WHERE request_id=?",(now(),rid))
        return ok({"transaction_ref":tref,"total_asset_value":tv,"net_asset_value":nv},msg="Asset surrender added successfully")

if __name__=="__main__":
    import webbrowser,threading
    port=int(os.environ.get("PORT",5000))
    def ob():
        import time; time.sleep(1); webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=ob,daemon=True).start()
    print(f"\n{'='*50}\n  FAMS at: http://localhost:{port}\n{'='*50}\n")
    app.run(debug=False,port=port,use_reloader=False)
