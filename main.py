"""
main.py — PRISMA · TA-ex System
FastAPI backend menggantikan Node.js/Express
Semua endpoint kompatibel 1:1 dengan frontend index.html asli

Jalankan: uvicorn main:app --reload --port 8080
"""
import io
import json
import os
import time
import uuid
import hashlib
import threading
from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from database import migrate, query, query_one, execute, get_state, set_state
from bulk_ops import (
    bulk_replace_taex, bulk_replace_prisma, bulk_replace_pr,
    bulk_replace_po, bulk_replace_kumpulan, bulk_replace_order,
    bulk_replace_project, bulk_replace_job_list,
    bulk_replace_job_detail, bulk_replace_job_detail_work_order,
    bulk_replace_equipment_taex,
    bulk_replace_job_area, bulk_replace_job_unit,
    bulk_replace_vw_joblist_wo, bulk_replace_vw_joblist_detail,
)
from header_maps import normalize_taex, normalize_sap, normalize_order

load_dotenv()

# ─── APP ────────────────────────────────────────────────────────
app = FastAPI(title="PRISMA · TA-ex System", version="2.0.0")

API_KEY        = os.getenv("API_KEY", "")
PUBLIC_API_KEY = os.getenv("PUBLIC_API_KEY", "")
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "*")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "x-api-key"],
)

# ─── DB MIGRATE ON STARTUP ──────────────────────────────────────
@app.on_event("startup")
def startup():
    migrate()
    print("🚀 PRISMA TA-ex FastAPI started")


# ─── JSON ENCODER (handle Decimal, date) ───────────────────────
class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, (datetime, date)): return str(obj)
        return super().default(obj)

def jsonify(data: Any) -> JSONResponse:
    return JSONResponse(content=json.loads(json.dumps(data, cls=_Encoder)))


# ─── AUTH MIDDLEWARE ────────────────────────────────────────────
def check_api_key(request: Request):
    if not API_KEY:
        return
    key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: API key tidak valid")

def check_public_api_key(request: Request):
    """Validator untuk public API — terima API_KEY atau PUBLIC_API_KEY."""
    key = (request.headers.get("x-api-key") or
           request.headers.get("Authorization","").replace("Bearer ","") or
           request.query_params.get("api_key") or "")
    if not key:
        raise HTTPException(401, "API key diperlukan. Sertakan header 'x-api-key' atau query param 'api_key'")
    if key not in (API_KEY, PUBLIC_API_KEY):
        raise HTTPException(403, "API key tidak valid")


# ─── USER AUTH ───────────────────────────────────────────────────
def _hash_password(password: str) -> str:
    import os
    salt = os.urandom(16).hex()
    h    = hashlib.sha256(f"{password}{salt}".encode()).hexdigest()
    return f"{h}:{salt}"

def _verify_password(password: str, stored: str) -> bool:
    try:
        h, salt = stored.split(":")
        return hashlib.sha256(f"{password}{salt}".encode()).hexdigest() == h
    except Exception:
        return False

def _create_session(user_id: int) -> str:
    token = uuid.uuid4().hex + uuid.uuid4().hex
    expires = datetime.utcnow() + timedelta(days=7)
    execute(
        "INSERT INTO user_sessions (token, user_id, expires_at) VALUES (%s, %s, %s)",
        (token, user_id, expires)
    )
    return token

def get_current_user(request: Request) -> dict:
    """Ambil user dari token. Raise 401 jika tidak valid."""
    token = (request.headers.get("x-auth-token") or
             request.query_params.get("token") or "")
    if not token:
        raise HTTPException(401, "Login diperlukan")
    row = query_one("""
        SELECT u.id, u.username, u.plant_code, u.pg_role, u.is_admin, u.is_active
        FROM user_sessions s
        JOIN users u ON s.user_id = u.id
        WHERE s.token = %s AND s.expires_at > NOW()
    """, (token,))
    if not row:
        raise HTTPException(401, "Sesi tidak valid atau sudah habis")
    if not row["is_active"]:
        raise HTTPException(403, "Akun dinonaktifkan")
    return dict(row)

def require_admin(request: Request) -> dict:
    user = get_current_user(request)
    if not user["is_admin"]:
        raise HTTPException(403, "Akses admin diperlukan")
    return user

# ── PG Filter mapping ─────────────────────────────────────────
PG_SUFFIX = { "TA": "T", "OH": "O", "Rutin": "R", "Inventory": None }

def plant_clause(user: dict, col: str = "plant") -> tuple:
    """Return (sql_clause, params) untuk filter plant."""
    if user["is_admin"] or not user["plant_code"]:
        return ("1=1", [])
    return (f"{col} = %s", [user["plant_code"]])

def pg_clause(user: dict, col: str = "pg") -> tuple:
    """Return (sql_clause, params) untuk filter PG."""
    if user["is_admin"]:
        return ("1=1", [])
    suffix = PG_SUFFIX.get(user["pg_role"])
    if not suffix:
        return ("1=1", [])
    return (f"{col} LIKE %s", [f"%{suffix}"])

def apply_filters(user: dict, base_clauses: list, base_params: list,
                  plant_col: str = "plant", pg_col: str = None) -> tuple:
    """Gabungkan base clauses dengan filter plant+pg user."""
    clauses = list(base_clauses)
    params  = list(base_params)
    pc, pp = plant_clause(user, plant_col)
    clauses.append(pc); params.extend(pp)
    if pg_col:
        gc, gp = pg_clause(user, pg_col)
        clauses.append(gc); params.extend(gp)
    return clauses, params

# ── Kertas kerja prefix ───────────────────────────────────────
KK_PREFIX = { "TA": "TA", "OH": "OH", "Rutin": "RT", "Admin": "AD", "Inventory": "IV" }


_jobs: dict = {}
_jobs_lock = threading.Lock()

def set_job(job_id: str, pct: int, msg: str, done: bool = False, error: str = None):
    with _jobs_lock:
        _jobs[job_id] = {"pct": pct, "msg": msg, "done": done, "error": error, "ts": time.time()}

def cleanup_jobs():
    cutoff = time.time() - 600
    with _jobs_lock:
        stale = [k for k, v in _jobs.items() if v["ts"] < cutoff]
        for k in stale:
            del _jobs[k]


# ─── ROW MAPPERS ────────────────────────────────────────────────
def _n(v):
    if v is None: return None
    try: return float(v)
    except: return None

def map_taex(r):
    return {
        "ID": r["id"], "Plant": r["plant"], "Equipment": r["equipment"],
        "Order": r["order"], "Revision": r["revision"], "Reservno": r["reservno"],
        "Material": r["material"], "Itm": r["itm"],
        "Material_Description": r["material_description"],
        "Qty_Reqmts": _n(r["qty_reqmts"]), "Qty_Stock": _n(r["qty_stock"]),
        "PR": r["pr"], "Item": r["item"], "Qty_PR": _n(r["qty_pr"]),
        "Cost_Ctrs": r["cost_ctrs"],
        "PO": r["po"], "PO_Date": r["po_date"], "Qty_Deliv": _n(r["qty_deliv"]),
        "Delivery_Date": r["delivery_date"],
        "SLoc": r["sloc"], "Del": r["del"], "FIs": r["fis"],
        "Ict": r["ict"], "PG": r["pg"],
        "Recipient": r["recipient"], "Unloading_point": r["unloading_point"],
        "Reqmts_Date": r["reqmts_date"],
        "Qty_f_avail_check": _n(r["qty_f_avail_check"]),
        "Qty_Withdrawn": _n(r["qty_withdrawn"]),
        "UoM": r["uom"], "GL_Acct": r["gl_acct"],
        "Res_Price": _n(r["res_price"]), "Res_per": _n(r["res_per"]),
        "Res_Curr": r["res_curr"],
    }

def map_prisma(r):
    return {
        "ID": r["id"], "Plant": r["plant"], "Equipment": r["equipment"],
        "Revision": r["revision"], "Order": r["order"], "Reservno": r["reservno"],
        "Itm": r["itm"], "Material": r["material"],
        "Material_Description": r["material_description"],
        "Del": r["del"], "FIs": r["fis"], "Ict": r["ict"], "PG": r["pg"],
        "Recipient": r["recipient"], "Unloading_point": r["unloading_point"],
        "Reqmts_Date": r["reqmts_date"],
        "Qty_Reqmts": _n(r["qty_reqmts"]), "UoM": r["uom"],
        "PR_Prisma": r["pr_prisma"], "Item_Prisma": r["item_prisma"],
        "Qty_PR_Prisma": _n(r["qty_pr_prisma"]),
        "Qty_StockOnhand": _n(r["qty_stock_onhand"]),
        "CodeKertasKerja": r["code_kertas_kerja"],
    }

def map_kumpulan(r):
    return {
        "ID": r["id"], "Plant": r["plant"], "Equipment": r["equipment"],
        "Revision": r["revision"], "Order": r["order"], "Reservno": r["reservno"],
        "Itm": r["itm"], "Material": r["material"],
        "Material_Description": r["material_description"],
        "Qty_Req": _n(r["qty_req"]), "Qty_Stock": _n(r["qty_stock"]),
        "Qty_PR": _n(r["qty_pr"]), "Qty_To_PR": _n(r["qty_to_pr"]),
        "CodeTracking": r["code_tracking"],
    }

def map_sap(r):
    return {
        "ID": r["id"], "Plant": r["plant"],
        "PR": r["pr"], "Item": r["item"],
        "Material": r["material"], "Material_Description": r["material_description"],
        "D": r["d"], "R": r["r"], "PGr": r["pgr"], "S": r["s"],
        "TrackingNo": r["tracking_no"],
        "Qty_PR": _n(r["qty_pr"]), "Un": r["un"], "Req_Date": r["req_date"],
        "Valn_price": _n(r["valn_price"]), "PR_Curr": r["pr_curr"],
        "PR_Per": _n(r["pr_per"]), "Release_Date": r["release_date"],
        "Tracking": r["tracking"],
    }

def map_po(r):
    return {
        "ID": r["id"], "Plnt": r["plnt"],
        "Purchreq": r["purchreq"], "Item": r["item"],
        "Material": r["material"], "Short_Text": r["short_text"],
        "PO": r["po"], "PO_Item": r["po_item"],
        "D": r["d"], "DCI": r["dci"], "PGr": r["pgr"],
        "Doc_Date": r["doc_date"],
        "PO_Quantity": _n(r["po_quantity"]), "Qty_Delivered": _n(r["qty_delivered"]),
        "Deliv_Date": r["deliv_date"], "OUn": r["oun"],
        "Net_Price": _n(r["net_price"]), "Crcy": r["crcy"], "Per": _n(r["per"]),
    }

def map_order(r):
    return {
        "ID": r["id"], "Plant": r["plant"], "Order": r["order"],
        "Superior_Order": r["superior_order"], "Notification": r["notification"],
        "Created_On": r["created_on"], "Description": r["description"],
        "Revision": r["revision"], "Equipment": r["equipment"],
        "System_Status": r["system_status"], "User_Status": r["user_status"],
        "FunctLocation": r["funct_location"], "Location": r["location"],
        "WBS_Ord_header": r["wbs_ord_header"], "CostCenter": r["cost_center"],
        "Total_Plan_Cost": _n(r["total_plan_cost"]),
        "Total_Act_Cost": _n(r["total_act_cost"]),
        "Planner_Group": r["planner_group"], "MainWorkCtr": r["main_work_ctr"],
        "Entry_by": r["entry_by"], "Changed_by": r["changed_by"],
        "Basic_start_date": r["basic_start_date"],
        "Basic_finish_date": r["basic_finish_date"],
        "Actual_Release": r["actual_release"],
    }


def map_vw_joblist_wo(r):
    return {
        "ID": r["id"], "Plant": r["plant"], "EquipmentNo": r["equipment_no"],
        "Disiplin": r["disiplin"], "JoblistDescription": r["joblist_description"],
        "PlanningJasaStatus": r["planning_jasa_status"],
        "PlanningMaterialStatus": r["planning_material_status"],
        "CodeName": r["code_name"], "IsLLDII": r["is_lldii"],
        "Order": r["order"], "Notification": r["notification"],
        "CreatedOn": r["created_on"], "SuperiorOrder": r["superior_order"],
        "Description": r["description"], "FunctionalLoc": r["functional_loc"],
        "Location": r["location"], "Revision": r["revision"],
        "SystemStatus": r["system_status"], "UserStatus": r["user_status"],
        "WBSordheader": r["wbs_ord_header"],
        "TotalPlnndCosts": _n(r["total_plnnd_costs"]),
        "Totalactcosts": _n(r["totalact_costs"]),
        "PlannerGroup": r["planner_group"], "MainWorkCtr": r["main_work_ctr"],
        "ChangeBy": r["change_by"], "Basstartdate": r["bas_start_date"],
        "Basicfindate": r["basic_fin_date"], "ActualRelease": r["actual_release"],
        "CostCenter": r["cost_center"], "EnteredBy": r["entered_by"],
    }


def map_vw_joblist_detail(r):
    return {
        "Id": r["id"], "JoblistId": r["joblist_id"],
        "JoblistDetailDesc": r["joblist_detail_desc"],
        "ReasonName": r["reason_name"], "DocTypeName": r["doc_type_name"],
        "NoDocument": r["no_document"],
        "IsMechanicalIntegrity": r["is_mechanical_integrity"],
        "JobDisciplineName": r["job_discipline_name"],
        "NomorPM": r["nomor_pm"], "Notes": r["notes"],
        "Plant": r["plant"], "Created": r["created"],
        "CreatorName": r["creator_name"], "CreatorJobTitle": r["creator_job_title"],
        "IsDeleted": r["is_deleted"],
        "JoblistDescription": r["joblist_description"], "NoJoblist": r["no_joblist"],
        "ProjectNumber": r["project_number"],
        "ProjectTypeCode": r["project_type_code"], "ProjectTypeName": r["project_type_name"],
        "StartDate": r["start_date"], "FinishDate": r["finish_date"],
        "Revision": r["revision"], "Description": r["description"],
        "ProjectStatus": r["project_status"], "EquipmentNo": r["equipment_no"],
        "AreaName": r["area_name"], "AreaAliasName": r["area_alias_name"],
        "UnitName": r["unit_name"], "UnitAliasName": r["unit_alias_name"],
        "FunctionalLocation": r["functional_location"], "Location": r["location"],
        "Disiplin": r["disiplin"], "Criticallity": r["criticallity"],
        "CriticallityText": r["criticallity_text"], "MainWorkCenter": r["main_work_center"],
        "IsAllIn": r["is_all_in"], "IsJasa": r["is_jasa"],
        "IsLLDII": r["is_lldii"], "IsMaterial": r["is_material"],
        "CodeName": r["code_name"],
        "PlanningJasaStatus": r["planning_jasa_status"],
        "PlanningMaterialStatus": r["planning_material_status"],
        "LLDIStatus": r["lldi_status"], "IsFreezing": r["is_freezing"],
    }


# ═══════════════════════════════════════════════════════════════
# STATIC FILES
# ═══════════════════════════════════════════════════════════════
app.mount("/static", StaticFiles(directory="public"), name="static")

@app.get("/")
def serve_index():
    return FileResponse("public/index.html")


# ═══════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════
@app.get("/api/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok", "db": "postgresql", "time": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# META — lightweight init (hanya COUNT + state)
# ═══════════════════════════════════════════════════════════════
@app.get("/api/meta")
def meta(request: Request):
    check_api_key(request)
    counts = query("""
        SELECT
            (SELECT COUNT(*) FROM taex_reservasi)    AS taex,
            (SELECT COUNT(*) FROM prisma_reservasi)  AS prisma,
            (SELECT COUNT(*) FROM kumpulan_summary)  AS kumpulan,
            (SELECT COUNT(*) FROM sap_pr)            AS pr,
            (SELECT COUNT(*) FROM sap_po)            AS po
    """)[0]
    kk      = get_state("kk_current")
    summary = get_state("summary_current")
    kk_ctr  = get_state("kk_counter")
    pr_ctr  = get_state("pr_counter")
    return jsonify({
        "kkData":      kk["data"] if kk else [],
        "kkCode":      kk["code"] if kk else None,
        "summaryData": summary or [],
        "kkCounter":   kk_ctr or 0,
        "prCounter":   pr_ctr or 0,
        "pagination": {
            "totalTaex":     int(counts["taex"]),
            "totalPrisma":   int(counts["prisma"]),
            "totalKumpulan": int(counts["kumpulan"]),
            "totalPR":       int(counts["pr"]),
            "totalPO":       int(counts["po"]),
        },
    })


# ═══════════════════════════════════════════════════════════════
# DATA — paginated per tabel
# ═══════════════════════════════════════════════════════════════
TABLE_CONFIG = {
    "taex": {
        "table": "taex_reservasi", "mapper": map_taex,
        "search_cols": ['material','material_description','"order"','equipment','pr','po','plant','itm','reservno','cost_ctrs'],
        "sortable": {'id','plant','equipment','"order"','revision','material','itm','qty_reqmts','qty_stock','pr','item','qty_pr','reservno','res_price'},
        "filters": {
            "pr": lambda v: ("pr = %s", v) if v else None,
            "po": lambda v: ("po IS NOT NULL AND po <> ''", None) if v=="with" else
                            ("(po IS NULL OR po = '')", None) if v=="without" else None,
        },
    },
    "prisma": {
        "table": "prisma_reservasi", "mapper": map_prisma,
        "search_cols": ['material','material_description','"order"','equipment','plant','reservno','pr_prisma'],
        "sortable": {'id','plant','equipment','"order"','material','qty_reqmts','pr_prisma','code_kertas_kerja'},
        "filters": {
            "order": lambda v: ('"order" = %s', v) if v else None,
        },
    },
    "kumpulan": {
        "table": "kumpulan_summary", "mapper": map_kumpulan,
        "search_cols": ['material','material_description','"order"','equipment','code_tracking'],
        "sortable": {'id','plant','"order"','material','qty_req','qty_stock','code_tracking'},
        "filters": {
            "code_tracking": lambda v: ("code_tracking = %s", v) if v else None,
        },
    },
    "pr": {
        "table": "sap_pr", "mapper": map_sap,
        "search_cols": ['pr','material','material_description','plant','tracking','tracking_no'],
        "sortable": {'id','plant','pr','material','qty_pr','req_date','release_date'},
        "filters": {},
    },
    "po": {
        "table": "sap_po", "mapper": map_po,
        "search_cols": ['po','purchreq','material','short_text','plnt'],
        "sortable": {'id','plnt','po','purchreq','material','po_quantity','deliv_date','doc_date'},
        "filters": {},
    },
}

@app.get("/api/data/{tabel}")
def get_data_table(tabel: str, request: Request,
                   page: int = 1, limit: int = 100,
                   q: str = "", order_by: str = "id", order_dir: str = "ASC"):
    check_api_key(request)
    cfg = TABLE_CONFIG.get(tabel)
    if not cfg:
        raise HTTPException(404, "Tabel tidak ditemukan")

    limit  = min(5000, max(1, limit))
    page   = max(1, page)
    offset = (page - 1) * limit

    conds, params = [], []
    if q:
        conds.append(f"({' OR '.join(f'{c}::text ILIKE %s' for c in cfg['search_cols'])})")
        params.extend([f"%{q}%"] * len(cfg["search_cols"]))

    for key, build in cfg["filters"].items():
        val = request.query_params.get(key)
        if not val: continue
        result = build(val)
        if not result: continue
        col_expr, col_val = result
        if col_val is not None:
            conds.append(f"{col_expr}")
            params.append(col_val)
        else:
            conds.append(col_expr)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    safe_ob  = order_by if order_by in cfg["sortable"] else "id"
    safe_dir = "DESC" if order_dir.upper() == "DESC" else "ASC"

    rows  = query(f"SELECT * FROM {cfg['table']} {where} ORDER BY {safe_ob} {safe_dir} LIMIT %s OFFSET %s",
                  params + [limit, offset])
    total = query(f"SELECT COUNT(*) AS c FROM {cfg['table']} {where}", params)[0]["c"]

    return jsonify({
        "data": [cfg["mapper"](r) for r in rows],
        "pagination": {
            "page": page, "limit": limit, "total": int(total),
            "totalPages": max(1, -(-int(total) // limit)),
            "hasMore": offset + limit < int(total),
        },
    })


# ═══════════════════════════════════════════════════════════════
# UPLOAD — server-side parse Excel, background job
# ═══════════════════════════════════════════════════════════════
@app.get("/api/upload-progress/{job_id}")
def upload_progress(job_id: str, request: Request):
    check_api_key(request)
    cleanup_jobs()
    job = _jobs.get(job_id)
    if not job:
        return {"pct": 0, "msg": "Menunggu...", "done": False}
    return jsonify(job)


@app.post("/api/upload/{upload_type}")
async def upload_excel(upload_type: str, request: Request,
                       file: UploadFile = File(...),
                       mode: Optional[str] = Form(None)):
    check_api_key(request)
    if upload_type not in ("taex","prisma","pr","po","project","joblist","jobdetail",
                           "jobdetailworkorder","equipment","jobarea","jobunit",
                           "vwjoblistwo","vwjoblistdetail"):
        raise HTTPException(400, "Type tidak valid")

    content = await file.read()
    job_id = f"{upload_type}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    set_job(job_id, 0, "Membaca file Excel...")

    def _bg():
        try:
            set_job(job_id, 5, "Parsing Excel...")
            fname = file.filename.lower()
            buf = io.BytesIO(content)
            if fname.endswith(".csv"):
                df = pd.read_csv(buf, dtype=str, keep_default_na=False)
            else:
                df = pd.read_excel(buf, dtype=str, keep_default_na=False)

            if df.empty:
                set_job(job_id, 100, "File kosong", True, "File Excel kosong")
                return

            total = len(df)
            set_job(job_id, 10, f"Parsed {total:,} baris. Menyimpan ke database...")

            if upload_type == "taex":
                _mode = mode if mode in ("append","replace") else "replace"
                cnt = bulk_replace_taex(df, mode=_mode)
            elif upload_type == "prisma":
                cnt = bulk_replace_prisma(df)
            elif upload_type == "pr":
                cnt = bulk_replace_pr(df)
            elif upload_type == "po":
                cnt = bulk_replace_po(df)
            elif upload_type == "project":
                cnt = bulk_replace_project(df)
            elif upload_type == "joblist":
                cnt = bulk_replace_job_list(df)
            elif upload_type == "jobdetail":
                cnt = bulk_replace_job_detail(df)
            elif upload_type == "jobdetailworkorder":
                cnt = bulk_replace_job_detail_work_order(df)
            elif upload_type == "equipment":
                cnt = bulk_replace_equipment_taex(df)
            elif upload_type == "jobarea":
                cnt = bulk_replace_job_area(df)
            elif upload_type == "jobunit":
                cnt = bulk_replace_job_unit(df)
            elif upload_type == "vwjoblistwo":
                cnt = bulk_replace_vw_joblist_wo(df)
            elif upload_type == "vwjoblistdetail":
                cnt = bulk_replace_vw_joblist_detail(df)
            else:
                cnt = 0

            set_job(job_id, 100, f"✅ Selesai! {cnt:,} baris tersimpan", True)

        except Exception as e:
            set_job(job_id, 100, f"❌ {e}", True, str(e))

    t = threading.Thread(target=_bg, daemon=True)
    t.start()
    return {"jobId": job_id}


# ═══════════════════════════════════════════════════════════════
# TAEX
# ═══════════════════════════════════════════════════════════════
@app.get("/api/taex")
def get_taex(request: Request):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["1=1"], []
    pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    gc, gp = pg_clause(user, "pg");       clauses.append(gc); params.extend(gp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM taex_reservasi WHERE {where} ORDER BY id", params)
    return jsonify([map_taex(r) for r in rows])

@app.post("/api/taex")
async def add_taex(request: Request):
    check_api_key(request)
    r = await request.json()
    res = query(
        """INSERT INTO taex_reservasi
           (plant,equipment,"order",revision,material,itm,material_description,
            qty_reqmts,qty_stock,pr,item,qty_pr,cost_ctrs,po,po_date,qty_deliv,
            delivery_date,sloc,del,fis,ict,pg,recipient,unloading_point,reqmts_date,
            qty_f_avail_check,qty_withdrawn,uom,gl_acct,res_price,res_per,res_curr,reservno)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           RETURNING id""",
        (r.get("Plant"),r.get("Equipment"),r.get("Order"),r.get("Revision"),
         r.get("Material"),r.get("Itm"),r.get("Material_Description"),
         r.get("Qty_Reqmts",0),r.get("Qty_Stock",0),
         r.get("PR"),r.get("Item"),r.get("Qty_PR"),r.get("Cost_Ctrs"),
         r.get("PO"),r.get("PO_Date"),r.get("Qty_Deliv"),r.get("Delivery_Date"),
         r.get("SLoc"),r.get("Del"),r.get("FIs"),r.get("Ict"),r.get("PG"),
         r.get("Recipient"),r.get("Unloading_point"),r.get("Reqmts_Date"),
         r.get("Qty_f_avail_check"),r.get("Qty_Withdrawn"),
         r.get("UoM"),r.get("GL_Acct"),r.get("Res_Price"),r.get("Res_per"),
         r.get("Res_Curr"),r.get("Reservno"))
    )
    return {"ok": True, "id": res[0]["id"]}

@app.put("/api/taex")
async def put_taex(request: Request):
    check_api_key(request)
    rows = await request.json()
    if not isinstance(rows, list):
        raise HTTPException(400, "Body harus array")
    df = pd.DataFrame(rows)
    # rename keys back to Excel-style for bulk_replace_taex
    df = df.rename(columns={v:k for k,v in {
        "plant":"Plant","equipment":"Equipment","order":"Order","revision":"Revision",
        "material":"Material","itm":"Itm","material_description":"Material_Description",
        "qty_reqmts":"Qty_Reqmts","qty_stock":"Qty_Stock","pr":"PR","item":"Item",
        "qty_pr":"Qty_PR","cost_ctrs":"Cost_Ctrs",
    }.items()})
    bulk_replace_taex(df, mode="replace")
    return {"ok": True}

@app.post("/api/taex/replace")
async def replace_taex(request: Request):
    check_api_key(request)
    rows = await request.json()
    df = pd.DataFrame(rows)
    cnt = bulk_replace_taex(df, mode="replace")
    return {"ok": True, "count": cnt}

@app.post("/api/taex/append")
async def append_taex(request: Request):
    check_api_key(request)
    rows = await request.json()
    df = pd.DataFrame(rows)
    cnt = bulk_replace_taex(df, mode="append")
    return {"ok": True, "count": cnt}

@app.delete("/api/taex/{row_id}")
def delete_taex(row_id: int, request: Request):
    check_api_key(request)
    execute("DELETE FROM taex_reservasi WHERE id=%s", (row_id,))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# PRISMA
# ═══════════════════════════════════════════════════════════════
@app.get("/api/prisma")
def get_prisma(request: Request):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["1=1"], []
    pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    gc, gp = pg_clause(user, "pg");       clauses.append(gc); params.extend(gp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM prisma_reservasi WHERE {where} ORDER BY id", params)
    return jsonify([map_prisma(r) for r in rows])

@app.get("/api/prisma/meta")
def prisma_meta(request: Request):
    check_api_key(request)
    total = query('SELECT COUNT(DISTINCT "order") AS c FROM prisma_reservasi')[0]["c"]
    # Jika terlalu banyak unique order, jangan return semua (akan freeze browser)
    if int(total) > 500:
        orders = []  # filter order via text search, bukan dropdown
    else:
        orders = query('SELECT DISTINCT "order" FROM prisma_reservasi WHERE "order" IS NOT NULL ORDER BY "order"')
        orders = [r["order"] for r in orders]
    pgs = query('SELECT DISTINCT pg FROM prisma_reservasi WHERE pg IS NOT NULL ORDER BY pg')
    return {"orders": orders, "total_orders": int(total), "pgs": [r["pg"] for r in pgs]}


# ─── GET WO list untuk modal Kertas Kerja ────────────────────
PG_SUFFIX_MAP = {"TA": "T", "OH": "O", "Rutin": "R"}

@app.get("/api/prisma/workorders")
def prisma_workorders(request: Request, pg: str = "All"):
    """
    Return distinct WO yang tersedia untuk Kertas Kerja:
    - Del≠X, FIs≠X, qty_reqmts>0
    - Belum punya CodeKertasKerja (belum dipakai di KK manapun)
    - Filter by planner group (pg=TA/OH/Rutin/All)
    """
    check_api_key(request)
    user = get_current_user(request)

    conds = [
        "UPPER(COALESCE(del,'')) != 'X'",
        "UPPER(COALESCE(fis,'')) != 'X'",
        "COALESCE(qty_reqmts,0) > 0",
        "(code_kertas_kerja IS NULL OR code_kertas_kerja = '')",
    ]
    params = []

    pc, pp = plant_clause(user, "plant"); conds.append(pc); params.extend(pp)

    suffix = PG_SUFFIX_MAP.get(pg)
    if suffix:
        conds.append("pg LIKE %s"); params.append(f"%{suffix}")

    where = " AND ".join(conds)

    rows = query(f"""
        SELECT "order",
               COUNT(*)                AS total_mat,
               COUNT(DISTINCT material) AS uniq_mat,
               SUM(qty_reqmts)         AS total_qty
        FROM prisma_reservasi
        WHERE {where}
        GROUP BY "order"
        ORDER BY "order"
    """, params)

    return jsonify([{
        "order":     r["order"],
        "total_mat": int(r["total_mat"]),
        "uniq_mat":  int(r["uniq_mat"]),
        "total_qty": float(r["total_qty"] or 0),
    } for r in rows])


# ─── CREATE Kertas Kerja server-side ─────────────────────────
@app.post("/api/kertas-kerja/create")
async def create_kertas_kerja(request: Request):
    """
    Buat Kertas Kerja dari WO terpilih:
    - Ambil baris PRISMA untuk WO tersebut (Del≠X, FIs≠X)
    - Simpan ke app_state kk_current
    """
    check_api_key(request)
    user = get_current_user(request)
    body = await request.json()

    code = body.get("code", "").strip()
    wos  = body.get("wos", [])

    if not code:
        raise HTTPException(400, "Kode Kertas Kerja wajib diisi")
    if not wos:
        raise HTTPException(400, "Pilih minimal satu Work Order")

    ph     = ",".join(["%s"] * len(wos))
    conds  = [
        f'"order" IN ({ph})',
        "UPPER(COALESCE(del,'')) != 'X'",
        "UPPER(COALESCE(fis,'')) != 'X'",
    ]
    params = list(wos)
    pc, pp = plant_clause(user, "plant"); conds.append(pc); params.extend(pp)
    where  = " AND ".join(conds)

    rows = query(f"SELECT * FROM prisma_reservasi WHERE {where} ORDER BY id", params)

    if not rows:
        raise HTTPException(404, "Tidak ada data PRISMA untuk WO yang dipilih")

    kk_data = []
    for r in rows:
        d = dict(r)
        d["CodeKertasKerja"]  = code
        d["Qty_StockOnhand"]  = d.get("qty_stock_onhand")
        # Rename keys ke format frontend
        kk_data.append({
            "ID": d["id"], "Plant": d["plant"], "Equipment": d["equipment"],
            "Revision": d["revision"], "Order": d["order"], "Reservno": d["reservno"],
            "Itm": d["itm"], "Material": d["material"],
            "Material_Description": d["material_description"],
            "Del": d["del"], "FIs": d["fis"], "Ict": d["ict"], "PG": d["pg"],
            "Recipient": d["recipient"], "Unloading_point": d["unloading_point"],
            "Reqmts_Date": d["reqmts_date"], "Qty_Reqmts": _n(d["qty_reqmts"]),
            "UoM": d["uom"], "PR_Prisma": d["pr_prisma"],
            "Item_Prisma": d["item_prisma"],
            "Qty_PR_Prisma": _n(d["qty_pr_prisma"]),
            "Qty_StockOnhand": _n(d["qty_stock_onhand"]),
            "CodeKertasKerja": code,
        })

    set_state("kk_current", {"code": code, "data": kk_data})

    return jsonify({
        "ok": True, "code": code,
        "rows": len(kk_data),
        "orders": len(wos),
        "msg": f"✅ Kertas Kerja {code} dibuat dengan {len(kk_data)} baris dari {len(wos)} WO"
    })

@app.put("/api/prisma")
async def put_prisma(request: Request):
    check_api_key(request)
    rows = await request.json()
    if not isinstance(rows, list):
        raise HTTPException(400, "Body harus array")
    from psycopg2.extras import execute_values
    from database import get_conn, release_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for r in rows:
                if r.get("ID"):
                    cur.execute("""UPDATE prisma_reservasi SET
                        plant=%s,equipment=%s,revision=%s,"order"=%s,reservno=%s,itm=%s,
                        material=%s,material_description=%s,del=%s,fis=%s,ict=%s,pg=%s,
                        recipient=%s,unloading_point=%s,reqmts_date=%s,qty_reqmts=%s,uom=%s,
                        pr_prisma=%s,item_prisma=%s,qty_pr_prisma=%s,qty_stock_onhand=%s,
                        code_kertas_kerja=%s,updated_at=NOW()
                        WHERE id=%s""",
                        (r.get("Plant"),r.get("Equipment"),r.get("Revision"),r.get("Order"),
                         r.get("Reservno"),r.get("Itm"),r.get("Material"),r.get("Material_Description"),
                         r.get("Del"),r.get("FIs"),r.get("Ict"),r.get("PG"),
                         r.get("Recipient"),r.get("Unloading_point"),r.get("Reqmts_Date"),
                         r.get("Qty_Reqmts",0),r.get("UoM"),
                         r.get("PR_Prisma"),r.get("Item_Prisma"),r.get("Qty_PR_Prisma"),
                         r.get("Qty_StockOnhand"),r.get("CodeKertasKerja"),r["ID"]))
                else:
                    cur.execute("""INSERT INTO prisma_reservasi
                        (plant,equipment,revision,"order",reservno,itm,material,material_description,
                         del,fis,ict,pg,recipient,unloading_point,reqmts_date,qty_reqmts,uom,
                         pr_prisma,item_prisma,qty_pr_prisma,qty_stock_onhand,code_kertas_kerja)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (r.get("Plant"),r.get("Equipment"),r.get("Revision"),r.get("Order"),
                         r.get("Reservno"),r.get("Itm"),r.get("Material"),r.get("Material_Description"),
                         r.get("Del"),r.get("FIs"),r.get("Ict"),r.get("PG"),
                         r.get("Recipient"),r.get("Unloading_point"),r.get("Reqmts_Date"),
                         r.get("Qty_Reqmts",0),r.get("UoM"),
                         r.get("PR_Prisma"),r.get("Item_Prisma"),r.get("Qty_PR_Prisma"),
                         r.get("Qty_StockOnhand"),r.get("CodeKertasKerja")))
        conn.commit()
    except Exception as e:
        conn.rollback(); raise HTTPException(500, str(e))
    finally:
        release_conn(conn)
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# SINKRON TA-EX → PRISMA (server-side, lebih cepat dari client-side)
# ═══════════════════════════════════════════════════════════════
@app.post("/api/prisma/sync-from-taex")
def sync_prisma_from_taex(request: Request):
    """
    Sinkron data dari TA-ex ke PRISMA dengan aturan:
    - ICt = 'L'
    - Del bukan 'X'
    - FIs bukan 'X'
    - qty_reqmts > 0  ← baris dengan qty 0 tidak ditarik
    Hanya tambah baris baru (tidak timpa yang sudah ada).
    """
    check_api_key(request)

    # Ambil semua dari taex dengan filter server-side
    all_taex = query("""
        SELECT * FROM taex_reservasi
        WHERE UPPER(COALESCE(ict,'')) = 'L'
          AND UPPER(COALESCE(del,'')) != 'X'
          AND UPPER(COALESCE(fis,'')) != 'X'
          AND COALESCE(qty_reqmts, 0) > 0
    """)

    if not all_taex:
        return {"ok": True, "added": 0, "skipped": 0,
                "msg": "Tidak ada data TA-ex yang memenuhi syarat (ICt=L, Del≠X, FIs≠X, Qty>0)"}

    # Ambil existing prisma untuk cek duplikat
    existing = query('SELECT "order", material, itm FROM prisma_reservasi')
    exist_set = {(r["order"], r["material"], r["itm"]) for r in existing}

    new_rows = [t for t in all_taex
                if (t["order"], t["material"], t["itm"]) not in exist_set]
    skip_count = len(all_taex) - len(new_rows)

    if new_rows:
        from psycopg2.extras import execute_values
        from database import get_conn, release_conn
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO prisma_reservasi
                    (plant, equipment, revision, "order", reservno, itm, material,
                     material_description, del, fis, ict, pg, recipient, unloading_point,
                     reqmts_date, qty_reqmts, uom)
                    VALUES %s
                """, [(
                    t["plant"], t["equipment"], t["revision"], t["order"],
                    t["reservno"], t["itm"], t["material"], t["material_description"],
                    t["del"], t["fis"], t["ict"], t["pg"], t["recipient"],
                    t["unloading_point"], t["reqmts_date"], t["qty_reqmts"], t["uom"]
                ) for t in new_rows])
            conn.commit()
        finally:
            release_conn(conn)

    total_prisma = query("SELECT COUNT(*) AS c FROM prisma_reservasi")[0]["c"]
    return {
        "ok": True,
        "added": len(new_rows),
        "skipped": skip_count,
        "total": int(total_prisma),
        "msg": f"✅ {len(new_rows):,} baris baru ditambahkan, {skip_count:,} sudah ada atau dilewati"
    }


# ═══════════════════════════════════════════════════════════════
# SINKRON PR → KUMPULAN SUMMARY (server-side, tidak return data taex)
# ═══════════════════════════════════════════════════════════════
@app.post("/api/kumpulan/sync-pr")
def sync_kumpulan_pr(request: Request):
    """
    Sinkron nomor PR dari SAP PR ke Kumpulan Summary.
    Match by: material + (tracking_no atau tracking) = code_tracking
    Return HANYA hasil sinkron — tidak return data taex agar tab TA-ex tidak terganggu.
    """
    check_api_key(request)

    kumpulan_rows = query("SELECT * FROM kumpulan_summary")
    pr_rows       = query("SELECT * FROM sap_pr")

    if not kumpulan_rows:
        return {"ok": True, "matched": 0, "msg": "Kumpulan Summary kosong"}

    from database import get_conn, release_conn
    conn = get_conn()
    matched_count = 0
    preview = []

    try:
        with conn.cursor() as cur:
            for k in kumpulan_rows:
                pr_item = next((
                    p for p in pr_rows
                    if p["material"] == k["material"]
                    and (p["tracking_no"] == k["code_tracking"]
                         or p["tracking"]  == k["code_tracking"])
                ), None)

                if not pr_item:
                    continue

                matched_count += 1
                qty_to_pr = max(0,
                    float(k["qty_req"] or 0)
                    - float(k["qty_stock"] or 0)
                    - float(pr_item["qty_pr"] or 0)
                )

                cur.execute("""
                    UPDATE kumpulan_summary
                    SET qty_pr=%s, qty_to_pr=%s, updated_at=NOW()
                    WHERE id=%s
                """, (pr_item["qty_pr"], qty_to_pr, k["id"]))

                # ── Update PRISMA: pr_prisma, item_prisma, qty_pr_prisma ──
                cur.execute("""
                    UPDATE prisma_reservasi
                    SET pr_prisma=%s, item_prisma=%s, qty_pr_prisma=%s, updated_at=NOW()
                    WHERE material=%s AND code_kertas_kerja=%s
                """, (pr_item["pr"], pr_item["item"], pr_item["qty_pr"],
                      k["material"], k["code_tracking"]))

                # ── Update TAEX: PR, Item, Qty_PR + Qty_Stock dari qty_stock_onhand prisma ──
                # qty_stock di taex diisi dari qty_stock_onhand di prisma (match by order+material+itm)
                cur.execute("""
                    UPDATE taex_reservasi t
                    SET pr       = %s,
                        item     = %s,
                        qty_pr   = %s,
                        qty_stock = COALESCE(p.qty_stock_onhand, t.qty_stock),
                        updated_at = NOW()
                    FROM prisma_reservasi p
                    WHERE t.material = p.material
                      AND t."order" = p."order"
                      AND t.itm     = p.itm
                      AND p.material = %s
                      AND p.code_kertas_kerja = %s
                """, (pr_item["pr"], pr_item["item"], pr_item["qty_pr"],
                      k["material"], k["code_tracking"]))

                preview.append({
                    "Material":  k["material"],
                    "Deskripsi": k["material_description"],
                    "PR":        pr_item["pr"],
                    "Item":      pr_item["item"],
                    "Qty_PR":    float(pr_item["qty_pr"] or 0),
                    "Tracking":  k["code_tracking"],
                })

        conn.commit()
    finally:
        release_conn(conn)

    # Return kumpulan yang terupdate — BUKAN semua data taex (tab TA-ex tidak reset)
    updated_kumpulan = query("SELECT * FROM kumpulan_summary ORDER BY id")
    return jsonify({
        "ok": True,
        "matched": matched_count,
        "preview": preview,
        "kumpulanData": [map_kumpulan(r) for r in updated_kumpulan],
        "msg": f"✅ {matched_count} material PR tersinkron — kumpulan + prisma + taex diupdate"
    })


# ═══════════════════════════════════════════════════════════════
# KUMPULAN
# ═══════════════════════════════════════════════════════════════
@app.get("/api/kumpulan")
def get_kumpulan(request: Request):
    check_api_key(request)
    rows = query("SELECT * FROM kumpulan_summary ORDER BY id")
    return jsonify([map_kumpulan(r) for r in rows])

@app.put("/api/kumpulan")
async def put_kumpulan(request: Request):
    check_api_key(request)
    rows = await request.json()
    if not isinstance(rows, list):
        raise HTTPException(400, "Body harus array")
    df = pd.DataFrame(rows)
    cnt = bulk_replace_kumpulan(df)
    return {"ok": True, "count": cnt}


# ═══════════════════════════════════════════════════════════════
# SAP PR
# ═══════════════════════════════════════════════════════════════
@app.get("/api/pr")
def get_pr(request: Request):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["1=1"], []
    pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    gc, gp = pg_clause(user, "pgr");      clauses.append(gc); params.extend(gp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM sap_pr WHERE {where} ORDER BY id", params)
    return jsonify([map_sap(r) for r in rows])

@app.put("/api/pr")
async def put_pr(request: Request):
    check_api_key(request)
    rows = await request.json()
    df = pd.DataFrame(rows)
    cnt = bulk_replace_pr(df)
    return {"ok": True, "count": cnt}

@app.post("/api/pr/replace")
async def replace_pr(request: Request):
    check_api_key(request)
    rows = await request.json()
    df = pd.DataFrame(rows)
    cnt = bulk_replace_pr(df)
    return {"ok": True, "count": cnt}

@app.post("/api/pr/append")
async def append_pr(request: Request):
    check_api_key(request)
    rows = await request.json()
    from psycopg2.extras import execute_values
    from database import get_conn, release_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            from bulk_ops import _s, _n
            vals = []
            for r in rows:
                nr = normalize_sap(r)
                vals.append((_s(nr.get("Plant")),_s(nr.get("PR")),_s(nr.get("Item")),
                              _s(nr.get("Material")),_s(nr.get("Material_Description")),
                              _s(nr.get("D")),_s(nr.get("R")),_s(nr.get("PGr")),
                              _s(nr.get("S")),_s(nr.get("TrackingNo")),
                              _n(nr.get("Qty_PR")),_s(nr.get("Un")),_s(nr.get("Req_Date")),
                              _n(nr.get("Valn_price")),_s(nr.get("PR_Curr")),_n(nr.get("PR_Per")),
                              _s(nr.get("Release_Date")),_s(nr.get("Tracking"))))
            execute_values(cur, """INSERT INTO sap_pr
                (plant,pr,item,material,material_description,d,r,pgr,s,tracking_no,
                 qty_pr,un,req_date,valn_price,pr_curr,pr_per,release_date,tracking)
                VALUES %s""", vals)
        conn.commit()
    finally:
        release_conn(conn)
    rows_all = query("SELECT * FROM sap_pr ORDER BY id")
    return jsonify({"ok": True, "count": len(vals), "data": [map_sap(r) for r in rows_all]})


# ═══════════════════════════════════════════════════════════════
# SAP PO
# ═══════════════════════════════════════════════════════════════
@app.get("/api/po")
def get_po(request: Request):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["1=1"], []
    pc, pp = plant_clause(user, "plnt"); clauses.append(pc); params.extend(pp)
    gc, gp = pg_clause(user, "pgr");     clauses.append(gc); params.extend(gp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM sap_po WHERE {where} ORDER BY id", params)
    return jsonify([map_po(r) for r in rows])

@app.put("/api/po")
async def put_po(request: Request):
    check_api_key(request)
    rows = await request.json()
    df = pd.DataFrame(rows)
    cnt = bulk_replace_po(df)
    return {"ok": True, "count": cnt}

@app.post("/api/po/replace")
async def replace_po(request: Request):
    check_api_key(request)
    rows = await request.json()
    df = pd.DataFrame(rows)
    cnt = bulk_replace_po(df)
    return {"ok": True, "count": cnt}

@app.post("/api/po/append")
async def append_po(request: Request):
    check_api_key(request)
    rows = await request.json()
    from psycopg2.extras import execute_values
    from database import get_conn, release_conn
    from bulk_ops import _s, _n
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            vals = [(_s(r.get("Plnt")),_s(r.get("Purchreq")),_s(r.get("Item")),
                     _s(r.get("Material")),_s(r.get("Short_Text")),
                     _s(r.get("PO")),_s(r.get("PO_Item")),
                     _s(r.get("D")),_s(r.get("DCI")),_s(r.get("PGr")),
                     _s(r.get("Doc_Date")),_n(r.get("PO_Quantity")),_n(r.get("Qty_Delivered")),
                     _s(r.get("Deliv_Date")),_s(r.get("OUn")),
                     _n(r.get("Net_Price")),_s(r.get("Crcy")),_n(r.get("Per")))
                    for r in rows]
            execute_values(cur, """INSERT INTO sap_po
                (plnt,purchreq,item,material,short_text,po,po_item,d,dci,pgr,
                 doc_date,po_quantity,qty_delivered,deliv_date,oun,net_price,crcy,per)
                VALUES %s""", vals)
        conn.commit()
    finally:
        release_conn(conn)
    rows_all = query("SELECT * FROM sap_po ORDER BY id")
    return jsonify({"ok": True, "count": len(vals), "data": [map_po(r) for r in rows_all]})


# ═══════════════════════════════════════════════════════════════
# WORK ORDER
# ═══════════════════════════════════════════════════════════════
@app.get("/api/order")
def get_order(request: Request):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["1=1"], []
    pc, pp = plant_clause(user, "plant");        clauses.append(pc); params.extend(pp)
    gc, gp = pg_clause(user, "planner_group");   clauses.append(gc); params.extend(gp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM work_order WHERE {where} ORDER BY id", params)
    return jsonify([map_order(r) for r in rows])

@app.put("/api/order")
async def put_order(request: Request):
    check_api_key(request)
    rows = await request.json()
    if not isinstance(rows, list):
        raise HTTPException(400, "Body harus array")
    df = pd.DataFrame(rows)
    cnt = bulk_replace_order(df)
    return {"ok": True, "count": cnt}

@app.delete("/api/order/{row_id}")
def delete_order(row_id: int, request: Request):
    check_api_key(request)
    execute("DELETE FROM work_order WHERE id=%s", (row_id,))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# VW JOBLIST WO  — server-side pagination
# ═══════════════════════════════════════════════════════════════
@app.get("/api/vwjoblistwo")
def get_vw_joblist_wo(
    request: Request,
    page: int = 1, limit: int = 100,
    q: str = "", revision: str = "", disiplin: str = "",
    planning_jasa: str = "", planning_material: str = "",
    known_total: int = 0,
):
    check_api_key(request)
    user = get_current_user(request)
    limit  = min(5000, max(1, limit))
    offset = (page - 1) * limit

    conds, params = [], []
    pc, pp = plant_clause(user, "plant"); conds.append(pc); params.extend(pp)

    if q:
        conds.append("""(
            "order" ILIKE %s OR equipment_no ILIKE %s OR
            joblist_description ILIKE %s OR description ILIKE %s OR
            revision ILIKE %s
        )""")
        params.extend([f"%{q}%"] * 5)
    if revision:
        conds.append("revision = %s"); params.append(revision)
    if disiplin:
        conds.append("disiplin = %s"); params.append(disiplin)
    if planning_jasa:
        conds.append("planning_jasa_status = %s"); params.append(planning_jasa)
    if planning_material:
        conds.append("planning_material_status = %s"); params.append(planning_material)

    where = " AND ".join(conds) if conds else "1=1"

    total = known_total
    if not total:
        total = int(query(f'SELECT COUNT(*) AS c FROM vw_joblist_wo WHERE {where}', params)[0]["c"])

    rows = query(
        f'SELECT * FROM vw_joblist_wo WHERE {where} ORDER BY id LIMIT %s OFFSET %s',
        params + [limit, offset]
    )
    return jsonify({
        "data": [map_vw_joblist_wo(r) for r in rows],
        "pagination": {
            "page": page, "limit": limit, "total": total,
            "totalPages": max(1, -(-total // limit)),
            "hasMore": offset + limit < total,
        },
    })

@app.delete("/api/vwjoblistwo")
def delete_vw_joblist_wo(request: Request):
    check_api_key(request)
    execute("DELETE FROM vw_joblist_wo")
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# VW JOBLIST DETAIL  — server-side pagination
# ═══════════════════════════════════════════════════════════════
@app.get("/api/vwjoblistdetail")
def get_vw_joblist_detail(
    request: Request,
    page: int = 1, limit: int = 100,
    q: str = "", revision: str = "", disiplin: str = "",
    planning_material: str = "", project_status: str = "",
    known_total: int = 0,
):
    check_api_key(request)
    user = get_current_user(request)
    limit  = min(5000, max(1, limit))
    offset = (page - 1) * limit

    conds, params = [], []
    pc, pp = plant_clause(user, "plant"); conds.append(pc); params.extend(pp)

    if q:
        conds.append("""(
            equipment_no ILIKE %s OR no_joblist ILIKE %s OR
            joblist_detail_desc ILIKE %s OR project_number ILIKE %s OR
            joblist_description ILIKE %s
        )""")
        params.extend([f"%{q}%"] * 5)
    if revision:
        conds.append("revision = %s"); params.append(revision)
    if disiplin:
        conds.append("disiplin = %s"); params.append(disiplin)
    if planning_material:
        conds.append("planning_material_status = %s"); params.append(planning_material)
    if project_status:
        conds.append("project_status = %s"); params.append(project_status)

    where = " AND ".join(conds) if conds else "1=1"

    total = known_total
    if not total:
        total = int(query(f'SELECT COUNT(*) AS c FROM vw_joblist_detail WHERE {where}', params)[0]["c"])

    rows = query(
        f'SELECT * FROM vw_joblist_detail WHERE {where} ORDER BY inserted_at DESC LIMIT %s OFFSET %s',
        params + [limit, offset]
    )
    return jsonify({
        "data": [map_vw_joblist_detail(r) for r in rows],
        "pagination": {
            "page": page, "limit": limit, "total": total,
            "totalPages": max(1, -(-total // limit)),
            "hasMore": offset + limit < total,
        },
    })


# ═══════════════════════════════════════════════════════════════
# TRACKING VIEW — berbasis taex_reservasi sebagai sumber utama
# Detail gabungan: taex + sap_pr + sap_po + kumpulan_summary
# Qty PR, Stock, PO semuanya diambil dari taex (bukan dari sap_pr langsung)
# karena taex sudah merupakan gabungan dari beberapa material/reservasi
# ═══════════════════════════════════════════════════════════════
@app.get("/api/tracking")
def get_tracking(
    request: Request,
    page: int = 1, limit: int = 100,
    q: str = "",
    order_val: str = "",
    material: str = "",
    pr: str = "",
    po: str = "",
    plant: str = "",
    status: str = "",   # "with_pr","without_pr","with_po","without_po","no-pr","pr-created","po-created","partial","complete"
    order_by: str = "t.id", order_dir: str = "ASC",
    known_total: int = 0,
):
    """
    Tracking view berbasis taex_reservasi.

    Sumber kebenaran data:
    - Qty_Reqmts, Qty_Stock, Qty_PR, Qty_Deliv  → dari taex_reservasi (sudah terupdate via sync-pr)
    - PR, PO, PO_Date, Delivery_Date            → dari taex_reservasi
    - Tracking, TrackingNo, Valn_price          → join sap_pr (match by taex.pr = sap_pr.pr AND taex.material = sap_pr.material)
    - PO detail (Doc_Date, Net_Price, Crcy)     → join sap_po (match by taex.po = sap_po.po AND taex.material = sap_po.material)
    - CodeTracking (kumpulan)                    → join kumpulan_summary (match by taex.material + taex.order)

    Dengan demikian Qty_PR di tracking = Qty_PR di taex (bukan SUM dari sap_pr),
    karena taex sudah merupakan breakdown per reservasi/material.
    """
    check_api_key(request)
    limit  = min(5000, max(1, limit))
    offset = (page - 1) * limit

    conds, params = [], []

    if q:
        conds.append("""(
            t.material          ILIKE %s OR
            t.material_description ILIKE %s OR
            t."order"           ILIKE %s OR
            t.equipment         ILIKE %s OR
            t.pr                ILIKE %s OR
            t.po                ILIKE %s OR
            t.reservno          ILIKE %s OR
            COALESCE(sp.tracking,'')    ILIKE %s OR
            COALESCE(sp.tracking_no,'') ILIKE %s OR
            COALESCE(k.code_tracking,'') ILIKE %s
        )""")
        params.extend([f"%{q}%"] * 10)

    if order_val:
        conds.append('t."order" ILIKE %s'); params.append(f"%{order_val}%")
    if material:
        conds.append("t.material ILIKE %s"); params.append(f"%{material}%")
    if pr:
        conds.append("t.pr ILIKE %s"); params.append(f"%{pr}%")
    if po:
        conds.append("t.po ILIKE %s"); params.append(f"%{po}%")

    # Filter plant
    if plant:
        conds.append("t.plant = %s"); params.append(plant)

    # Filter status PR/PO/Delivery
    if status in ("with_pr",):
        conds.append("t.pr IS NOT NULL AND t.pr <> ''")
    elif status in ("without_pr", "no-pr"):
        conds.append("(t.pr IS NULL OR t.pr = '')")
    elif status == "with_po":
        conds.append("t.po IS NOT NULL AND t.po <> ''")
    elif status == "without_po":
        conds.append("(t.po IS NULL OR t.po = '')")
    elif status == "pr-created":
        conds.append("t.pr IS NOT NULL AND t.pr <> '' AND (t.po IS NULL OR t.po = '')")
    elif status == "po-created":
        conds.append("t.po IS NOT NULL AND t.po <> '' AND COALESCE(po.po_qty_delivered, 0) = 0")
    elif status == "partial":
        conds.append("COALESCE(po.po_qty_delivered, 0) > 0 AND COALESCE(po.po_qty_delivered, 0) < COALESCE(po.po_quantity, 0)")
    elif status == "complete":
        conds.append("COALESCE(po.po_quantity, 0) > 0 AND COALESCE(po.po_qty_delivered, 0) >= COALESCE(po.po_quantity, 0)")

    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    # Kolom sortable yang aman
    SORTABLE = {
        "t.id", "t.plant", "t.equipment", 't."order"', "t.material",
        "t.itm", "t.qty_reqmts", "t.qty_stock", "t.pr", "t.qty_pr",
        "t.po", "t.qty_deliv", "t.delivery_date", "t.reqmts_date",
        "t.res_price", "sp.tracking", "sp.tracking_no",
    }
    safe_ob  = order_by if order_by in SORTABLE else "t.id"
    safe_dir = "DESC" if order_dir.upper() == "DESC" else "ASC"

    # ── JOIN utama: taex sebagai driving table ──
    base_sql = """
        FROM taex_reservasi t
        -- sap_pr: ambil semua kolom, match by pr + material
        LEFT JOIN LATERAL (
            SELECT sp.plant     AS pr_plant,
                   sp.pr        AS pr_pr,
                   sp.item      AS pr_item,
                   sp.material  AS pr_material,
                   sp.material_description AS pr_material_description,
                   sp.d         AS pr_d,
                   sp.r         AS pr_r,
                   sp.pgr       AS pr_pgr,
                   sp.s         AS pr_s,
                   sp.tracking_no,
                   sp.qty_pr    AS pr_qty_pr,
                   sp.un        AS pr_un,
                   sp.req_date,
                   sp.valn_price,
                   sp.pr_curr,
                   sp.pr_per,
                   sp.release_date,
                   sp.tracking
            FROM sap_pr sp
            WHERE sp.pr = t.pr
              AND sp.material = t.material
            ORDER BY sp.id
            LIMIT 1
        ) sp ON TRUE
        -- sap_po: ambil semua kolom, match by po + material
        LEFT JOIN LATERAL (
            SELECT po.plnt          AS po_plnt,
                   po.purchreq      AS po_purchreq,
                   po.item          AS po_item,
                   po.material      AS po_material,
                   po.short_text    AS po_short_text,
                   po.po            AS po_po,
                   po.po_item       AS po_po_item,
                   po.d             AS po_d,
                   po.dci           AS po_dci,
                   po.pgr           AS po_pgr,
                   po.doc_date      AS po_doc_date,
                   po.po_quantity   AS po_quantity,
                   po.qty_delivered AS po_qty_delivered,
                   po.deliv_date    AS po_deliv_date,
                   po.oun           AS po_oun,
                   po.net_price     AS po_net_price,
                   po.crcy          AS po_crcy,
                   po.per           AS po_per
            FROM sap_po po
            WHERE po.po = t.po
              AND po.material = t.material
            ORDER BY po.id
            LIMIT 1
        ) po ON TRUE
        -- work_order: kolom lengkap untuk tracking
        LEFT JOIN LATERAL (
            SELECT wo.description,
                   wo.system_status,
                   wo.user_status,
                   wo.basic_start_date,
                   wo.basic_finish_date,
                   wo.actual_release,
                   wo.notification,
                   wo.funct_location,
                   wo.planner_group,
                   wo.main_work_ctr,
                   wo.superior_order,
                   wo.created_on,
                   wo.location,
                   wo.wbs_ord_header,
                   wo.cost_center,
                   wo.total_plan_cost,
                   wo.total_act_cost,
                   wo.entry_by,
                   wo.changed_by,
                   wo.revision,
                   wo.equipment AS wo_equipment
            FROM work_order wo
            WHERE wo."order" = t."order"
            ORDER BY wo.id
            LIMIT 1
        ) wo ON TRUE
    """

    # COUNT — skip jika known_total sudah ada (page nav tanpa filter berubah)
    if known_total > 0:
        total = known_total
    else:
        needs_po_join = status in ("partial", "complete", "po-created")
        if needs_po_join:
            count_res = query(f"SELECT COUNT(*) AS c {base_sql} {where}", params)
        else:
            sb = """
                FROM taex_reservasi t
                LEFT JOIN LATERAL (
                    SELECT sp.tracking FROM sap_pr sp
                    WHERE sp.pr = t.pr AND sp.material = t.material
                    ORDER BY sp.id LIMIT 1
                ) sp ON TRUE
            """
            count_res = query(f"SELECT COUNT(*) AS c {sb} {where}", params)
        total = int(count_res[0]["c"])

    data_res  = query(
        f"""SELECT
            -- ── Semua kolom taex_reservasi ──
            t.id,
            t.plant, t.equipment, t."order", t.revision, t.reservno,
            t.material, t.itm, t.material_description,
            t.qty_reqmts, t.qty_stock, t.qty_pr, t.qty_deliv,
            t.qty_f_avail_check, t.qty_withdrawn,
            t.pr, t.item, t.cost_ctrs,
            t.po, t.po_date, t.delivery_date,
            t.sloc, t.del, t.fis, t.ict, t.pg,
            t.recipient, t.unloading_point, t.reqmts_date,
            t.uom, t.gl_acct, t.res_price, t.res_per, t.res_curr,
            -- ── Semua kolom sap_pr ──
            sp.pr_plant, sp.pr_pr, sp.pr_item, sp.pr_material,
            sp.pr_material_description, sp.pr_d, sp.pr_r, sp.pr_pgr, sp.pr_s,
            sp.tracking_no, sp.pr_qty_pr, sp.pr_un,
            sp.req_date, sp.valn_price, sp.pr_curr, sp.pr_per,
            sp.release_date, sp.tracking,
            -- ── Semua kolom sap_po ──
            po.po_plnt, po.po_purchreq, po.po_item, po.po_material,
            po.po_short_text, po.po_po, po.po_po_item,
            po.po_d, po.po_dci, po.po_pgr,
            po.po_doc_date, po.po_quantity, po.po_qty_delivered,
            po.po_deliv_date, po.po_oun,
            po.po_net_price, po.po_crcy, po.po_per,
            -- ── Kolom work_order yang relevan untuk tracking progress ──
            wo.description       AS wo_description,
            wo.system_status     AS wo_system_status,
            wo.user_status       AS wo_user_status,
            wo.basic_start_date  AS wo_basic_start_date,
            wo.basic_finish_date AS wo_basic_finish_date,
            wo.actual_release    AS wo_actual_release,
            wo.notification      AS wo_notification,
            wo.funct_location    AS wo_funct_location,
            wo.planner_group     AS wo_planner_group,
            wo.main_work_ctr     AS wo_main_work_ctr,
            wo.superior_order    AS wo_superior_order,
            wo.created_on        AS wo_created_on,
            wo.location          AS wo_location,
            wo.wbs_ord_header    AS wo_wbs_ord_header,
            wo.cost_center       AS wo_cost_center,
            wo.total_plan_cost   AS wo_total_plan_cost,
            wo.total_act_cost    AS wo_total_act_cost,
            wo.entry_by          AS wo_entry_by,
            wo.changed_by        AS wo_changed_by
        {base_sql} {where}
        ORDER BY {safe_ob} {safe_dir}
        LIMIT %s OFFSET %s
        """,
        params + [limit, offset],
    )

    def map_tracking(r):
        return {
            # ── taex_reservasi — semua kolom ──
            "ID":                   r["id"],
            "Plant":                r["plant"],
            "Equipment":            r["equipment"],
            "Order":                r["order"],
            "Revision":             r["revision"],
            "Reservno":             r["reservno"],
            "Material":             r["material"],
            "Itm":                  r["itm"],
            "Material_Description": r["material_description"],
            "Qty_Reqmts":           _n(r["qty_reqmts"]),
            "Qty_Stock":            _n(r["qty_stock"]),
            "Qty_PR":               _n(r["qty_pr"]),
            "Qty_Deliv":            _n(r["qty_deliv"]),
            "Qty_f_avail_check":    _n(r["qty_f_avail_check"]),
            "Qty_Withdrawn":        _n(r["qty_withdrawn"]),
            "PR":                   r["pr"],
            "Item":                 r["item"],
            "Cost_Ctrs":            r["cost_ctrs"],
            "PO":                   r["po"],
            "PO_Date":              r["po_date"],
            "Delivery_Date":        r["delivery_date"],
            "SLoc":                 r["sloc"],
            "Del":                  r["del"],
            "FIs":                  r["fis"],
            "Ict":                  r["ict"],
            "PG":                   r["pg"],
            "Recipient":            r["recipient"],
            "Unloading_point":      r["unloading_point"],
            "Reqmts_Date":          r["reqmts_date"],
            "UoM":                  r["uom"],
            "GL_Acct":              r["gl_acct"],
            "Res_Price":            _n(r["res_price"]),
            "Res_per":              _n(r["res_per"]),
            "Res_Curr":             r["res_curr"],
            # ── sap_pr — semua kolom ──
            "PR_Plant":             r["pr_plant"],
            "PR_PR":                r["pr_pr"],
            "PR_Item":              r["pr_item"],
            "PR_Material":          r["pr_material"],
            "PR_Material_Desc":     r["pr_material_description"],
            "PR_D":                 r["pr_d"],
            "PR_R":                 r["pr_r"],
            "PR_PGr":               r["pr_pgr"],
            "PR_S":                 r["pr_s"],
            "TrackingNo":           r["tracking_no"],
            "PR_Qty_PR":            _n(r["pr_qty_pr"]),
            "PR_Un":                r["pr_un"],
            "Req_Date":             r["req_date"],
            "Valn_price":           _n(r["valn_price"]),
            "PR_Curr":              r["pr_curr"],
            "PR_Per":               _n(r["pr_per"]),
            "Release_Date":         r["release_date"],
            "Tracking":             r["tracking"],
            # ── sap_po — semua kolom ──
            "PO_Plnt":              r["po_plnt"],
            "PO_Purchreq":          r["po_purchreq"],
            "PO_Item":              r["po_item"],
            "PO_Material":          r["po_material"],
            "PO_Short_Text":        r["po_short_text"],
            "PO_PO":                r["po_po"],
            "PO_PO_Item":           r["po_po_item"],
            "PO_D":                 r["po_d"],
            "PO_DCI":               r["po_dci"],
            "PO_PGr":               r["po_pgr"],
            "PO_Doc_Date":          r["po_doc_date"],
            "PO_Quantity":          _n(r["po_quantity"]),
            "PO_Qty_Delivered":     _n(r["po_qty_delivered"]),
            "PO_Deliv_Date":        r["po_deliv_date"],
            "PO_OUn":               r["po_oun"],
            "PO_Net_Price":         _n(r["po_net_price"]),
            "PO_Crcy":              r["po_crcy"],
            "PO_Per":               _n(r["po_per"]),
            # ── work_order — kolom relevan untuk progress tracking ──
            "WO_Description":       r["wo_description"],
            "WO_System_Status":     r["wo_system_status"],
            "WO_User_Status":       r["wo_user_status"],
            "WO_Basic_Start":       r["wo_basic_start_date"],
            "WO_Basic_Finish":      r["wo_basic_finish_date"],
            "WO_Actual_Release":    r["wo_actual_release"],
            "WO_Notification":      r["wo_notification"],
            "WO_Funct_Location":    r["wo_funct_location"],
            "WO_Planner_Group":     r["wo_planner_group"],
            "WO_Main_Work_Ctr":     r["wo_main_work_ctr"],
            # ── work_order extra fields ──
            "Superior_Order":       r["wo_superior_order"],
            "Created_On":           r["wo_created_on"],
            "Location":             r["wo_location"],
            "WBS_Ord_header":       r["wo_wbs_ord_header"],
            "CostCenter":           r["wo_cost_center"],
            "Total_Plan_Cost":      _n(r["wo_total_plan_cost"]),
            "Total_Act_Cost":       _n(r["wo_total_act_cost"]),
            "Entry_by":             r["wo_entry_by"],
            "Changed_by":           r["wo_changed_by"],
        }

    return jsonify({
        "data": [map_tracking(r) for r in data_res],
        "pagination": {
            "page": page, "limit": limit, "total": total,
            "totalPages": max(1, -(-total // limit)),
            "hasMore": offset + limit < total,
        },
    })


# ═══════════════════════════════════════════════════════════════
# TRACKING SUMMARY — ringkasan per PR/Tracking dari taex
# (digunakan untuk card summary di halaman tracking)
# ═══════════════════════════════════════════════════════════════
@app.get("/api/tracking/summary")
def get_tracking_summary(request: Request):
    """
    Ringkasan tracking berbasis taex_reservasi:
    - Total material, total Qty_Reqmts, total Qty_Stock, total Qty_PR, total Qty_Deliv
    - Digroup per PR (bukan per sap_pr row)
    - Karena taex sudah 1 baris per reservasi/material, SUM di sini adalah benar
    """
    check_api_key(request)

    summary = query("""
        SELECT
            COALESCE(t.pr, '(Tanpa PR)')  AS pr,
            COUNT(*)                        AS jumlah_material,
            SUM(COALESCE(t.qty_reqmts, 0)) AS total_reqmts,
            SUM(COALESCE(t.qty_stock,  0)) AS total_stock,
            SUM(COALESCE(t.qty_pr,     0)) AS total_qty_pr,
            SUM(COALESCE(t.qty_deliv,  0)) AS total_deliv,
            -- Cek apakah ada PO
            COUNT(CASE WHEN t.po IS NOT NULL AND t.po <> '' THEN 1 END) AS with_po,
            COUNT(CASE WHEN t.po IS NULL OR t.po = ''       THEN 1 END) AS without_po,
            -- Tracking info dari sap_pr (ambil salah satu yang match)
            (SELECT sp.tracking
             FROM sap_pr sp
             WHERE sp.pr = t.pr
             LIMIT 1) AS tracking,
            (SELECT sp.tracking_no
             FROM sap_pr sp
             WHERE sp.pr = t.pr
             LIMIT 1) AS tracking_no
        FROM taex_reservasi t
        GROUP BY t.pr
        ORDER BY t.pr NULLS LAST
    """)

    return jsonify([{
        "PR":             r["pr"],
        "JumlahMaterial": int(r["jumlah_material"]),
        "Total_Reqmts":   _n(r["total_reqmts"]),
        "Total_Stock":    _n(r["total_stock"]),
        "Total_Qty_PR":   _n(r["total_qty_pr"]),
        "Total_Deliv":    _n(r["total_deliv"]),
        "With_PO":        int(r["with_po"]),
        "Without_PO":     int(r["without_po"]),
        "Tracking":       r["tracking"] or "",
        "TrackingNo":     r["tracking_no"] or "",
    } for r in summary])


# ═══════════════════════════════════════════════════════════════
# TRACKING COUNTS — angka untuk summary card di halaman tracking
# ═══════════════════════════════════════════════════════════════
@app.get("/api/tracking/counts")
def get_tracking_counts(request: Request):
    """
    Hitung semua angka card tracking dalam satu query:
    - total_material, total_order
    - sudah_pr, sudah_po, belum_pr
    - partial, complete
    - total_nilai_po
    """
    check_api_key(request)

    # Query cepat — tidak butuh JOIN (index scan saja)
    fast = query_one("""
        SELECT
            COUNT(*)                                                AS total_material,
            COUNT(DISTINCT t."order")                               AS total_order,
            COUNT(*) FILTER (WHERE t.pr IS NOT NULL AND t.pr <> '') AS sudah_pr,
            COUNT(*) FILTER (WHERE t.po IS NOT NULL AND t.po <> '') AS sudah_po,
            COUNT(*) FILTER (WHERE t.pr IS NULL OR t.pr = '')       AS belum_pr
        FROM taex_reservasi t
    """)

    # Query lambat — butuh JOIN ke sap_po (dipisah agar tidak blok load awal)
    slow = query_one(f"""
        SELECT
            COUNT(*) FILTER (
                WHERE COALESCE(po.po_qty_delivered, 0) > 0
                  AND COALESCE(po.po_qty_delivered, 0) < COALESCE(po.po_quantity, 0)
            )                               AS partial,
            COUNT(*) FILTER (
                WHERE COALESCE(po.po_quantity, 0) > 0
                  AND COALESCE(po.po_qty_delivered, 0) >= COALESCE(po.po_quantity, 0)
            )                               AS complete,
            COALESCE(SUM(po.po_net_price), 0) AS total_nilai_po
        FROM taex_reservasi t
        LEFT JOIN LATERAL (
            SELECT po.po_quantity, po.qty_delivered AS po_qty_delivered,
                   po.net_price AS po_net_price
            FROM sap_po po
            WHERE po.po = t.po AND po.material = t.material
            ORDER BY po.id LIMIT 1
        ) po ON TRUE
        WHERE t.po IS NOT NULL AND t.po <> ''
    """)

    return jsonify({
        "total_material": int(fast["total_material"] or 0),
        "total_order":    int(fast["total_order"]    or 0),
        "sudah_pr":       int(fast["sudah_pr"]       or 0),
        "sudah_po":       int(fast["sudah_po"]       or 0),
        "belum_pr":       int(fast["belum_pr"]       or 0),
        "partial":        int(slow["partial"]        or 0),
        "complete":       int(slow["complete"]       or 0),
        "total_nilai_po": float(slow["total_nilai_po"] or 0),
    })


# ═══════════════════════════════════════════════════════════════
# AUDIT — server-side JOIN taex vs prisma
# ═══════════════════════════════════════════════════════════════
AUDIT_COLS = [
    ("equipment","Equipment"), ("reservno","Reserv.No."), ("revision","Revision"),
    ("material_description","Material Description"), ("qty_reqmts","Reqmt Qty"),
    ("del","Del"), ("fis","FIs"), ("ict","ICt"), ("pg","PG"),
    ("uom","BUn"), ("recipient","Recipient"), ("unloading_point","Unloading Point"),
    ("reqmts_date","Reqmt Date"),
]

@app.get("/api/audit")
def audit(request: Request, page: int = 1, limit: int = 100,
          q: str = "", col: str = ""):
    check_api_key(request)
    limit = min(500, max(1, limit))
    offset = (page - 1) * limit

    target = [(col, next(v for k,v in AUDIT_COLS if k==col))] if col else AUDIT_COLS

    extra = ""
    if q:
        extra = f" AND (t.\"order\" ILIKE '%{q}%' OR t.material ILIKE '%{q}%' OR t.itm::text ILIKE '%{q}%')"

    unions = []
    for key, label in target:
        pv = f"COALESCE(p.{key}::text,'')"
        tv = f"COALESCE(t.{key}::text,'')"
        unions.append(f"""
            SELECT t."order" AS order_val, t.material, t.itm,
                   '{key}' AS col_key, '{label}' AS col_label,
                   {pv} AS val_prisma, {tv} AS val_taex
            FROM prisma_reservasi p
            JOIN taex_reservasi t
              ON p."order"=t."order" AND p.material=t.material AND p.itm=t.itm
            WHERE p.{key} IS DISTINCT FROM t.{key}{extra}
        """)

    if not unions:
        return jsonify({"data":[], "pagination":{"page":1,"limit":limit,"total":0,"totalPages":1}, "changedRows":0})

    full_sql = " UNION ALL ".join(unions)
    count_res = query(f"SELECT COUNT(*) AS c FROM ({full_sql}) sub")
    data_res  = query(f"SELECT * FROM ({full_sql}) sub ORDER BY order_val,material,itm,col_key LIMIT %s OFFSET %s",
                      (limit, offset))

    all_diff = " OR ".join([f"p.{k} IS DISTINCT FROM t.{k}" for k,_ in AUDIT_COLS])
    changed_res = query(f"""
        SELECT COUNT(DISTINCT (t."order",t.material,t.itm)) AS c
        FROM prisma_reservasi p
        JOIN taex_reservasi t ON p."order"=t."order" AND p.material=t.material AND p.itm=t.itm
        WHERE {all_diff}{extra}
    """)

    total = int(count_res[0]["c"])
    return jsonify({
        "data": [{"Order": r["order_val"], "Material": r["material"], "Itm": r["itm"],
                  "col_key": r["col_key"], "col_label": r["col_label"],
                  "val_prisma": r["val_prisma"] or None, "val_taex": r["val_taex"] or None}
                 for r in data_res],
        "pagination": {"page": page, "limit": limit, "total": total,
                       "totalPages": max(1, -(-total // limit))},
        "changedRows": int(changed_res[0]["c"]),
    })


# ═══════════════════════════════════════════════════════════════
# APP STATE
# ═══════════════════════════════════════════════════════════════
@app.get("/api/state/{key}")
def get_state_api(key: str, request: Request):
    check_api_key(request)
    return {"key": key, "value": get_state(key)}

@app.post("/api/state/{key}")
async def set_state_api(key: str, request: Request):
    check_api_key(request)
    body = await request.json()
    set_state(key, body.get("value"))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# BULK SAVE
# ═══════════════════════════════════════════════════════════════
@app.post("/api/save")
async def bulk_save(request: Request):
    check_api_key(request)
    body = await request.json()

    taex_d   = body.get("taexData")
    prisma_d = body.get("prismaReservasiData")
    kumpulan_d = body.get("kumpulanData")
    pr_d     = body.get("prData")
    po_d     = body.get("poData")
    kk_d     = body.get("kkData")
    kk_code  = body.get("kkCode")
    sum_d    = body.get("summaryData")
    kk_ctr   = body.get("kkCounter")
    pr_ctr   = body.get("prCounter")

    if isinstance(taex_d, list)   and taex_d:   bulk_replace_taex(pd.DataFrame(taex_d), mode="replace")
    if isinstance(prisma_d, list) and prisma_d: bulk_replace_prisma(pd.DataFrame(prisma_d))
    if isinstance(kumpulan_d, list) and kumpulan_d: bulk_replace_kumpulan(pd.DataFrame(kumpulan_d))
    if isinstance(pr_d, list)     and pr_d:     bulk_replace_pr(pd.DataFrame(pr_d))
    if isinstance(po_d, list)     and po_d:     bulk_replace_po(pd.DataFrame(po_d))

    if kk_d is not None or kk_code is not None:
        set_state("kk_current", {"data": kk_d or [], "code": kk_code or None})
    if sum_d is not None:  set_state("summary_current", sum_d or [])
    if kk_ctr is not None: set_state("kk_counter", kk_ctr)
    if pr_ctr is not None: set_state("pr_counter", pr_ctr)

    return {"ok": True, "savedAt": datetime.now().isoformat()}


# ═══════════════════════════════════════════════════════════════
# RESET ALL
# ═══════════════════════════════════════════════════════════════
@app.post("/api/reset")
def reset_all(request: Request):
    check_api_key(request)
    for tbl in ["taex_reservasi","prisma_reservasi","kumpulan_summary",
                "sap_pr","sap_po","work_order","app_state"]:
        execute(f"DELETE FROM {tbl}")
    for seq in ["taex_reservasi_id_seq","prisma_reservasi_id_seq",
                "kumpulan_summary_id_seq","sap_pr_id_seq",
                "sap_po_id_seq","work_order_id_seq"]:
        try: execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
        except: pass
    migrate()
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# CHATBOT API — Endpoint khusus untuk chatbot external
# API Key terpisah: CHATBOT_API_KEY
# Base URL: /chatbot/tracking
# ═══════════════════════════════════════════════════════════════

CHATBOT_API_KEY = os.getenv("CHATBOT_API_KEY", "5cRtu21X6O1VHJbE2JVfcKinfSknxgTX56EPS5NIGuY")

def check_chatbot_key(request: Request):
    key = request.headers.get("x-chatbot-key") or request.query_params.get("chatbot_key")
    if key != CHATBOT_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: chatbot API key tidak valid")


@app.get("/chatbot/tracking")
def chatbot_tracking_simple(
    request: Request,
    status:       str  = "",
    plant:        str  = "",
    order:        str  = "",
    equipment:    str  = "",
    material:     str  = "",
    q:            str  = "",
    summary_only: bool = False,
    page:         int  = 1,
    limit:        int  = 50,
):
    """
    Jalur SEDERHANA — chatbot kirim filter, PRISMA yang query.
    Tidak perlu LLM generate SQL.

    Filter tersedia:
    - status: no-pr | pr-created | po-created | partial | complete
    - plant, order, equipment, material, q (search bebas)
    - summary_only=true → return ringkasan bukan detail baris
    - page, limit (max 200)

    Auth: header x-chatbot-key atau query param chatbot_key
    """
    check_chatbot_key(request)

    limit  = min(200, max(1, limit))
    offset = (page - 1) * limit

    conds, params = [], []

    if q:
        conds.append("""(
            t.material ILIKE %s OR t.material_description ILIKE %s
            OR t."order" ILIKE %s OR t.equipment ILIKE %s
            OR t.pr ILIKE %s OR t.reservno ILIKE %s
        )""")
        p = f"%{q}%"; params.extend([p]*6)

    if plant:
        conds.append("t.plant = %s"); params.append(plant)
    if order:
        conds.append('t."order" = %s'); params.append(order)
    if material:
        conds.append("t.material ILIKE %s"); params.append(f"%{material}%")
    if equipment:
        conds.append("t.equipment ILIKE %s"); params.append(f"%{equipment}%")

    status_cond = ""
    if status == "no-pr":
        status_cond = "AND (t.pr IS NULL OR t.pr = '')"
    elif status == "pr-created":
        status_cond = "AND (t.pr IS NOT NULL AND t.pr != '') AND (po_agg.po IS NULL OR po_agg.po = '')"
    elif status == "po-created":
        status_cond = "AND (po_agg.po IS NOT NULL AND po_agg.po != '') AND COALESCE(po_agg.qty_delivered,0) = 0"
    elif status == "partial":
        status_cond = "AND COALESCE(po_agg.qty_delivered,0) > 0 AND COALESCE(po_agg.qty_delivered,0) < COALESCE(po_agg.po_quantity,0)"
    elif status == "complete":
        status_cond = "AND COALESCE(po_agg.qty_delivered,0) >= COALESCE(po_agg.po_quantity,0) AND COALESCE(po_agg.po_quantity,0) > 0"

    sql_base = f"""
        FROM taex_reservasi t
        LEFT JOIN LATERAL (
            SELECT * FROM work_order wo WHERE wo."order" = t."order" LIMIT 1
        ) wo ON true
        LEFT JOIN LATERAL (
            SELECT sp.req_date FROM sap_pr sp
            WHERE sp.pr = t.pr AND (sp.d IS NULL OR sp.d = '') LIMIT 1
        ) sp ON true
        LEFT JOIN LATERAL (
            SELECT
                po.po, po.doc_date, po.deliv_date, po.crcy,
                SUM(po.po_quantity)   AS po_quantity,
                SUM(po.qty_delivered) AS qty_delivered,
                SUM(po.net_price)     AS net_price
            FROM sap_po po
            WHERE po.purchreq = t.pr AND (po.d IS NULL OR po.d = '')
            GROUP BY po.po, po.doc_date, po.deliv_date, po.crcy
            ORDER BY po.po LIMIT 1
        ) po_agg ON true
        WHERE t.material IS NOT NULL AND t.material != ''
        {("AND " + " AND ".join(conds)) if conds else ""}
        {status_cond}
    """

    def calc_status(r):
        has_pr  = bool(r.get("pr"))
        has_po  = bool(r.get("po_num"))
        qty_po  = float(r.get("po_quantity") or 0)
        qty_del = float(r.get("qty_delivered") or 0)
        if not has_pr:         return "no-pr"
        if not has_po:         return "pr-created"
        if qty_del <= 0:       return "po-created"
        if qty_del < qty_po:   return "partial"
        return "complete"

    if summary_only:
        summary_sql = f"""
            SELECT
                COUNT(*)                                                              AS total_material,
                COUNT(DISTINCT t."order")                                             AS total_order,
                COUNT(DISTINCT t.equipment)                                           AS total_equipment,
                SUM(CASE WHEN t.pr IS NULL OR t.pr='' THEN 1 ELSE 0 END)           AS no_pr,
                SUM(CASE WHEN t.pr IS NOT NULL AND t.pr!=''
                          AND (po_agg.po IS NULL OR po_agg.po='') THEN 1 ELSE 0 END) AS pr_created,
                SUM(CASE WHEN po_agg.po IS NOT NULL AND po_agg.po!=''
                          AND COALESCE(po_agg.qty_delivered,0)=0 THEN 1 ELSE 0 END)  AS po_created,
                SUM(CASE WHEN COALESCE(po_agg.qty_delivered,0)>0
                          AND COALESCE(po_agg.qty_delivered,0)<COALESCE(po_agg.po_quantity,0)
                         THEN 1 ELSE 0 END)                                           AS partial,
                SUM(CASE WHEN COALESCE(po_agg.qty_delivered,0)>=COALESCE(po_agg.po_quantity,0)
                          AND COALESCE(po_agg.po_quantity,0)>0 THEN 1 ELSE 0 END)    AS complete,
                COALESCE(SUM(t.qty_reqmts),0)                                         AS total_qty_reqmts,
                COALESCE(SUM(t.qty_pr),0)                                             AS total_qty_pr,
                COALESCE(SUM(po_agg.net_price),0)                                     AS total_nilai_po
            {sql_base}
        """
        s = query(summary_sql, params)[0]
        return jsonify({
            "mode": "summary",
            "filter": {"status":status,"plant":plant,"order":order,
                       "equipment":equipment,"material":material,"q":q},
            "summary": {
                "total_material":    int(s["total_material"] or 0),
                "total_order":       int(s["total_order"] or 0),
                "total_equipment":   int(s["total_equipment"] or 0),
                "no_pr":             int(s["no_pr"] or 0),
                "pr_created":        int(s["pr_created"] or 0),
                "po_created":        int(s["po_created"] or 0),
                "partial_delivery":  int(s["partial"] or 0),
                "complete":          int(s["complete"] or 0),
                "total_qty_reqmts":  float(s["total_qty_reqmts"] or 0),
                "total_qty_pr":      float(s["total_qty_pr"] or 0),
                "total_nilai_po_idr":float(s["total_nilai_po"] or 0),
            }
        })

    # Detail mode
    count_res = query(f"SELECT COUNT(*) AS c {sql_base}", params)
    total = int(count_res[0]["c"])

    data_sql = f"""
        SELECT
            t.plant, t.equipment, t."order" AS order_val,
            t.reservno, t.material, t.itm, t.material_description,
            t.qty_reqmts, t.qty_stock, t.pr, t.item AS pr_item, t.qty_pr,
            t.del, t.fis, t.ict, t.pg, t.reqmts_date, t.uom,
            wo.description AS order_desc, wo.system_status, wo.planner_group,
            wo.basic_start_date, wo.basic_finish_date,
            sp.req_date,
            po_agg.po AS po_num, po_agg.po_quantity, po_agg.qty_delivered,
            po_agg.deliv_date, po_agg.net_price, po_agg.crcy
        {sql_base}
        ORDER BY t.id
        LIMIT %s OFFSET %s
    """
    rows = query(data_sql, params + [limit, offset])

    data = []
    for r in rows:
        data.append({
            "plant":               r["plant"],
            "equipment":           r["equipment"],
            "order":               r["order_val"],
            "reservno":            r["reservno"],
            "material":            r["material"],
            "itm":                 r["itm"],
            "material_description":r["material_description"],
            "qty_reqmts":          _n(r["qty_reqmts"]),
            "qty_stock":           _n(r["qty_stock"]),
            "pr":                  r["pr"],
            "pr_item":             r["pr_item"],
            "qty_pr":              _n(r["qty_pr"]),
            "uom":                 r["uom"],
            "reqmts_date":         r["reqmts_date"],
            "order_desc":          r["order_desc"],
            "system_status":       r["system_status"],
            "planner_group":       r["planner_group"],
            "basic_start_date":    r["basic_start_date"],
            "basic_finish_date":   r["basic_finish_date"],
            "req_date":            r["req_date"],
            "po_num":              r["po_num"],
            "po_quantity":         _n(r["po_quantity"]),
            "qty_delivered":       _n(r["qty_delivered"]),
            "deliv_date":          r["deliv_date"],
            "net_price":           _n(r["net_price"]),
            "crcy":                r["crcy"],
            "status":              calc_status(r),
        })

    return jsonify({
        "mode": "detail",
        "filter": {"status":status,"plant":plant,"order":order,
                   "equipment":equipment,"material":material,"q":q},
        "pagination": {
            "page":page, "limit":limit,
            "total":total, "total_pages": max(1,-(-total//limit)),
        },
        "data": data,
    })


@app.post("/chatbot/query")
async def chatbot_query(request: Request):
    """
    Endpoint untuk chatbot mengirim query SQL dan mendapat hasilnya.

    Request body:
    {
        "sql": "SELECT material, qty_reqmts FROM taex_reservasi WHERE pr IS NULL LIMIT 10"
    }

    Auth: header x-chatbot-key atau query param chatbot_key

    Aturan keamanan:
    - Hanya SELECT yang diizinkan
    - Tabel yang boleh di-query: taex_reservasi, prisma_reservasi,
      kumpulan_summary, sap_pr, sap_po, work_order
    - LIMIT wajib ada, max 500 baris
    - Query berbahaya (DROP, DELETE, UPDATE, INSERT, TRUNCATE) ditolak
    """
    check_chatbot_key(request)

    body = await request.json()
    sql  = (body.get("sql") or "").strip()

    if not sql:
        raise HTTPException(400, "Body harus berisi field 'sql'")

    sql_upper = sql.upper()
    FORBIDDEN = ["DROP","DELETE","UPDATE","INSERT","TRUNCATE","ALTER","CREATE",
                 "GRANT","REVOKE","EXEC","EXECUTE","COPY","pg_","information_schema"]
    for word in FORBIDDEN:
        if word.upper() in sql_upper:
            raise HTTPException(403, f"Query tidak diizinkan: mengandung '{word}'")

    if not sql_upper.lstrip().startswith("SELECT"):
        raise HTTPException(403, "Hanya query SELECT yang diizinkan")

    ALLOWED_TABLES = {
        "taex_reservasi", "prisma_reservasi", "kumpulan_summary",
        "sap_pr", "sap_po", "work_order"
    }
    import re
    tables_in_query = set(re.findall(r'(?:FROM|JOIN)\s+([\w\"]+)', sql, re.IGNORECASE))
    tables_clean    = {t.strip('"').lower() for t in tables_in_query}
    disallowed      = tables_clean - ALLOWED_TABLES
    if disallowed:
        raise HTTPException(403, f"Tabel tidak diizinkan: {', '.join(disallowed)}")

    limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
    if not limit_match:
        raise HTTPException(400, "Query harus mengandung LIMIT (maksimal 500)")
    if int(limit_match.group(1)) > 500:
        raise HTTPException(400, "LIMIT maksimal 500 baris")

    try:
        rows = query(sql)
    except Exception as e:
        raise HTTPException(400, f"Query error: {str(e)}")

    import decimal, datetime as dt
    def clean(v):
        if isinstance(v, decimal.Decimal): return float(v)
        if isinstance(v, (dt.datetime, dt.date)): return str(v)
        return v

    data = [{k: clean(v) for k, v in dict(r).items()} for r in rows]

    return jsonify({
        "ok":      True,
        "sql":     sql,
        "rows":    len(data),
        "columns": list(data[0].keys()) if data else [],
        "data":    data,
    })


@app.get("/chatbot/schema")
def chatbot_schema(request: Request):
    """
    Fetch schema langsung dari PostgreSQL information_schema.
    Return nama kolom, tipe data, dan nullable untuk semua tabel yang diizinkan.
    Dipanggil chatbot sekali saat startup untuk build prompt otomatis.
    """
    check_chatbot_key(request)

    ALLOWED_TABLES = [
        "taex_reservasi", "prisma_reservasi", "kumpulan_summary",
        "sap_pr", "sap_po", "work_order"
    ]

    rows = query("""
        SELECT
            table_name, column_name, data_type,
            is_nullable, column_default, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
    """, (ALLOWED_TABLES,))

    tables = {}
    for r in rows:
        tbl = r["table_name"]
        if tbl not in tables:
            tables[tbl] = {"columns": [], "column_names": []}
        tables[tbl]["columns"].append({
            "name":     r["column_name"],
            "type":     r["data_type"],
            "nullable": r["is_nullable"] == "YES",
            "default":  r["column_default"],
        })
        tables[tbl]["column_names"].append(r["column_name"])

    TABLE_DESC = {
        "taex_reservasi":   "Data reservasi material TA-ex (sumber utama tracking procurement)",
        "prisma_reservasi": "Subset taex aktif (ict=L), berisi status kertas kerja dan PR",
        "kumpulan_summary": "Ringkasan kebutuhan material per kertas kerja (code_tracking)",
        "sap_pr":           "Purchase Request dari SAP (join ke taex via pr=pr)",
        "sap_po":           "Purchase Order dari SAP (join ke taex via purchreq=pr)",
        "work_order":       "Work Order SAP (join ke taex via order=order)",
    }

    result = {}
    for tbl in ALLOWED_TABLES:
        if tbl in tables:
            result[tbl] = {
                "description":  TABLE_DESC.get(tbl, ""),
                "columns":      tables[tbl]["columns"],
                "column_names": tables[tbl]["column_names"],
            }

    return jsonify({
        "allowed_tables": list(result.keys()),
        "tables":         result,
        "join_hints": {
            "taex_ke_workorder":  'taex_reservasi t JOIN work_order wo ON wo."order" = t."order"',
            "taex_ke_sap_pr":     "taex_reservasi t JOIN sap_pr sp ON sp.pr = t.pr",
            "taex_ke_sap_po":     "taex_reservasi t JOIN sap_po po ON po.purchreq = t.pr",
            "prisma_ke_kumpulan": "prisma_reservasi p JOIN kumpulan_summary k ON k.code_tracking = p.code_kertas_kerja AND k.material = p.material",
        },
        "status_logic": {
            "no-pr":      "pr IS NULL OR pr = ''",
            "pr-created": "pr IS NOT NULL AND pr != '' AND po belum ada",
            "po-created": "po ada AND qty_delivered = 0",
            "partial":    "qty_delivered > 0 AND qty_delivered < po_quantity",
            "complete":   "qty_delivered >= po_quantity AND po_quantity > 0",
        },
        "important_notes": [
            "Kolom 'order' adalah reserved word PostgreSQL — WAJIB ditulis dengan tanda kutip: \"order\"",
            "Selalu gunakan LIMIT maksimal 500",
            "Join PO ke taex: sap_po.purchreq = taex_reservasi.pr",
            "Join PR ke taex: sap_pr.pr = taex_reservasi.pr",
            "Join WO ke taex: work_order.\"order\" = taex_reservasi.\"order\"",
        ],
        "security": {
            "allowed_statements": ["SELECT only"],
            "max_limit":          500,
            "forbidden_keywords": ["DROP","DELETE","UPDATE","INSERT","TRUNCATE","ALTER","CREATE"],
        }
    })


# ═══════════════════════════════════════════════════════════════
# SPA FALLBACK
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# PROJECT
# ═══════════════════════════════════════════════════════════════
def map_project(r):
    return {
        "ID": r["id"], "ProjectNumber": r["project_number"],
        "ProjectTypeId": r["project_type_id"],
        "StartDate": r["start_date"], "FinishDate": r["finish_date"],
        "Revision": r["revision"], "Description": r["description"],
        "ProjectStatus": r["project_status"], "Plant": r["plant"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
        "Modified": r["modified"], "ModifiedBy": r["modified_by"],
        "DurationTaBrickId": r["duration_ta_brick_id"],
    }

@app.get("/api/project")
def get_project(request: Request, plant: str = None):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["is_deleted=0"], []
    if plant:
        clauses.append("plant=%s"); params.append(plant)
    else:
        pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM project WHERE {where} ORDER BY project_number", params)
    return jsonify([map_project(r) for r in rows])

@app.post("/api/project/replace")
async def replace_project(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    cnt = bulk_replace_project(df)
    return {"inserted": cnt}

@app.get("/api/project/{project_id}/full")
def get_project_full_moved(project_id: str, request: Request):
    return get_project_full(project_id, request)

@app.get("/api/project/{project_id}")
def get_project_by_id(project_id: str, request: Request):
    check_api_key(request)
    row = query("SELECT * FROM project WHERE id=%s", (project_id,))
    if not row:
        raise HTTPException(404, "Project tidak ditemukan")
    return jsonify(map_project(row[0]))

@app.delete("/api/project/{project_id}")
def delete_project(project_id: str, request: Request):
    check_api_key(request)
    execute("UPDATE project SET is_deleted=1 WHERE id=%s", (project_id,))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# JOB LIST
# ═══════════════════════════════════════════════════════════════
def map_job_list(r):
    return {
        "ID": r["id"], "ProjectId": r["project_id"],
        "EquipmentId": r["equipment_id"], "Plant": r["plant"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
        "Modified": r["modified"], "ModifiedBy": r["modified_by"],
        "JoblistDescription": r["joblist_description"],
        "NoJoblist": r["no_joblist"],
    }

@app.get("/api/joblist")
def get_job_list(request: Request, project_id: str = None, plant: str = None):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["is_deleted=0"], []
    if project_id:
        clauses.append("project_id=%s"); params.append(project_id)
    elif plant:
        clauses.append("plant=%s"); params.append(plant)
    else:
        pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM job_list WHERE {where} ORDER BY no_joblist", params)
    return jsonify([map_job_list(r) for r in rows])

@app.post("/api/joblist/replace")
async def replace_job_list(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    cnt = bulk_replace_job_list(df)
    return {"inserted": cnt}

@app.get("/api/joblist/{joblist_id}")
def get_job_list_by_id(joblist_id: str, request: Request):
    check_api_key(request)
    row = query("SELECT * FROM job_list WHERE id=%s", (joblist_id,))
    if not row:
        raise HTTPException(404, "Joblist tidak ditemukan")
    return jsonify(map_job_list(row[0]))

@app.delete("/api/joblist/{joblist_id}")
def delete_job_list(joblist_id: str, request: Request):
    check_api_key(request)
    execute("UPDATE job_list SET is_deleted=1 WHERE id=%s", (joblist_id,))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# JOB DETAIL
# ═══════════════════════════════════════════════════════════════
def map_job_detail(r):
    return {
        "ID": r["id"], "JoblistId": r["joblist_id"],
        "JoblistDetailReasonId": r["joblist_detail_reason_id"],
        "JoblistDetailDescription": r["joblist_detail_description"],
        "IsMechanicalIntegrity": r["is_mechanical_integrity"],
        "IsOptimization": r["is_optimization"],
        "JobDisciplineId": r["job_discipline_id"],
        "Plant": r["plant"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
        "Modified": r["modified"], "ModifiedBy": r["modified_by"],
        "NoDocument": r["no_document"],
        "CreatorJobTitle": r["creator_job_title"], "CreatorName": r["creator_name"],
        "AssignTo": r["assign_to"], "AuthparamArea": r["authparam_area"],
        "StatusId": r["status_id"],
        "IsOffStream": r["is_off_stream"],
        "NomorPM": r["nomor_pm"], "Collective": r["collective"],
        "Notes": r["notes"],
        "PICPlanner": r["pic_planner"], "PICPlannerName": r["pic_planner_name"],
        "IsAllIn": r["is_all_in"],
        "IsJasa": r["is_jasa"], "IsLLDII": r["is_lldii"], "IsMaterial": r["is_material"],
        "NoJoblistDetail": r["no_joblist_detail"],
        "IsRequestFreezing": r["is_request_freezing"],
        "PlanningStatusId": r["planning_status_id"],
        "PlanningMaterialStatusId": r["planning_material_status_id"],
        "PlanningJasaStatusId": r["planning_jasa_status_id"],
    }

@app.get("/api/jobdetail")
def get_job_detail(request: Request, joblist_id: str = None, plant: str = None,
                   page: int = 1, limit: int = 500):
    check_api_key(request)
    user = get_current_user(request)
    offset = (page - 1) * limit
    clauses, params = ["is_deleted=0"], []
    if joblist_id:
        clauses.append("joblist_id=%s"); params.append(joblist_id)
    elif plant:
        clauses.append("plant=%s"); params.append(plant)
    else:
        pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    where = " AND ".join(clauses)
    total = query(f"SELECT COUNT(*) AS n FROM job_detail WHERE {where}", params)[0]["n"]
    rows = query(
        f"SELECT * FROM job_detail WHERE {where} ORDER BY no_joblist_detail LIMIT %s OFFSET %s",
        params + [limit, offset]
    )
    return jsonify({"total": int(total), "page": page, "limit": limit,
                    "data": [map_job_detail(r) for r in rows]})

@app.post("/api/jobdetail/replace")
async def replace_job_detail(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    job_id = f"jobdetail_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    set_job(job_id, 0, "Membaca file Excel...")

    def _bg():
        try:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
            if df.empty:
                set_job(job_id, 100, "File kosong", True, "File Excel kosong")
                return
            set_job(job_id, 20, f"Parsed {len(df):,} baris. Menyimpan...")
            cnt = bulk_replace_job_detail(df)
            set_job(job_id, 100, f"✅ Selesai! {cnt:,} baris tersimpan", True)
        except Exception as e:
            set_job(job_id, 100, f"❌ {e}", True, str(e))

    threading.Thread(target=_bg, daemon=True).start()
    return {"jobId": job_id}

@app.get("/api/jobdetail/{detail_id}")
def get_job_detail_by_id(detail_id: str, request: Request):
    check_api_key(request)
    row = query("SELECT * FROM job_detail WHERE id=%s", (detail_id,))
    if not row:
        raise HTTPException(404, "Job detail tidak ditemukan")
    return jsonify(map_job_detail(row[0]))

@app.delete("/api/jobdetail/{detail_id}")
def delete_job_detail(detail_id: str, request: Request):
    check_api_key(request)
    execute("UPDATE job_detail SET is_deleted=1 WHERE id=%s", (detail_id,))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# JOB DETAIL WORK ORDER
# ═══════════════════════════════════════════════════════════════
def map_job_detail_work_order(r):
    return {
        "ID": r["id"], "JoblistDetailId": r["joblist_detail_id"],
        "Notification": r["notification"], "CreatedOn": r["created_on"],
        "SuperiorOrder": r["superior_order"], "Order": r["order"],
        "Description": r["description"], "Equipment": r["equipment"],
        "FunctionalLoc": r["functional_loc"], "Location": r["location"],
        "Revision": r["revision"],
        "SystemStatus": r["system_status"], "UserStatus": r["user_status"],
        "WBSordheader": r["wbs_ord_header"],
        "TotalPlnndCosts": _n(r["total_plnnd_costs"]),
        "Totalactcosts": _n(r["totalact_costs"]),
        "PlannerGroup": r["planner_group"], "MainWorkCtr": r["main_work_ctr"],
        "ChangeBy": r["change_by"],
        "Basstartdate": r["bas_start_date"], "Basicfindate": r["basic_fin_date"],
        "ActualRelease": r["actual_release"],
        "CostCenter": r["cost_center"], "EnteredBy": r["entered_by"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
        "Modified": r["modified"], "ModifiedBy": r["modified_by"],
    }

@app.get("/api/jobdetailworkorder")
def get_job_detail_work_order(request: Request,
                               joblist_detail_id: str = None,
                               order: str = None):
    check_api_key(request)
    if joblist_detail_id:
        rows = query(
            'SELECT * FROM job_detail_work_order WHERE is_deleted=0 AND joblist_detail_id=%s ORDER BY id',
            (joblist_detail_id,)
        )
    elif order:
        rows = query(
            'SELECT * FROM job_detail_work_order WHERE is_deleted=0 AND "order"=%s ORDER BY id',
            (order,)
        )
    else:
        rows = query('SELECT * FROM job_detail_work_order WHERE is_deleted=0 ORDER BY id')
    return jsonify([map_job_detail_work_order(r) for r in rows])

@app.post("/api/jobdetailworkorder/replace")
async def replace_job_detail_work_order(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    cnt = bulk_replace_job_detail_work_order(df)
    return {"inserted": cnt}

@app.delete("/api/jobdetailworkorder/{row_id}")
def delete_job_detail_work_order(row_id: str, request: Request):
    check_api_key(request)
    execute("UPDATE job_detail_work_order SET is_deleted=1 WHERE id=%s", (row_id,))
    return {"ok": True}

# ─── JOIN: Project → Joblist → JobDetail → WorkOrder ────────
@app.get("/api/project/{project_id}/full")
def get_project_full(project_id: str, request: Request):
    """
    Mengembalikan satu project beserta semua joblist, jobdetail,
    dan work order yang terkait — dalam satu response JSON terstruktur.
    """
    check_api_key(request)
    proj = query("SELECT * FROM project WHERE id=%s", (project_id,))
    if not proj:
        raise HTTPException(404, "Project tidak ditemukan")

    joblists = query(
        "SELECT * FROM job_list WHERE project_id=%s AND is_deleted=0 ORDER BY no_joblist",
        (project_id,)
    )
    result = map_project(proj[0])
    result["Joblists"] = []

    for jl in joblists:
        jl_data = map_job_list(jl)
        details = query(
            "SELECT * FROM job_detail WHERE joblist_id=%s AND is_deleted=0 ORDER BY no_joblist_detail",
            (jl["id"],)
        )
        jl_data["JobDetails"] = []
        for jd in details:
            jd_data = map_job_detail(jd)
            wos = query(
                "SELECT * FROM job_detail_work_order WHERE joblist_detail_id=%s AND is_deleted=0",
                (jd["id"],)
            )
            jd_data["WorkOrders"] = [map_job_detail_work_order(w) for w in wos]
            jl_data["JobDetails"].append(jd_data)
        result["Joblists"].append(jl_data)

    return jsonify(result)


@app.get("/api/jobdetail/summary")
def get_jobdetail_summary(request: Request, plant: str = None):
    """Summary jobdetail: count per collective, per status material/jasa"""
    check_api_key(request)
    plant_clause = "AND plant=%s" if plant else ""
    params = (plant,) if plant else ()
    rows = query(f"""
        SELECT
            collective,
            COUNT(*) AS total,
            SUM(CASE WHEN is_material=1 THEN 1 ELSE 0 END) AS total_material,
            SUM(CASE WHEN is_jasa=1 THEN 1 ELSE 0 END) AS total_jasa,
            SUM(CASE WHEN is_lldii=1 THEN 1 ELSE 0 END) AS total_lldii,
            SUM(CASE WHEN is_off_stream=1 THEN 1 ELSE 0 END) AS total_off_stream,
            SUM(CASE WHEN is_mechanical_integrity=1 THEN 1 ELSE 0 END) AS total_mi
        FROM job_detail
        WHERE is_deleted=0 {plant_clause}
        GROUP BY collective
        ORDER BY collective NULLS LAST
    """, params)
    return jsonify([{
        "Collective": r["collective"],
        "Total": int(r["total"]),
        "TotalMaterial": int(r["total_material"] or 0),
        "TotalJasa": int(r["total_jasa"] or 0),
        "TotalLLDII": int(r["total_lldii"] or 0),
        "TotalOffStream": int(r["total_off_stream"] or 0),
        "TotalMI": int(r["total_mi"] or 0),
    } for r in rows])


@app.get("/api/data/project")
def get_data_project(request: Request):
    check_api_key(request)
    rows = query("SELECT * FROM project WHERE is_deleted=0 ORDER BY project_number")
    return jsonify([map_project(r) for r in rows])

@app.get("/api/data/joblist")
def get_data_joblist(request: Request):
    check_api_key(request)
    rows = query("SELECT * FROM job_list WHERE is_deleted=0 ORDER BY no_joblist")
    return jsonify([map_job_list(r) for r in rows])

@app.get("/api/data/jobdetail")
def get_data_jobdetail(request: Request, page: int = 1, limit: int = 500):
    check_api_key(request)
    offset = (page - 1) * limit
    rows = query(
        "SELECT * FROM job_detail WHERE is_deleted=0 ORDER BY no_joblist_detail LIMIT %s OFFSET %s",
        (limit, offset)
    )
    return jsonify([map_job_detail(r) for r in rows])

@app.get("/api/data/jobdetailworkorder")
def get_data_jdwo(request: Request):
    check_api_key(request)
    rows = query("SELECT * FROM job_detail_work_order WHERE is_deleted=0 ORDER BY id")
    return jsonify([map_job_detail_work_order(r) for r in rows])


# ═══════════════════════════════════════════════════════════════
# EQUIPMENT TAEX
# ═══════════════════════════════════════════════════════════════
def map_equipment(r):
    return {
        "ID": r["id"], "Plant": r["plant"],
        "UnitId": r["unit_id"],
        "EquipmentNo": r["equipment_no"],
        "DescriptionofTechnicalObject": r["description_of_technical_object"],
        "FunctionalLocation": r["functional_location"],
        "Location": r["location"],
        "Disiplin": r["disiplin"],
        "EquipmentCategory": r["equipment_category"],
        "GroupAsset": r["group_asset"],
        "Criticallity": r["criticallity"],
        "CriticallityText": r["criticallity_text"],
        "CatalogProfile": r["catalog_profile"],
        "CatalogProfileText": r["catalog_profile_text"],
        "MainWorkCenter": r["main_work_center"],
        "MaintenancePlant": r["maintenance_plant"],
        "PlanningPlant": r["planning_plant"],
        "ModelType": r["model_type"],
        "ManufacturerOfAsset": r["manufacturer_of_asset"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
        "Modified": r["modified"], "ModifiedBy": r["modified_by"],
    }

@app.get("/api/equipment")
def get_equipment(request: Request, plant: str = None, disiplin: str = None,
                  q: str = None, page: int = 1, limit: int = 500):
    check_api_key(request)
    user = get_current_user(request)
    clauses = ["is_deleted=0"]
    params = []
    if plant:
        clauses.append("plant=%s"); params.append(plant)
    else:
        pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    if disiplin:
        clauses.append("disiplin=%s"); params.append(disiplin)
    if q:
        clauses.append("(equipment_no ILIKE %s OR description_of_technical_object ILIKE %s OR functional_location ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
    where = " AND ".join(clauses)
    offset = (page - 1) * limit
    total = query(f"SELECT COUNT(*) AS n FROM equipment_taex WHERE {where}", params)[0]["n"]
    rows = query(f"SELECT * FROM equipment_taex WHERE {where} ORDER BY equipment_no LIMIT %s OFFSET %s",
                 params + [limit, offset])
    return jsonify({"total": total, "page": page, "limit": limit,
                    "data": [map_equipment(r) for r in rows]})

@app.get("/api/equipment/meta/filters")
def equipment_meta(request: Request):
    check_api_key(request)
    plants    = query("SELECT DISTINCT plant FROM equipment_taex WHERE is_deleted=0 AND plant IS NOT NULL ORDER BY plant")
    disiplins = query("SELECT DISTINCT disiplin FROM equipment_taex WHERE is_deleted=0 AND disiplin IS NOT NULL ORDER BY disiplin")
    groups    = query("SELECT DISTINCT group_asset FROM equipment_taex WHERE is_deleted=0 AND group_asset IS NOT NULL ORDER BY group_asset")
    crits     = query("SELECT DISTINCT criticallity_text FROM equipment_taex WHERE is_deleted=0 AND criticallity_text IS NOT NULL ORDER BY criticallity_text")
    return jsonify({
        "plants":    [r["plant"] for r in plants],
        "disiplins": [r["disiplin"] for r in disiplins],
        "groups":    [r["group_asset"] for r in groups],
        "criticallities": [r["criticallity_text"] for r in crits],
    })

@app.post("/api/equipment/replace")
async def replace_equipment(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    job_id = f"equipment_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    set_job(job_id, 0, "Membaca file Excel...")

    def _bg():
        try:
            df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
            if df.empty:
                set_job(job_id, 100, "File kosong", True, "File Excel kosong"); return
            set_job(job_id, 20, f"Parsed {len(df):,} baris. Menyimpan...")
            cnt = bulk_replace_equipment_taex(df)
            set_job(job_id, 100, f"✅ Selesai! {cnt:,} baris tersimpan", True)
        except Exception as e:
            set_job(job_id, 100, f"❌ {e}", True, str(e))

    threading.Thread(target=_bg, daemon=True).start()
    return {"jobId": job_id}

@app.get("/api/equipment/{eq_id}")
def get_equipment_by_id(eq_id: str, request: Request):
    check_api_key(request)
    row = query("SELECT * FROM equipment_taex WHERE id=%s", (eq_id,))
    if not row:
        raise HTTPException(404, "Equipment tidak ditemukan")
    return jsonify(map_equipment(row[0]))

@app.delete("/api/equipment/{eq_id}")
def delete_equipment(eq_id: str, request: Request):
    check_api_key(request)
    execute("UPDATE equipment_taex SET is_deleted=1 WHERE id=%s", (eq_id,))
    return {"ok": True}

@app.get("/api/data/equipment")
def get_data_equipment(request: Request, page: int = 1, limit: int = 500):
    check_api_key(request)
    offset = (page - 1) * limit
    total = query("SELECT COUNT(*) AS n FROM equipment_taex WHERE is_deleted=0")[0]["n"]
    rows = query("SELECT * FROM equipment_taex WHERE is_deleted=0 ORDER BY equipment_no LIMIT %s OFFSET %s",
                 (limit, offset))
    return jsonify({"total": total, "page": page, "limit": limit,
                    "data": [map_equipment(r) for r in rows]})


# ═══════════════════════════════════════════════════════════════
# JOB AREA
# ═══════════════════════════════════════════════════════════════
def map_area(r):
    return {
        "ID": r["id"], "AreaName": r["area_name"], "Plant": r["plant"],
        "AreaAliasName": r["area_alias_name"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
    }

@app.get("/api/jobarea")
def get_job_area(request: Request, plant: str = None):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["is_deleted=0"], []
    if plant:
        clauses.append("plant=%s"); params.append(plant)
    else:
        pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM job_area WHERE {where} ORDER BY area_name", params)
    return jsonify([map_area(r) for r in rows])

@app.post("/api/jobarea/replace")
async def replace_job_area(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    cnt = bulk_replace_job_area(df)
    return {"inserted": cnt}


# ═══════════════════════════════════════════════════════════════
# JOB UNIT
# ═══════════════════════════════════════════════════════════════
def map_unit(r):
    return {
        "ID": r["id"], "AreaId": r["area_id"],
        "UnitName": r["unit_name"], "Plant": r["plant"],
        "UnitAliasName": r["unit_alias_name"],
        "Created": r["created"], "CreatedBy": r["created_by"],
        "IsDeleted": r["is_deleted"],
    }

@app.get("/api/jobunit")
def get_job_unit(request: Request, plant: str = None, area_id: str = None):
    check_api_key(request)
    user = get_current_user(request)
    clauses, params = ["is_deleted=0"], []
    if area_id:
        clauses.append("area_id=%s"); params.append(area_id)
    elif plant:
        clauses.append("plant=%s"); params.append(plant)
    else:
        pc, pp = plant_clause(user, "plant"); clauses.append(pc); params.extend(pp)
    where = " AND ".join(clauses)
    rows = query(f"SELECT * FROM job_unit WHERE {where} ORDER BY unit_name", params)
    return jsonify([map_unit(r) for r in rows])

@app.post("/api/jobunit/replace")
async def replace_job_unit(request: Request, file: UploadFile = File(...)):
    check_api_key(request)
    content = await file.read()
    df = pd.read_excel(io.BytesIO(content), dtype=str, keep_default_na=False)
    cnt = bulk_replace_job_unit(df)
    return {"inserted": cnt}


# ═══════════════════════════════════════════════════════════════
# TRACKING JOBLIST (updated with area + unit JOIN)
# ═══════════════════════════════════════════════════════════════
@app.get("/api/tracking-joblist")
def get_tracking_joblist(
    request: Request,
    project: str = None,
    area: str = None,
    unit: str = None,
    collective: str = None,
    status: str = None,
    disiplin: str = None,
    q: str = None,
):
    check_api_key(request)
    user = get_current_user(request)

    clauses = ["wo.is_deleted = 0"]
    params  = []
    pc, pp = plant_clause(user, "jl.plant"); clauses.append(pc); params.extend(pp)
    gc, gp = pg_clause(user, "wo.planner_group"); clauses.append(gc); params.extend(gp)

    if project:
        clauses.append("p.project_number = %s"); params.append(project)
    if area:
        clauses.append("a.id = %s"); params.append(area)
    if unit:
        clauses.append("u.id = %s"); params.append(unit)
    if collective:
        clauses.append("jd.collective = %s"); params.append(collective)
    if status:
        clauses.append("wo.system_status ILIKE %s"); params.append(f"%{status}%")
    if disiplin:
        clauses.append("eq.disiplin = %s"); params.append(disiplin)
    if q:
        clauses.append("""(
            wo."order" ILIKE %s OR wo.equipment ILIKE %s OR
            jd.no_joblist_detail ILIKE %s OR jd.joblist_detail_description ILIKE %s OR
            jl.no_joblist ILIKE %s OR p.project_number ILIKE %s OR
            a.area_name ILIKE %s OR u.unit_name ILIKE %s OR
            eq.equipment_no ILIKE %s OR eq.description_of_technical_object ILIKE %s
        )""")
        params.extend([f"%{q}%"] * 10)

    where = " AND ".join(clauses)

    rows = query(f"""
        SELECT
            -- WO Detail
            wo.id                         AS wo_id,
            wo."order"                    AS "order",
            wo.notification,
            wo.superior_order,
            wo.system_status,
            wo.user_status,
            wo.total_plnnd_costs,
            wo.totalact_costs,
            wo.bas_start_date,
            wo.basic_fin_date,
            wo.actual_release,
            wo.wbs_ord_header,
            wo.planner_group,
            wo.main_work_ctr,
            wo.cost_center,
            wo.revision                   AS wo_revision,

            -- Job Detail
            jd.id                         AS jd_id,
            jd.no_joblist_detail,
            jd.joblist_detail_description AS jd_desc,
            jd.collective,
            jd.nomor_pm,
            jd.is_mechanical_integrity,
            jd.is_off_stream,
            jd.is_material,
            jd.is_jasa,
            jd.is_lldii,
            jd.status_id                  AS jd_status_id,
            jd.planning_status_id,
            jd.planning_material_status_id,
            jd.planning_jasa_status_id,
            jd.no_document,
            jd.notes                      AS jd_notes,
            jd.pic_planner_name,
            jd.authparam_area,

            -- Job List
            jl.id                         AS jl_id,
            jl.no_joblist,
            jl.joblist_description        AS jl_desc,
            jl.plant,

            -- Project
            p.id                          AS project_id,
            p.project_number,
            p.description                 AS project_desc,
            p.project_status,
            p.start_date                  AS project_start,
            p.finish_date,
            p.revision                    AS project_revision,

            -- Equipment
            eq.id                         AS eq_id,
            eq.equipment_no,
            eq.description_of_technical_object AS equipment_desc,
            eq.functional_location,
            eq.location                   AS eq_location,
            eq.disiplin,
            eq.criticallity_text,
            eq.group_asset,
            eq.main_work_center           AS eq_main_work_center,
            eq.catalog_profile_text,
            eq.unit_id                    AS eq_unit_id,

            -- Unit
            u.id                          AS unit_id,
            u.unit_name,
            u.unit_alias_name,

            -- Area
            a.id                          AS area_id,
            a.area_name,
            a.area_alias_name

        FROM job_detail_work_order wo
        LEFT JOIN job_detail       jd  ON wo.joblist_detail_id = jd.id
        LEFT JOIN job_list         jl  ON jd.joblist_id        = jl.id
        LEFT JOIN project          p   ON jl.project_id        = p.id
        LEFT JOIN equipment_taex   eq  ON jl.equipment_id      = eq.id
        LEFT JOIN job_unit         u   ON eq.unit_id           = u.id
        LEFT JOIN job_area         a   ON u.area_id            = a.id
        WHERE {where}
        ORDER BY a.area_name, u.unit_name, eq.equipment_no,
                 p.project_number, jl.no_joblist, jd.no_joblist_detail, wo."order"
    """, params)

    # ── Readiness calculation ─────────────────────────────────
    orders = list({r["order"] for r in rows if r["order"]})
    order_readiness = {}

    if orders:
        placeholders = ",".join(["%s"] * len(orders))
        mat_rows = query(f"""
            SELECT "order",
                   COUNT(*) AS total_mat,
                   SUM(CASE WHEN COALESCE(qty_deliv,0) >= qty_reqmts AND qty_reqmts > 0 THEN 1 ELSE 0 END) AS ready_mat
            FROM taex_reservasi
            WHERE "order" IN ({placeholders})
            GROUP BY "order"
        """, orders)
        for m in mat_rows:
            total = int(m["total_mat"] or 0)
            ready = int(m["ready_mat"] or 0)
            order_readiness[m["order"]] = {
                "order_ready": total > 0 and ready == total,
                "order_readiness_pct": round(ready / total * 100, 1) if total > 0 else 0,
                "order_total_mat": total,
                "order_ready_mat": ready,
            }
    for o in orders:
        if o not in order_readiness:
            order_readiness[o] = {"order_ready": False, "order_readiness_pct": 0,
                                  "order_total_mat": 0, "order_ready_mat": 0}

    from collections import defaultdict
    jd_orders  = defaultdict(list)
    jl_jds     = defaultdict(set)
    eq_jls     = defaultdict(set)
    unit_eqs   = defaultdict(set)
    area_units = defaultdict(set)
    proj_jls   = defaultdict(set)

    for r in rows:
        if r["jd_id"]:  jd_orders[r["jd_id"]].append(r["order"])
        if r["jl_id"] and r["jd_id"]:  jl_jds[r["jl_id"]].add(r["jd_id"])
        if r["eq_id"] and r["jl_id"]:  eq_jls[r["eq_id"]].add(r["jl_id"])
        if r["unit_id"] and r["eq_id"]: unit_eqs[r["unit_id"]].add(r["eq_id"])
        if r["area_id"] and r["unit_id"]: area_units[r["area_id"]].add(r["unit_id"])
        if r["project_id"] and r["jl_id"]: proj_jls[r["project_id"]].add(r["jl_id"])

    def _pct(ready, total):
        return round(ready / total * 100, 1) if total > 0 else 0

    def jd_ready_info(jd_id):
        os = jd_orders.get(jd_id, [])
        if not os: return False, 0
        rc = sum(1 for o in os if order_readiness.get(o, {}).get("order_ready"))
        return rc == len(os), _pct(rc, len(os))

    def jl_ready_info(jl_id):
        jds = jl_jds.get(jl_id, set())
        if not jds: return False, 0
        rc = sum(1 for jd in jds if jd_ready_info(jd)[0])
        return rc == len(jds), _pct(rc, len(jds))

    def eq_ready_info(eq_id):
        jls = eq_jls.get(eq_id, set())
        if not jls: return False, 0
        rc = sum(1 for jl in jls if jl_ready_info(jl)[0])
        return rc == len(jls), _pct(rc, len(jls))

    def unit_ready_info(unit_id):
        eqs = unit_eqs.get(unit_id, set())
        if not eqs: return False, 0
        rc = sum(1 for eq in eqs if eq_ready_info(eq)[0])
        return rc == len(eqs), _pct(rc, len(eqs))

    def area_ready_info(area_id):
        units = area_units.get(area_id, set())
        if not units: return False, 0
        rc = sum(1 for u in units if unit_ready_info(u)[0])
        return rc == len(units), _pct(rc, len(units))

    def proj_ready_info(project_id):
        jls = proj_jls.get(project_id, set())
        if not jls: return False, 0
        rc = sum(1 for jl in jls if jl_ready_info(jl)[0])
        return rc == len(jls), _pct(rc, len(jls))

    result = []
    for r in rows:
        d = dict(r)
        o = d.get("order")
        d.update(order_readiness.get(o, {"order_ready": False, "order_readiness_pct": 0,
                                         "order_total_mat": 0, "order_ready_mat": 0}))
        jd_r, jd_p = jd_ready_info(d.get("jd_id")) if d.get("jd_id") else (False, 0)
        d["jd_ready"] = jd_r; d["jd_readiness_pct"] = jd_p

        jl_r, jl_p = jl_ready_info(d.get("jl_id")) if d.get("jl_id") else (False, 0)
        d["jl_ready"] = jl_r; d["jl_readiness_pct"] = jl_p

        eq_r, eq_p = eq_ready_info(d.get("eq_id")) if d.get("eq_id") else (False, 0)
        d["eq_ready"] = eq_r; d["eq_readiness_pct"] = eq_p

        u_r, u_p = unit_ready_info(d.get("unit_id")) if d.get("unit_id") else (False, 0)
        d["unit_ready"] = u_r; d["unit_readiness_pct"] = u_p

        a_r, a_p = area_ready_info(d.get("area_id")) if d.get("area_id") else (False, 0)
        d["area_ready"] = a_r; d["area_readiness_pct"] = a_p

        pr_r, pr_p = proj_ready_info(d.get("project_id")) if d.get("project_id") else (False, 0)
        d["project_ready"] = pr_r; d["project_readiness_pct"] = pr_p

        result.append(d)

    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
# TRACKING JOBLIST 2 — berbasis vw_joblist_wo + vw_joblist_detail
# Join via equipment_no, readiness via sap_po (sama seperti tab Tracking)
# ═══════════════════════════════════════════════════════════════

def _build_trkjl2_query(user, page=None, limit=None,
                         q="", project="", area="", unit="",
                         disiplin="", status="", known_total=0):
    """Helper: bangun WHERE + params, return (where, params, total_if_needed)."""
    clauses = []
    params  = []

    pc, pp = plant_clause(user, "wo.plant"); clauses.append(pc); params.extend(pp)

    if q:
        clauses.append("""(
            wo."order" ILIKE %s OR wo.equipment_no ILIKE %s OR
            jld.no_joblist ILIKE %s OR jld.joblist_detail_desc ILIKE %s OR
            jld.project_number ILIKE %s OR jld.area_name ILIKE %s OR
            jld.unit_name ILIKE %s OR jld.joblist_description ILIKE %s
        )""")
        params.extend([f"%{q}%"] * 8)
    if project:
        clauses.append("jld.project_number = %s"); params.append(project)
    if area:
        clauses.append("jld.area_name = %s"); params.append(area)
    if unit:
        clauses.append("jld.unit_name = %s"); params.append(unit)
    if disiplin:
        clauses.append("COALESCE(jld.disiplin, wo.disiplin) = %s"); params.append(disiplin)
    if status:
        clauses.append("wo.system_status ILIKE %s"); params.append(f"%{status}%")

    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


SELECT_TRK2 = """
    SELECT
        wo.id AS wo_id, wo."order", wo.notification,
        wo.system_status, wo.user_status,
        wo.total_plnnd_costs, wo.totalact_costs,
        wo.planner_group, wo.main_work_ctr, wo.wbs_ord_header,
        wo.bas_start_date, wo.basic_fin_date, wo.actual_release,
        wo.cost_center, wo.plant,
        wo.equipment_no AS wo_equipment_no,
        wo.disiplin     AS wo_disiplin,
        wo.joblist_description AS wo_joblist_description,
        wo.revision     AS wo_revision,
        wo.planning_jasa_status     AS wo_planning_jasa,
        wo.planning_material_status AS wo_planning_material,
        wo.code_name    AS wo_code_name,
        wo.is_lldii     AS wo_is_lldii,
        jld.id          AS jld_id,
        jld.joblist_detail_desc, jld.reason_name,
        jld.doc_type_name, jld.no_document,
        jld.is_mechanical_integrity, jld.job_discipline_name,
        jld.nomor_pm, jld.notes, jld.creator_name,
        jld.joblist_description, jld.no_joblist,
        jld.project_number, jld.project_type_code, jld.project_status,
        jld.start_date AS project_start, jld.finish_date AS project_finish,
        jld.revision, jld.description AS project_desc,
        jld.equipment_no, jld.area_name, jld.area_alias_name,
        jld.unit_name, jld.unit_alias_name,
        jld.functional_location, jld.location,
        jld.disiplin, jld.criticallity_text, jld.main_work_center,
        jld.is_jasa, jld.is_lldii, jld.is_material,
        jld.code_name AS jld_code_name,
        jld.planning_jasa_status     AS jld_planning_jasa,
        jld.planning_material_status AS jld_planning_material
    FROM vw_joblist_wo wo
    LEFT JOIN vw_joblist_detail jld ON jld.equipment_no = wo.equipment_no
"""

ORDER_TRK2 = """
    ORDER BY jld.area_name, jld.unit_name, wo.equipment_no,
             jld.project_number, jld.no_joblist, jld.joblist_detail_desc, wo."order"
"""


def _attach_readiness(rows):
    """Attach order readiness dari sap_po ke tiap baris."""
    orders = list({r["order"] for r in rows if r["order"]})
    order_readiness = {}
    if orders:
        ph = ",".join(["%s"] * len(orders))
        mat_rows = query(f"""
            SELECT t."order",
                   COUNT(*) AS total_mat,
                   SUM(CASE
                       WHEN COALESCE(po.po_quantity,0) > 0
                        AND COALESCE(po.po_qty_delivered,0) >= po.po_quantity
                       THEN 1 ELSE 0 END) AS ready_mat
            FROM taex_reservasi t
            LEFT JOIN LATERAL (
                SELECT po.po_quantity, po.qty_delivered AS po_qty_delivered
                FROM sap_po po
                WHERE po.po = t.po AND po.material = t.material
                ORDER BY po.id LIMIT 1
            ) po ON TRUE
            WHERE t."order" IN ({ph})
            GROUP BY t."order"
        """, orders)
        for m in mat_rows:
            tot = int(m["total_mat"] or 0)
            rdy = int(m["ready_mat"] or 0)
            order_readiness[m["order"]] = {
                "order_ready":         tot > 0 and rdy == tot,
                "order_readiness_pct": round(rdy / tot * 100, 1) if tot > 0 else 0,
                "order_total_mat":     tot,
                "order_ready_mat":     rdy,
            }
    result = []
    for r in rows:
        d = dict(r)
        rd = order_readiness.get(r["order"], {}) if r["order"] else {}
        d["order_ready"]         = rd.get("order_ready", False)
        d["order_readiness_pct"] = rd.get("order_readiness_pct", 0)
        d["order_total_mat"]     = rd.get("order_total_mat", 0)
        d["order_ready_mat"]     = rd.get("order_ready_mat", 0)
        result.append(d)
    return result


@app.get("/api/tracking-joblist2")
def get_tracking_joblist2(
    request: Request,
    page: int = 1, limit: int = 100,
    q: str = "", project: str = "", area: str = "", unit: str = "",
    disiplin: str = "", status: str = "", known_total: int = 0,
):
    check_api_key(request)
    user   = get_current_user(request)
    limit  = min(5000, max(1, limit))
    offset = (page - 1) * limit

    where, params = _build_trkjl2_query(user, q=q, project=project, area=area,
                                         unit=unit, disiplin=disiplin, status=status)
    total = known_total
    if not total:
        total = int(query(
            f"SELECT COUNT(*) AS c FROM vw_joblist_wo wo "
            f"LEFT JOIN vw_joblist_detail jld ON jld.equipment_no = wo.equipment_no "
            f"WHERE {where}", params
        )[0]["c"])

    rows = query(
        f"{SELECT_TRK2} WHERE {where} {ORDER_TRK2} LIMIT %s OFFSET %s",
        params + [limit, offset]
    )
    data = _attach_readiness(rows)

    return jsonify({
        "data": data,
        "pagination": {
            "page": page, "limit": limit, "total": total,
            "totalPages": max(1, -(-total // limit)),
            "hasMore": offset + limit < total,
        },
    })


@app.get("/api/tracking-joblist2/all")
def get_tracking_joblist2_all(
    request: Request,
    q: str = "", project: str = "", area: str = "", unit: str = "",
    disiplin: str = "", status: str = "",
):
    """Get all rows (tanpa pagination) — untuk keperluan export Excel."""
    check_api_key(request)
    user  = get_current_user(request)
    where, params = _build_trkjl2_query(user, q=q, project=project, area=area,
                                         unit=unit, disiplin=disiplin, status=status)
    rows = query(f"{SELECT_TRK2} WHERE {where} {ORDER_TRK2}", params)
    return jsonify(_attach_readiness(rows))


# ═══════════════════════════════════════════════════════════════
# AUTH ENDPOINTS
# ═══════════════════════════════════════════════════════════════
@app.get("/api/auth/reset-admin")
def reset_admin():
    pw_hash = _hash_password("Admin@123")
    existing = query_one("SELECT id FROM users WHERE username='admin'")
    if existing:
        execute("UPDATE users SET password_hash=%s, is_active=TRUE WHERE username='admin'", (pw_hash,))
        return {"ok": True, "message": "Password admin direset ke Admin@123"}
    else:
        execute(
            "INSERT INTO users (username, password_hash, plant_code, pg_role, is_admin) VALUES ('admin',%s,NULL,'Admin',TRUE)",
            (pw_hash,)
        )
        return {"ok": True, "message": "Admin dibuat dengan password Admin@123"}

@app.post("/api/auth/login")
async def login(request: Request):
    try:
        body = await request.json()
        username = body.get("username", "").strip()
        password = body.get("password", "")
        if not username or not password:
            raise HTTPException(400, "Username dan password wajib diisi")
        user = query_one("SELECT * FROM users WHERE username=%s AND is_active=TRUE", (username,))
        if not user or not _verify_password(password, user["password_hash"]):
            raise HTTPException(401, "Username atau password salah")
        token = _create_session(user["id"])
        return jsonify({
            "token": token,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "plant_code": user["plant_code"],
                "pg_role": user["pg_role"],
                "is_admin": user["is_admin"],
            }
        })
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Login error: {e}")
        raise HTTPException(500, f"Server error: {str(e)}")

@app.post("/api/auth/logout")
def logout(request: Request):
    token = request.headers.get("x-auth-token", "")
    if token:
        execute("DELETE FROM user_sessions WHERE token=%s", (token,))
    return {"ok": True}

@app.get("/api/auth/me")
def me(request: Request):
    user = get_current_user(request)
    return jsonify(user)

# ═══════════════════════════════════════════════════════════════
# ADMIN — PLANT MASTER
# ═══════════════════════════════════════════════════════════════
@app.get("/api/admin/plants")
def admin_get_plants(request: Request):
    require_admin(request)
    rows = query("SELECT * FROM plants ORDER BY plant_code")
    return jsonify([dict(r) for r in rows])

@app.post("/api/admin/plants")
async def admin_create_plant(request: Request):
    require_admin(request)
    body = await request.json()
    code = body.get("plant_code", "").strip().upper()
    name = body.get("plant_name", "").strip()
    if not code:
        raise HTTPException(400, "plant_code wajib diisi")
    execute(
        "INSERT INTO plants (plant_code, plant_name) VALUES (%s,%s) ON CONFLICT (plant_code) DO UPDATE SET plant_name=%s",
        (code, name, name)
    )
    return {"ok": True, "plant_code": code}

@app.delete("/api/admin/plants/{code}")
def admin_delete_plant(code: str, request: Request):
    require_admin(request)
    execute("DELETE FROM plants WHERE plant_code=%s", (code,))
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# ADMIN — USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════
@app.get("/api/admin/users")
def admin_get_users(request: Request):
    require_admin(request)
    rows = query("SELECT id,username,plant_code,pg_role,is_admin,is_active,created_at FROM users ORDER BY created_at DESC")
    return jsonify([dict(r) for r in rows])

@app.post("/api/admin/users")
async def admin_create_user(request: Request):
    require_admin(request)
    body     = await request.json()
    username = body.get("username", "").strip()
    password = body.get("password", "").strip()
    plant    = body.get("plant_code") or None
    pg_role  = body.get("pg_role", "TA")
    is_admin = bool(body.get("is_admin", False))
    if not username or not password:
        raise HTTPException(400, "username dan password wajib")
    if len(password) < 6:
        raise HTTPException(400, "Password minimal 6 karakter")
    pw_hash = _hash_password(password)
    try:
        execute(
            "INSERT INTO users (username,password_hash,plant_code,pg_role,is_admin) VALUES (%s,%s,%s,%s,%s)",
            (username, pw_hash, plant, pg_role, is_admin)
        )
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(400, "Username sudah digunakan")
        raise e
    return {"ok": True}

@app.put("/api/admin/users/{user_id}")
async def admin_update_user(user_id: int, request: Request):
    require_admin(request)
    body   = await request.json()
    sets   = []; params = []
    if "plant_code" in body:
        sets.append("plant_code=%s"); params.append(body["plant_code"] or None)
    if "pg_role" in body:
        sets.append("pg_role=%s"); params.append(body["pg_role"])
    if "is_admin" in body:
        sets.append("is_admin=%s"); params.append(bool(body["is_admin"]))
    if "is_active" in body:
        sets.append("is_active=%s"); params.append(bool(body["is_active"]))
    if body.get("password"):
        if len(body["password"]) < 6:
            raise HTTPException(400, "Password minimal 6 karakter")
        sets.append("password_hash=%s"); params.append(_hash_password(body["password"]))
    if not sets:
        raise HTTPException(400, "Tidak ada field yang diupdate")
    params.append(user_id)
    execute(f"UPDATE users SET {', '.join(sets)} WHERE id=%s", params)
    if body.get("is_active") == False:
        execute("DELETE FROM user_sessions WHERE user_id=%s", (user_id,))
    return {"ok": True}

@app.delete("/api/admin/users/{user_id}")
def admin_delete_user(user_id: int, request: Request):
    require_admin(request)
    execute("DELETE FROM users WHERE id=%s AND is_admin=FALSE", (user_id,))
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# PUBLIC API — TRACKING & TRACKING JOBLIST
# Akses dengan header: x-api-key: <PUBLIC_API_KEY>
# atau query param:    ?api_key=<PUBLIC_API_KEY>
# ═══════════════════════════════════════════════════════════════

@app.get("/api/public/tracking")
def public_tracking(
    request: Request,
    page: int = 1,
    limit: int = 99999,
    q: str = "",
    order_val: str = "",
    material: str = "",
    pr: str = "",
    po: str = "",
    status: str = "",
    plant: str = "",
    order_by: str = "t.id",
    order_dir: str = "ASC",
):
    """
    Public API — Tracking material (TA-ex Reservasi + PR/PO).

    **Auth:** Header `x-api-key` atau query `?api_key=`

    **Filter params:**
    - `q` — pencarian bebas (order, material, equipment)
    - `order_val` — filter by order number
    - `material` — filter by material number
    - `pr` — filter by PR number
    - `po` — filter by PO number
    - `status` — `with_pr`, `without_pr`, `with_po`, `without_po`
    - `plant` — filter by plant code
    - `page`, `limit` — pagination (max limit 500)
    - `order_by`, `order_dir` — sorting
    """
    check_public_api_key(request)

    # Delegate ke fungsi tracking internal
    from fastapi import Request as FRequest
    clauses = ["1=1"]
    params  = []

    if plant:
        clauses.append("t.plant = %s"); params.append(plant)
    if order_val:
        clauses.append('t."order" ILIKE %s'); params.append(f"%{order_val}%")
    if material:
        clauses.append("t.material ILIKE %s"); params.append(f"%{material}%")
    if pr:
        clauses.append("t.pr ILIKE %s"); params.append(f"%{pr}%")
    if po:
        clauses.append("t.po ILIKE %s"); params.append(f"%{po}%")
    if q:
        clauses.append("""(t."order" ILIKE %s OR t.material ILIKE %s
            OR t.material_description ILIKE %s OR t.equipment ILIKE %s)""")
        params.extend([f"%{q}%"]*4)
    if status == "with_pr":
        clauses.append("t.pr IS NOT NULL AND t.pr != ''")
    elif status == "without_pr":
        clauses.append("(t.pr IS NULL OR t.pr = '')")
    elif status == "with_po":
        clauses.append("t.po IS NOT NULL AND t.po != ''")
    elif status == "without_po":
        clauses.append("(t.po IS NULL OR t.po = '')")

    safe_cols = {"t.id","t.plant","t.equipment","t.order","t.material",
                 "t.qty_reqmts","t.qty_stock","t.pr","t.po","t.qty_deliv"}
    if order_by not in safe_cols:
        order_by = "t.id"
    order_dir = "DESC" if order_dir.upper() == "DESC" else "ASC"

    where  = " AND ".join(clauses)
    offset = (page - 1) * limit
    total  = query(f'SELECT COUNT(*) AS n FROM taex_reservasi t WHERE {where}', params)[0]["n"]
    rows   = query(f"""
        SELECT
            t.plant, t.equipment, t."order", t.material,
            t.material_description, t.qty_reqmts, t.qty_stock,
            t.pr, t.item, t.qty_pr, t.cost_ctrs,
            t.po, t.po_date, t.qty_deliv, t.delivery_date,
            t.reqmts_date, t.uom, t.sloc, t.reservno,
            -- PR data from sap_pr
            sp.release_date AS pr_release_date, sp.req_date AS pr_req_date,
            sp.tracking AS pr_tracking, sp.pgr AS pr_pgr,
            -- PO data from sap_po
            spo.doc_date AS po_doc_date, spo.deliv_date AS po_deliv_date,
            spo.net_price AS po_net_price, spo.crcy AS po_currency
        FROM taex_reservasi t
        LEFT JOIN sap_pr  sp  ON sp.pr = t.pr  AND sp.item = t.item  AND sp.plant = t.plant
        LEFT JOIN sap_po  spo ON spo.purchreq = t.pr AND spo.item = t.item
        WHERE {where}
        ORDER BY {order_by} {order_dir}
        LIMIT %s OFFSET %s
    """, params + [limit, offset])

    data = [dict(r) for r in rows]
    return jsonify({
        "@odata.count": int(total),
        "meta": {
            "total": int(total), "page": page, "limit": limit,
            "total_pages": max(1, -(-int(total)//limit)),
        },
        "value": data,
        "data":  data,
    })


@app.get("/api/public/tracking-joblist")
def public_tracking_joblist(
    request: Request,
    page: int = 1,
    limit: int = 99999,
    q: str = "",
    project: str = "",
    area: str = "",
    unit: str = "",
    collective: str = "",
    status: str = "",
    plant: str = "",
    equipment: str = "",
    order_by: str = "wo.\"order\"",
    order_dir: str = "ASC",
):
    """
    Public API — Tracking Joblist (WO + Job Detail + Job List + Project + Equipment + Area + Unit).

    **Auth:** Header `x-api-key` atau query `?api_key=`

    **Filter params:**
    - `q` — pencarian bebas
    - `project` — filter by project number
    - `area` — filter by area name
    - `unit` — filter by unit name
    - `collective` — filter by collective
    - `status` — filter by system status WO
    - `plant` — filter by plant
    - `equipment` — filter by equipment number
    - `page`, `limit` — pagination (max 500)
    """
    check_public_api_key(request)

    clauses = ["wo.is_deleted = 0"]
    params  = []

    if plant:
        clauses.append("jl.plant = %s"); params.append(plant)
    if project:
        clauses.append("p.project_number ILIKE %s"); params.append(f"%{project}%")
    if area:
        clauses.append("a.area_name ILIKE %s"); params.append(f"%{area}%")
    if unit:
        clauses.append("u.unit_name ILIKE %s"); params.append(f"%{unit}%")
    if collective:
        clauses.append("jd.collective = %s"); params.append(collective)
    if status:
        clauses.append("wo.system_status ILIKE %s"); params.append(f"%{status}%")
    if equipment:
        clauses.append("eq.equipment_no ILIKE %s"); params.append(f"%{equipment}%")
    if q:
        clauses.append("""(wo."order" ILIKE %s OR eq.equipment_no ILIKE %s OR
            jd.no_joblist_detail ILIKE %s OR jl.no_joblist ILIKE %s OR
            p.project_number ILIKE %s OR a.area_name ILIKE %s)""")
        params.extend([f"%{q}%"]*6)

    where  = " AND ".join(clauses)
    offset = (page-1)*limit
    total  = query(f"""
        SELECT COUNT(*) AS n
        FROM job_detail_work_order wo
        LEFT JOIN job_detail     jd  ON wo.joblist_detail_id = jd.id
        LEFT JOIN job_list       jl  ON jd.joblist_id = jl.id
        LEFT JOIN project        p   ON jl.project_id = p.id
        LEFT JOIN equipment_taex eq  ON jl.equipment_id = eq.id
        LEFT JOIN job_unit       u   ON eq.unit_id = u.id
        LEFT JOIN job_area       a   ON u.area_id = a.id
        WHERE {where}
    """, params)[0]["n"]

    rows = query(f"""
        SELECT
            p.project_number, p.description AS project_desc,
            p.project_status, p.finish_date,
            a.area_name, a.area_alias_name,
            u.unit_name, u.unit_alias_name,
            eq.equipment_no, eq.description_of_technical_object AS equipment_desc,
            eq.functional_location, eq.disiplin, eq.criticallity_text,
            jl.no_joblist, jl.joblist_description AS jl_desc, jl.plant,
            jd.no_joblist_detail, jd.joblist_detail_description AS jd_desc,
            jd.collective, jd.nomor_pm,
            jd.is_mechanical_integrity, jd.is_material, jd.is_jasa, jd.is_lldii,
            jd.planning_status_id, jd.planning_material_status_id, jd.planning_jasa_status_id,
            wo."order", wo.notification, wo.system_status, wo.user_status,
            wo.total_plnnd_costs, wo.totalact_costs,
            wo.bas_start_date, wo.basic_fin_date, wo.actual_release,
            wo.planner_group, wo.wbs_ord_header, wo.cost_center
        FROM job_detail_work_order wo
        LEFT JOIN job_detail     jd  ON wo.joblist_detail_id = jd.id
        LEFT JOIN job_list       jl  ON jd.joblist_id = jl.id
        LEFT JOIN project        p   ON jl.project_id = p.id
        LEFT JOIN equipment_taex eq  ON jl.equipment_id = eq.id
        LEFT JOIN job_unit       u   ON eq.unit_id = u.id
        LEFT JOIN job_area       a   ON u.area_id = a.id
        WHERE {where}
        ORDER BY p.project_number, a.area_name, u.unit_name,
                 eq.equipment_no, jl.no_joblist, jd.no_joblist_detail, wo."order"
        LIMIT %s OFFSET %s
    """, params + [limit, offset])

    data = [dict(r) for r in rows]
    return jsonify({
        "@odata.count": int(total),
        "meta": {
            "total": int(total), "page": page, "limit": limit,
            "total_pages": max(1, -(-int(total)//limit)),
        },
        "value": data,
        "data":  data,
    })


@app.get("/api/public/tracking-joblist2")
def public_tracking_joblist2(
    request: Request,
    page: int = 1,
    limit: int = 99999,
    q: str = "",
    project: str = "",
    area: str = "",
    unit: str = "",
    disiplin: str = "",
    status: str = "",
    plant: str = "",
):
    """
    Public API — Tracking Joblist 2 (vw_joblist_wo + vw_joblist_detail via equipment_no).

    **Auth:** Header `x-api-key` atau query `?api_key=`

    **Filter params:**
    - `q`        — pencarian bebas (order, equipment, project, no_joblist)
    - `project`  — filter by project number
    - `area`     — filter by area name
    - `unit`     — filter by unit name
    - `disiplin` — filter by disiplin
    - `status`   — filter by system status WO
    - `plant`    — filter by plant code
    - `page`, `limit` — pagination (default: semua data)
    """
    check_public_api_key(request)

    limit  = min(99999, max(1, limit))
    offset = (page - 1) * limit

    # Pakai helper yang sama dengan endpoint internal
    # tapi tanpa user session — plant dari query param
    clauses = []
    params  = []

    if plant:
        clauses.append("wo.plant = %s"); params.append(plant)
    if q:
        clauses.append("""(
            wo."order" ILIKE %s OR wo.equipment_no ILIKE %s OR
            jld.no_joblist ILIKE %s OR jld.joblist_detail_desc ILIKE %s OR
            jld.project_number ILIKE %s OR jld.area_name ILIKE %s OR
            jld.unit_name ILIKE %s OR jld.joblist_description ILIKE %s
        )""")
        params.extend([f"%{q}%"] * 8)
    if project:
        clauses.append("jld.project_number ILIKE %s"); params.append(f"%{project}%")
    if area:
        clauses.append("jld.area_name ILIKE %s"); params.append(f"%{area}%")
    if unit:
        clauses.append("jld.unit_name ILIKE %s"); params.append(f"%{unit}%")
    if disiplin:
        clauses.append("COALESCE(jld.disiplin, wo.disiplin) ILIKE %s"); params.append(f"%{disiplin}%")
    if status:
        clauses.append("wo.system_status ILIKE %s"); params.append(f"%{status}%")

    where = " AND ".join(clauses) if clauses else "1=1"

    total = query(
        f"SELECT COUNT(*) AS c FROM vw_joblist_wo wo "
        f"LEFT JOIN vw_joblist_detail jld ON jld.equipment_no = wo.equipment_no "
        f"WHERE {where}", params
    )[0]["c"]

    rows = query(
        f"{SELECT_TRK2} WHERE {where} {ORDER_TRK2} LIMIT %s OFFSET %s",
        params + [limit, offset]
    )
    data = _attach_readiness(rows)

    return jsonify({
        "@odata.count": int(total),
        "meta": {
            "total": int(total), "page": page, "limit": limit,
            "total_pages": max(1, -(-int(total) // limit)),
        },
        "value": data,
        "data":  data,
    })


@app.get("/api/public/info")
def public_info(request: Request):
    """Info public API endpoints yang tersedia."""
    check_public_api_key(request)
    return jsonify({
        "endpoints": [
            {
                "method": "GET",
                "path": "/api/public/tracking",
                "description": "Tracking material (TA-ex + PR + PO)",
                "params": ["page","limit","q","order_val","material","pr","po","status","plant"]
            },
            {
                "method": "GET",
                "path": "/api/public/tracking-joblist",
                "description": "Tracking Joblist (WO + JD + JL + Project + Equipment + Area + Unit)",
                "params": ["page","limit","q","project","area","unit","collective","status","plant","equipment"]
            },
            {
                "method": "GET",
                "path": "/api/public/tracking-joblist2",
                "description": "Tracking Joblist 2 (vw_joblist_wo + vw_joblist_detail via equipment_no + readiness)",
                "params": ["page","limit","q","project","area","unit","disiplin","status","plant"]
            },
        ],
        "auth": "Header 'x-api-key: <key>' atau query param '?api_key=<key>'",
        "note": "PUBLIC_API_KEY tersedia di environment variable server"
    })


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    return FileResponse("public/index.html")