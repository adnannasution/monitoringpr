"""
database.py — Koneksi PostgreSQL, migrasi, dan helper query
"""
import os
import json
import decimal
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_SSL = os.getenv("DB_SSL", "false").lower() == "true"

_pool = None


def get_pool():
    global _pool
    if _pool is None:
        ssl_config = {"sslmode": "require"} if DB_SSL else {}
        _pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,
            DATABASE_URL,
            **ssl_config,
            cursor_factory=RealDictCursor
        )
    return _pool


def get_conn():
    return get_pool().getconn()


def release_conn(conn):
    get_pool().putconn(conn)


def query(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
            try:
                return cur.fetchall()
            except Exception:
                return []
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def query_one(sql, params=None):
    rows = query(sql, params)
    return rows[0] if rows else None


def execute(sql, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def execute_many(sql, params_list):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, params_list)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def with_transaction(fn):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            result = fn(conn, cur)
            conn.commit()
            return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── STATE HELPERS ───────────────────────────────────────────
def get_state(key):
    row = query_one("SELECT value FROM app_state WHERE key=%s", (key,))
    if row:
        try:
            return json.loads(row["value"])
        except Exception:
            return row["value"]
    return None


class _JSONEncoder(json.JSONEncoder):
    """Handle Decimal, date, datetime dari PostgreSQL."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return str(obj)
        return super().default(obj)


def _dumps(v):
    return json.dumps(v, cls=_JSONEncoder)


def set_state(key, value):
    execute(
        """
        INSERT INTO app_state(key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT(key) DO UPDATE SET value=%s, updated_at=NOW()
        """,
        (key, _dumps(value), _dumps(value)),
    )


# ─── MIGRASI ─────────────────────────────────────────────────
def migrate():
    print("🔄 Running PostgreSQL migration...")

    execute("""
        CREATE TABLE IF NOT EXISTS taex_reservasi (
            id                   SERIAL PRIMARY KEY,
            plant                TEXT,
            equipment            TEXT,
            "order"              TEXT,
            revision             TEXT,
            material             TEXT,
            itm                  TEXT,
            material_description TEXT,
            qty_reqmts           NUMERIC DEFAULT 0,
            qty_stock            NUMERIC DEFAULT 0,
            pr                   TEXT,
            item                 TEXT,
            qty_pr               NUMERIC,
            po                   TEXT,
            po_date              TEXT,
            qty_deliv            NUMERIC,
            delivery_date        TEXT,
            sloc                 TEXT,
            del                  TEXT,
            fis                  TEXT,
            ict                  TEXT,
            pg                   TEXT,
            recipient            TEXT,
            unloading_point      TEXT,
            reqmts_date          TEXT,
            qty_f_avail_check    NUMERIC,
            qty_withdrawn        NUMERIC,
            uom                  TEXT,
            gl_acct              TEXT,
            res_price            NUMERIC,
            res_per              NUMERIC,
            res_curr             TEXT,
            reservno             TEXT,
            cost_ctrs            TEXT,
            created_at           TIMESTAMPTZ DEFAULT NOW(),
            updated_at           TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS prisma_reservasi (
            id                   SERIAL PRIMARY KEY,
            plant                TEXT,
            equipment            TEXT,
            revision             TEXT,
            "order"              TEXT,
            reservno             TEXT,
            itm                  TEXT,
            material             TEXT,
            material_description TEXT,
            del                  TEXT,
            fis                  TEXT,
            ict                  TEXT,
            pg                   TEXT,
            recipient            TEXT,
            unloading_point      TEXT,
            reqmts_date          TEXT,
            qty_reqmts           NUMERIC DEFAULT 0,
            uom                  TEXT,
            pr_prisma            TEXT,
            item_prisma          TEXT,
            qty_pr_prisma        NUMERIC,
            qty_stock_onhand     NUMERIC,
            code_kertas_kerja    TEXT,
            created_at           TIMESTAMPTZ DEFAULT NOW(),
            updated_at           TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS kumpulan_summary (
            id                   SERIAL PRIMARY KEY,
            plant                TEXT,
            equipment            TEXT,
            revision             TEXT,
            "order"              TEXT,
            reservno             TEXT,
            itm                  TEXT,
            material             TEXT,
            material_description TEXT,
            qty_req              NUMERIC DEFAULT 0,
            qty_stock            NUMERIC DEFAULT 0,
            qty_pr               NUMERIC,
            qty_to_pr            NUMERIC,
            code_tracking        TEXT,
            created_at           TIMESTAMPTZ DEFAULT NOW(),
            updated_at           TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS sap_pr (
            id                   SERIAL PRIMARY KEY,
            plant                TEXT,
            pr                   TEXT,
            item                 TEXT,
            material             TEXT,
            material_description TEXT,
            d                    TEXT,
            r                    TEXT,
            pgr                  TEXT,
            tracking_no          TEXT,
            qty_pr               NUMERIC,
            un                   TEXT,
            req_date             TEXT,
            valn_price           NUMERIC,
            pr_curr              TEXT,
            pr_per               NUMERIC,
            release_date         TEXT,
            tracking             TEXT,
            s                    TEXT,
            created_at           TIMESTAMPTZ DEFAULT NOW(),
            updated_at           TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS sap_po (
            id             SERIAL PRIMARY KEY,
            plnt           TEXT,
            purchreq       TEXT,
            item           TEXT,
            material       TEXT,
            short_text     TEXT,
            po             TEXT,
            po_item        TEXT,
            d              TEXT,
            dci            TEXT,
            pgr            TEXT,
            doc_date       TEXT,
            po_quantity    NUMERIC,
            qty_delivered  NUMERIC,
            deliv_date     TEXT,
            oun            TEXT,
            net_price      NUMERIC,
            crcy           TEXT,
            per            NUMERIC,
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            updated_at     TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS work_order (
            id                 SERIAL PRIMARY KEY,
            plant              TEXT,
            "order"            TEXT,
            superior_order     TEXT,
            notification       TEXT,
            created_on         TEXT,
            description        TEXT,
            revision           TEXT,
            equipment          TEXT,
            system_status      TEXT,
            user_status        TEXT,
            funct_location     TEXT,
            location           TEXT,
            wbs_ord_header     TEXT,
            cost_center        TEXT,
            total_plan_cost    NUMERIC,
            total_act_cost     NUMERIC,
            planner_group      TEXT,
            main_work_ctr      TEXT,
            entry_by           TEXT,
            changed_by         TEXT,
            basic_start_date   TEXT,
            basic_finish_date  TEXT,
            actual_release     TEXT,
            created_at         TIMESTAMPTZ DEFAULT NOW(),
            updated_at         TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS app_state (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS equipment_taex (
            id                              TEXT PRIMARY KEY,
            plant                           TEXT,
            created                         TEXT,
            created_by                      TEXT,
            is_deleted                      INTEGER DEFAULT 0,
            modified                        TEXT,
            modified_by                     TEXT,
            unit_id                         TEXT,
            catalog_profile                 TEXT,
            criticallity                    TEXT,
            criticallity_text               TEXT,
            description_of_technical_object TEXT,
            disiplin                        TEXT,
            equipment_category              TEXT,
            equipment_no                    TEXT,
            functional_location             TEXT,
            group_asset                     TEXT,
            location                        TEXT,
            main_work_center                TEXT,
            maintenance_plant               TEXT,
            model_type                      TEXT,
            planning_plant                  TEXT,
            catalog_profile_text            TEXT,
            manufacturer_of_asset           TEXT,
            inserted_at                     TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS project (
            id                   TEXT PRIMARY KEY,
            project_number       TEXT,
            project_type_id      TEXT,
            start_date           TEXT,
            finish_date          TEXT,
            revision             TEXT,
            description          TEXT,
            project_status       TEXT,
            plant                TEXT,
            created              TEXT,
            created_by           TEXT,
            is_deleted           INTEGER DEFAULT 0,
            modified             TEXT,
            modified_by          TEXT,
            duration_ta_brick_id TEXT,
            inserted_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS job_list (
            id                  TEXT PRIMARY KEY,
            project_id          TEXT,
            equipment_id        TEXT,
            plant               TEXT,
            created             TEXT,
            created_by          TEXT,
            is_deleted          INTEGER DEFAULT 0,
            modified            TEXT,
            modified_by         TEXT,
            joblist_description TEXT,
            no_joblist          TEXT,
            inserted_at         TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS job_detail (
            id                           TEXT PRIMARY KEY,
            joblist_id                   TEXT,
            joblist_detail_reason_id     TEXT,
            joblist_detail_description   TEXT,
            is_mechanical_integrity      INTEGER,
            is_optimization              INTEGER,
            job_discipline_id            TEXT,
            plant                        TEXT,
            created                      TEXT,
            created_by                   TEXT,
            is_deleted                   INTEGER DEFAULT 0,
            modified                     TEXT,
            modified_by                  TEXT,
            no_document                  TEXT,
            creator_created              TEXT,
            creator_job_title            TEXT,
            creator_name                 TEXT,
            assign_to                    TEXT,
            authparam_area               TEXT,
            status_id                    TEXT,
            document_joblist_type_id     TEXT,
            economic_consiqiency_class   INTEGER,
            economic_probability_class   INTEGER,
            environment_consiqiency_class INTEGER,
            environment_probability_class INTEGER,
            health_consiqiency_class     INTEGER,
            health_probability_class     INTEGER,
            is_off_stream                INTEGER,
            job_execution_id             TEXT,
            joblist_detail_category_id   TEXT,
            legal_consiqiency_class      INTEGER,
            legal_probability_class      INTEGER,
            responsibility_id            TEXT,
            nomor_pm                     TEXT,
            collective                   TEXT,
            maintenance_plan             TEXT,
            maintenanceitem              TEXT,
            notes                        TEXT,
            pic_planner                  TEXT,
            assign_to_pic_date           TEXT,
            pic_planner_job_title        TEXT,
            pic_planner_name             TEXT,
            is_all_in                    INTEGER,
            is_aspek_durasi              INTEGER,
            is_aspek_quality             INTEGER,
            is_aspek_safety              INTEGER,
            is_jasa                      INTEGER,
            is_lldii                     INTEGER,
            is_material                  INTEGER,
            no_joblist_detail            TEXT,
            is_request_freezing          INTEGER,
            request_freezing_date        TEXT,
            planning_status_id           TEXT,
            planning_contract_id         TEXT,
            planning_jasa_status_id      TEXT,
            planning_material_status_id  TEXT,
            inserted_at                  TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS job_detail_work_order (
            id                  TEXT PRIMARY KEY,
            joblist_detail_id   TEXT,
            notification        TEXT,
            created_on          TEXT,
            superior_order      TEXT,
            "order"             TEXT,
            description         TEXT,
            equipment           TEXT,
            functional_loc      TEXT,
            location            TEXT,
            revision            TEXT,
            system_status       TEXT,
            user_status         TEXT,
            wbs_ord_header      TEXT,
            total_plnnd_costs   NUMERIC,
            totalact_costs      NUMERIC,
            planner_group       TEXT,
            main_work_ctr       TEXT,
            change_by           TEXT,
            bas_start_date      TEXT,
            basic_fin_date      TEXT,
            actual_release      TEXT,
            cost_center         TEXT,
            entered_by          TEXT,
            created             TEXT,
            created_by          TEXT,
            is_deleted          INTEGER DEFAULT 0,
            modified            TEXT,
            modified_by         TEXT,
            inserted_at         TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS job_area (
            id              TEXT PRIMARY KEY,
            area_name       TEXT,
            plant           TEXT,
            created         TEXT,
            created_by      TEXT,
            is_deleted      INTEGER DEFAULT 0,
            modified        TEXT,
            modified_by     TEXT,
            area_alias_name TEXT,
            inserted_at     TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS job_unit (
            id              TEXT PRIMARY KEY,
            area_id         TEXT REFERENCES job_area(id) ON DELETE SET NULL,
            unit_name       TEXT,
            plant           TEXT,
            created         TEXT,
            created_by      TEXT,
            is_deleted      INTEGER DEFAULT 0,
            modified        TEXT,
            modified_by     TEXT,
            unit_alias_name TEXT,
            inserted_at     TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # ── Auth tables ──────────────────────────────────────────
    execute("""
        CREATE TABLE IF NOT EXISTS plants (
            plant_code  TEXT PRIMARY KEY,
            plant_name  TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            plant_code    TEXT REFERENCES plants(plant_code),
            pg_role       TEXT NOT NULL DEFAULT 'TA',
            is_admin      BOOLEAN DEFAULT FALSE,
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            token       TEXT PRIMARY KEY,
            user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Seed default admin jika belum ada
    existing = query_one("SELECT id FROM users WHERE username='admin'")
    if not existing:
        import hashlib, os
        salt = os.urandom(16).hex()
        pw   = hashlib.sha256(f"Admin@123{salt}".encode()).hexdigest()
        execute(
            "INSERT INTO users (username, password_hash, plant_code, pg_role, is_admin) VALUES (%s, %s, NULL, 'Admin', TRUE)",
            (f"admin", f"{pw}:{salt}")
        )
        print("✅ Default admin created: admin / Admin@123")

    execute("""
        CREATE TABLE IF NOT EXISTS vw_joblist_wo (
            id                       SERIAL PRIMARY KEY,
            plant                    TEXT,
            equipment_no             TEXT,
            disiplin                 TEXT,
            joblist_description      TEXT,
            planning_jasa_status     TEXT,
            planning_material_status TEXT,
            code_name                TEXT,
            is_lldii                 INTEGER,
            "order"                  TEXT,
            notification             TEXT,
            created_on               TEXT,
            superior_order           TEXT,
            description              TEXT,
            functional_loc           TEXT,
            location                 TEXT,
            revision                 TEXT,
            system_status            TEXT,
            user_status              TEXT,
            wbs_ord_header           TEXT,
            total_plnnd_costs        NUMERIC,
            totalact_costs           NUMERIC,
            planner_group            TEXT,
            main_work_ctr            TEXT,
            change_by                TEXT,
            bas_start_date           TEXT,
            basic_fin_date           TEXT,
            actual_release           TEXT,
            cost_center              TEXT,
            entered_by               TEXT,
            inserted_at              TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    execute("""
        CREATE TABLE IF NOT EXISTS vw_joblist_detail (
            id                       TEXT,
            joblist_id               TEXT,
            joblist_detail_desc      TEXT,
            reason_name              TEXT,
            doc_type_name            TEXT,
            no_document              TEXT,
            is_mechanical_integrity  INTEGER,
            job_discipline_name      TEXT,
            nomor_pm                 TEXT,
            notes                    TEXT,
            plant                    TEXT,
            created                  TEXT,
            creator_name             TEXT,
            creator_job_title        TEXT,
            is_deleted               INTEGER DEFAULT 0,
            joblist_description      TEXT,
            no_joblist               TEXT,
            project_number           TEXT,
            project_type_code        TEXT,
            project_type_name        TEXT,
            start_date               TEXT,
            finish_date              TEXT,
            revision                 TEXT,
            description              TEXT,
            project_status           TEXT,
            equipment_no             TEXT,
            area_name                TEXT,
            area_alias_name          TEXT,
            unit_name                TEXT,
            unit_alias_name          TEXT,
            functional_location      TEXT,
            location                 TEXT,
            disiplin                 TEXT,
            criticallity             TEXT,
            criticallity_text        TEXT,
            main_work_center         TEXT,
            is_all_in                INTEGER,
            is_jasa                  INTEGER,
            is_lldii                 INTEGER,
            is_material              INTEGER,
            code_name                TEXT,
            planning_jasa_status     TEXT,
            planning_material_status TEXT,
            lldi_status              TEXT,
            is_freezing              INTEGER,
            inserted_at              TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Indexes
    for sql in [
        'CREATE INDEX IF NOT EXISTS idx_taex_material ON taex_reservasi(material)',
        'CREATE INDEX IF NOT EXISTS idx_taex_order    ON taex_reservasi("order")',
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_taex_upsert_key ON taex_reservasi("order", material, itm)',
        'CREATE INDEX IF NOT EXISTS idx_prisma_material ON prisma_reservasi(material)',
        'CREATE INDEX IF NOT EXISTS idx_prisma_order    ON prisma_reservasi("order")',
        'CREATE INDEX IF NOT EXISTS idx_sap_pr          ON sap_pr(pr)',
        'CREATE INDEX IF NOT EXISTS idx_kumpulan_code   ON kumpulan_summary(code_tracking)',
        'CREATE INDEX IF NOT EXISTS idx_sap_po_po              ON sap_po(po)',
        'CREATE INDEX IF NOT EXISTS idx_sap_po_purchreq        ON sap_po(purchreq)',
        # ── Index untuk /api/tracking (JOIN + filter) ──
        'CREATE INDEX IF NOT EXISTS idx_taex_pr        ON taex_reservasi(pr)',
        'CREATE INDEX IF NOT EXISTS idx_taex_po        ON taex_reservasi(po)',
        'CREATE INDEX IF NOT EXISTS idx_taex_plant     ON taex_reservasi(plant)',
        'CREATE INDEX IF NOT EXISTS idx_sap_pr_pr_mat  ON sap_pr(pr, material)',
        'CREATE INDEX IF NOT EXISTS idx_sap_po_po_mat  ON sap_po(po, material)',
        'CREATE INDEX IF NOT EXISTS idx_wo_order       ON work_order("order")',
        'CREATE INDEX IF NOT EXISTS idx_job_area_plant      ON job_area(plant)',
        'CREATE INDEX IF NOT EXISTS idx_job_unit_area       ON job_unit(area_id)',
        'CREATE INDEX IF NOT EXISTS idx_job_unit_plant      ON job_unit(plant)',
        'CREATE INDEX IF NOT EXISTS idx_equipment_no            ON equipment_taex(equipment_no)',
        'CREATE INDEX IF NOT EXISTS idx_equipment_plant         ON equipment_taex(plant)',
        'CREATE INDEX IF NOT EXISTS idx_equipment_func_loc      ON equipment_taex(functional_location)',
        'CREATE INDEX IF NOT EXISTS idx_project_number         ON project(project_number)',
        'CREATE INDEX IF NOT EXISTS idx_project_plant          ON project(plant)',
        'CREATE INDEX IF NOT EXISTS idx_joblist_project        ON job_list(project_id)',
        'CREATE INDEX IF NOT EXISTS idx_joblist_plant          ON job_list(plant)',
        'CREATE INDEX IF NOT EXISTS idx_jobdetail_joblist      ON job_detail(joblist_id)',
        'CREATE INDEX IF NOT EXISTS idx_jobdetail_plant        ON job_detail(plant)',
        'CREATE INDEX IF NOT EXISTS idx_jdwo_joblist_detail    ON job_detail_work_order(joblist_detail_id)',
        'CREATE INDEX IF NOT EXISTS idx_jdwo_order             ON job_detail_work_order("order")',
        'CREATE INDEX IF NOT EXISTS idx_vw_jl_wo_order        ON vw_joblist_wo("order")',
        'CREATE INDEX IF NOT EXISTS idx_vw_jl_wo_plant        ON vw_joblist_wo(plant)',
        'CREATE INDEX IF NOT EXISTS idx_vw_jl_wo_revision     ON vw_joblist_wo(revision)',
        'CREATE INDEX IF NOT EXISTS idx_vw_jld_plant          ON vw_joblist_detail(plant)',
        'CREATE INDEX IF NOT EXISTS idx_vw_jld_revision       ON vw_joblist_detail(revision)',
        'CREATE INDEX IF NOT EXISTS idx_vw_jld_project        ON vw_joblist_detail(project_number)',
    ]:
        try:
            execute(sql)
        except Exception:
            pass

    print("✅ Migration complete")