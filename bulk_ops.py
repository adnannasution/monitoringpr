"""
bulk_ops.py — Operasi bulk INSERT/UPDATE ke PostgreSQL menggunakan psycopg2 execute_values
Langsung kirim semua baris sekaligus — tidak perlu chunking, tetap cepat.
execute_values otomatis handle ribuan bahkan ratusan ribu baris dalam satu round-trip ke DB.
"""
import pandas as pd
from psycopg2.extras import execute_values
from database import get_conn, release_conn
from header_maps import normalize_taex, normalize_sap, normalize_order


def _n(v):
    """Konversi ke float atau None."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _s(v):
    """Konversi ke string atau None. String 'NULL'/'null'/'None' dianggap None."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s.upper() in ('NULL', 'NONE', 'NAN', 'N/A', 'NA', '#N/A'):
        return None
    return s if s else None


# ─── TAEX RESERVASI ──────────────────────────────────────────
def bulk_replace_taex(df: pd.DataFrame, mode: str = "replace") -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if mode == "replace":
                cur.execute("DELETE FROM taex_reservasi")

            rows = []
            for _, raw in df.iterrows():
                r = normalize_taex(raw.to_dict())
                rows.append((
                    _s(r.get("Plant")), _s(r.get("Equipment")), _s(r.get("Order")),
                    _s(r.get("Revision")), _s(r.get("Material")), _s(r.get("Itm")),
                    _s(r.get("Material_Description")), _n(r.get("Qty_Reqmts")) or 0,
                    _n(r.get("Qty_Stock")) or 0,
                    _s(r.get("PR")), _s(r.get("Item")), _n(r.get("Qty_PR")),
                    _s(r.get("Cost_Ctrs")),
                    _s(r.get("PO")), _s(r.get("PO_Date")), _n(r.get("Qty_Deliv")),
                    _s(r.get("Delivery_Date")),
                    _s(r.get("SLoc")), _s(r.get("Del")), _s(r.get("FIs")),
                    _s(r.get("Ict")), _s(r.get("PG")),
                    _s(r.get("Recipient")), _s(r.get("Unloading_point")),
                    _s(r.get("Reqmts_Date")),
                    _n(r.get("Qty_f_avail_check")), _n(r.get("Qty_Withdrawn")),
                    _s(r.get("UoM")), _s(r.get("GL_Acct")),
                    _n(r.get("Res_Price")), _n(r.get("Res_per")), _s(r.get("Res_Curr")),
                    _s(r.get("Reservno")),
                ))

            if mode == "append":
                sql = """
                    INSERT INTO taex_reservasi
                    (plant, equipment, "order", revision, material, itm,
                     material_description, qty_reqmts, qty_stock,
                     pr, item, qty_pr, cost_ctrs,
                     po, po_date, qty_deliv, delivery_date,
                     sloc, del, fis, ict, pg,
                     recipient, unloading_point, reqmts_date,
                     qty_f_avail_check, qty_withdrawn,
                     uom, gl_acct, res_price, res_per, res_curr, reservno)
                    VALUES %s
                    ON CONFLICT ("order", material, itm) DO UPDATE SET
                        plant=EXCLUDED.plant, equipment=EXCLUDED.equipment,
                        revision=EXCLUDED.revision, material_description=EXCLUDED.material_description,
                        qty_reqmts=EXCLUDED.qty_reqmts, qty_stock=EXCLUDED.qty_stock,
                        cost_ctrs=EXCLUDED.cost_ctrs, sloc=EXCLUDED.sloc,
                        del=EXCLUDED.del, fis=EXCLUDED.fis, ict=EXCLUDED.ict, pg=EXCLUDED.pg,
                        recipient=EXCLUDED.recipient, unloading_point=EXCLUDED.unloading_point,
                        reqmts_date=EXCLUDED.reqmts_date,
                        qty_f_avail_check=EXCLUDED.qty_f_avail_check,
                        qty_withdrawn=EXCLUDED.qty_withdrawn,
                        uom=EXCLUDED.uom, gl_acct=EXCLUDED.gl_acct,
                        res_price=EXCLUDED.res_price, res_per=EXCLUDED.res_per,
                        res_curr=EXCLUDED.res_curr, reservno=EXCLUDED.reservno,
                        updated_at=NOW()
                """
            else:
                sql = """
                    INSERT INTO taex_reservasi
                    (plant, equipment, "order", revision, material, itm,
                     material_description, qty_reqmts, qty_stock,
                     pr, item, qty_pr, cost_ctrs,
                     po, po_date, qty_deliv, delivery_date,
                     sloc, del, fis, ict, pg,
                     recipient, unloading_point, reqmts_date,
                     qty_f_avail_check, qty_withdrawn,
                     uom, gl_acct, res_price, res_per, res_curr, reservno)
                    VALUES %s
                """

            execute_values(cur, sql, rows)

        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── PRISMA RESERVASI ─────────────────────────────────────────
def bulk_replace_prisma(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM prisma_reservasi")
            sql = """
                INSERT INTO prisma_reservasi
                (plant, equipment, revision, "order", reservno, itm, material, material_description,
                 del, fis, ict, pg, recipient, unloading_point, reqmts_date,
                 qty_reqmts, uom, pr_prisma, item_prisma, qty_pr_prisma, qty_stock_onhand, code_kertas_kerja)
                VALUES %s
            """
            rows = []
            for _, raw in df.iterrows():
                r = raw.to_dict()
                rows.append((
                    _s(r.get("Plant")), _s(r.get("Equipment")), _s(r.get("Revision")),
                    _s(r.get("Order")), _s(r.get("Reservno")), _s(r.get("Itm")),
                    _s(r.get("Material")), _s(r.get("Material_Description")),
                    _s(r.get("Del")), _s(r.get("FIs")), _s(r.get("Ict")), _s(r.get("PG")),
                    _s(r.get("Recipient")), _s(r.get("Unloading_point")), _s(r.get("Reqmts_Date")),
                    _n(r.get("Qty_Reqmts")) or 0, _s(r.get("UoM")),
                    _s(r.get("PR_Prisma")), _s(r.get("Item_Prisma")), _n(r.get("Qty_PR_Prisma")),
                    _n(r.get("Qty_StockOnhand")), _s(r.get("CodeKertasKerja")),
                ))

            execute_values(cur, sql, rows)

        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── SAP PR ──────────────────────────────────────────────────
def bulk_replace_pr(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sap_pr")
            sql = """
                INSERT INTO sap_pr
                (plant, pr, item, material, material_description, d, r, pgr, s, tracking_no,
                 qty_pr, un, req_date, valn_price, pr_curr, pr_per, release_date, tracking)
                VALUES %s
            """
            rows = []
            for _, raw in df.iterrows():
                r = normalize_sap(raw.to_dict())
                rows.append((
                    _s(r.get("Plant")), _s(r.get("PR")), _s(r.get("Item")),
                    _s(r.get("Material")), _s(r.get("Material_Description")),
                    _s(r.get("D")), _s(r.get("R")), _s(r.get("PGr")),
                    _s(r.get("S")), _s(r.get("TrackingNo")),
                    _n(r.get("Qty_PR")), _s(r.get("Un")), _s(r.get("Req_Date")),
                    _n(r.get("Valn_price")), _s(r.get("PR_Curr")), _n(r.get("PR_Per")),
                    _s(r.get("Release_Date")), _s(r.get("Tracking")),
                ))

            execute_values(cur, sql, rows)

        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── SAP PO ──────────────────────────────────────────────────
def bulk_replace_po(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sap_po")
            sql = """
                INSERT INTO sap_po
                (plnt, purchreq, item, material, short_text, po, po_item, d, dci, pgr,
                 doc_date, po_quantity, qty_delivered, deliv_date, oun, net_price, crcy, per)
                VALUES %s
            """
            rows = []
            for _, raw in df.iterrows():
                r = raw.to_dict()
                rows.append((
                    _s(r.get("Plnt") or r.get("plnt")),
                    _s(r.get("Purchreq") or r.get("PurchReq")),
                    _s(r.get("Item") or r.get("item")),
                    _s(r.get("Material") or r.get("material")),
                    _s(r.get("Short_Text") or r.get("Short Text") or r.get("short_text")),
                    _s(r.get("PO") or r.get("po")),
                    _s(r.get("PO_Item") or r.get("Item1") or r.get("PO Item")),
                    _s(r.get("D") or r.get("d")),
                    _s(r.get("DCI") or r.get("dci")),
                    _s(r.get("PGr") or r.get("pgr")),
                    _s(r.get("Doc_Date") or r.get("PO Date") or r.get("Doc. Date")),
                    _n(r.get("PO_Quantity") or r.get("Ordered") or r.get("PO Quantity")),
                    _n(r.get("Qty_Delivered") or r.get("Qty Delivered")),
                    _s(r.get("Deliv_Date") or r.get("DelivDate") or r.get("Deliv. Date")),
                    _s(r.get("OUn") or r.get("Un")),
                    _n(r.get("Net_Price") or r.get("Net Price")),
                    _s(r.get("Crcy") or r.get("crcy")),
                    _n(r.get("Per") or r.get("per")),
                ))

            execute_values(cur, sql, rows)

        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── KUMPULAN SUMMARY ─────────────────────────────────────────
def bulk_replace_kumpulan(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM kumpulan_summary")
            sql = """
                INSERT INTO kumpulan_summary
                (plant, equipment, revision, "order", reservno, itm, material, material_description,
                 qty_req, qty_stock, qty_pr, qty_to_pr, code_tracking)
                VALUES %s
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Plant")), _s(r.get("Equipment")), _s(r.get("Revision")),
                    _s(r.get("Order")), _s(r.get("Reservno")), _s(r.get("Itm")),
                    _s(r.get("Material")), _s(r.get("Material_Description")),
                    _n(r.get("Qty_Req")) or 0, _n(r.get("Qty_Stock")) or 0,
                    _n(r.get("Qty_PR")), _n(r.get("Qty_To_PR")),
                    _s(r.get("CodeTracking")),
                ))

            execute_values(cur, sql, rows)

        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── WORK ORDER ──────────────────────────────────────────────
def bulk_replace_order(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM work_order")
            sql = """
                INSERT INTO work_order
                (plant, "order", superior_order, notification, created_on, description, revision,
                 equipment, system_status, user_status, funct_location, location, wbs_ord_header,
                 cost_center, total_plan_cost, total_act_cost, planner_group, main_work_ctr,
                 entry_by, changed_by, basic_start_date, basic_finish_date, actual_release)
                VALUES %s
            """
            rows = []
            for _, raw in df.iterrows():
                r = normalize_order(raw.to_dict())
                rows.append((
                    _s(r.get("Plant")), _s(r.get("Order")),
                    _s(r.get("Superior_Order")), _s(r.get("Notification")),
                    _s(r.get("Created_On")), _s(r.get("Description")),
                    _s(r.get("Revision")), _s(r.get("Equipment")),
                    _s(r.get("System_Status")), _s(r.get("User_Status")),
                    _s(r.get("FunctLocation")), _s(r.get("Location")),
                    _s(r.get("WBS_Ord_header")), _s(r.get("CostCenter")),
                    _n(r.get("Total_Plan_Cost")), _n(r.get("Total_Act_Cost")),
                    _s(r.get("Planner_Group")), _s(r.get("MainWorkCtr")),
                    _s(r.get("Entry_by")), _s(r.get("Changed_by")),
                    _s(r.get("Basic_start_date")), _s(r.get("Basic_finish_date")),
                    _s(r.get("Actual_Release")),
                ))

            execute_values(cur, sql, rows)

        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── PROJECT ─────────────────────────────────────────────────
def bulk_replace_project(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM project WHERE is_deleted = 0")
            sql = """
                INSERT INTO project
                (id, project_number, project_type_id, start_date, finish_date,
                 revision, description, project_status, plant,
                 created, created_by, is_deleted, modified, modified_by, duration_ta_brick_id)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    project_number=EXCLUDED.project_number,
                    project_type_id=EXCLUDED.project_type_id,
                    start_date=EXCLUDED.start_date,
                    finish_date=EXCLUDED.finish_date,
                    revision=EXCLUDED.revision,
                    description=EXCLUDED.description,
                    project_status=EXCLUDED.project_status,
                    plant=EXCLUDED.plant,
                    created=EXCLUDED.created,
                    created_by=EXCLUDED.created_by,
                    is_deleted=EXCLUDED.is_deleted,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    duration_ta_brick_id=EXCLUDED.duration_ta_brick_id
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("ProjectNumber")), _s(r.get("ProjectTypeId")),
                    _s(r.get("StartDate")), _s(r.get("FinishDate")),
                    _s(r.get("Revision")), _s(r.get("Description")), _s(r.get("ProjectStatus")),
                    _s(r.get("Plant")), _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                    _s(r.get("DurationTaBrickId")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── JOB LIST ────────────────────────────────────────────────
def bulk_replace_job_list(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM job_list WHERE is_deleted = 0")
            sql = """
                INSERT INTO job_list
                (id, project_id, equipment_id, plant, created, created_by,
                 is_deleted, modified, modified_by, joblist_description, no_joblist)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    project_id=EXCLUDED.project_id,
                    equipment_id=EXCLUDED.equipment_id,
                    plant=EXCLUDED.plant,
                    created=EXCLUDED.created,
                    created_by=EXCLUDED.created_by,
                    is_deleted=EXCLUDED.is_deleted,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    joblist_description=EXCLUDED.joblist_description,
                    no_joblist=EXCLUDED.no_joblist
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("ProjectId")), _s(r.get("EquipmentId")),
                    _s(r.get("Plant")), _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                    _s(r.get("JoblistDescription")), _s(r.get("NoJoblist")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── JOB DETAIL ──────────────────────────────────────────────
def bulk_replace_job_detail(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM job_detail WHERE is_deleted = 0")
            sql = """
                INSERT INTO job_detail
                (id, joblist_id, joblist_detail_reason_id, joblist_detail_description,
                 is_mechanical_integrity, is_optimization, job_discipline_id, plant,
                 created, created_by, is_deleted, modified, modified_by,
                 no_document, creator_created, creator_job_title, creator_name,
                 assign_to, authparam_area, status_id, document_joblist_type_id,
                 economic_consiqiency_class, economic_probability_class,
                 environment_consiqiency_class, environment_probability_class,
                 health_consiqiency_class, health_probability_class,
                 is_off_stream, job_execution_id, joblist_detail_category_id,
                 legal_consiqiency_class, legal_probability_class, responsibility_id,
                 nomor_pm, collective, maintenance_plan, maintenanceitem, notes,
                 pic_planner, assign_to_pic_date, pic_planner_job_title, pic_planner_name,
                 is_all_in, is_aspek_durasi, is_aspek_quality, is_aspek_safety,
                 is_jasa, is_lldii, is_material, no_joblist_detail,
                 is_request_freezing, request_freezing_date,
                 planning_status_id, planning_contract_id,
                 planning_jasa_status_id, planning_material_status_id)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    joblist_id=EXCLUDED.joblist_id,
                    joblist_detail_description=EXCLUDED.joblist_detail_description,
                    is_mechanical_integrity=EXCLUDED.is_mechanical_integrity,
                    is_optimization=EXCLUDED.is_optimization,
                    plant=EXCLUDED.plant,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    no_document=EXCLUDED.no_document,
                    status_id=EXCLUDED.status_id,
                    notes=EXCLUDED.notes,
                    no_joblist_detail=EXCLUDED.no_joblist_detail,
                    planning_status_id=EXCLUDED.planning_status_id,
                    planning_material_status_id=EXCLUDED.planning_material_status_id,
                    planning_jasa_status_id=EXCLUDED.planning_jasa_status_id,
                    is_deleted=EXCLUDED.is_deleted
            """
            def _i(v):
                try: return int(float(v)) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None
                except: return None

            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("JoblistId")),
                    _s(r.get("JoblistDetailReasonId")), _s(r.get("JoblistDetailDescription")),
                    _i(r.get("IsMechanicalIntegrity")), _i(r.get("IsOptimization")),
                    _s(r.get("JobDisciplineId")), _s(r.get("Plant")),
                    _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                    _s(r.get("NoDocument")), _s(r.get("CreatorCreated")),
                    _s(r.get("CreatorJobTitle")), _s(r.get("CreatorName")),
                    _s(r.get("AssignTo")), _s(r.get("AuthparamArea")),
                    _s(r.get("StatusId")), _s(r.get("DocumentJoblistTypeId")),
                    _i(r.get("EconomicConsiqiencyClass")), _i(r.get("EconomicProbabilityClass")),
                    _i(r.get("EnvironmentConsiqiencyClass")), _i(r.get("EnvironmentProbabilityClass")),
                    _i(r.get("HealthConsiqiencyClass")), _i(r.get("HealthProbabilityClass")),
                    _i(r.get("IsOffStream")), _s(r.get("JobExecutionId")),
                    _s(r.get("JoblistDetailCategoryId")),
                    _i(r.get("LegalConsiqiencyClass")), _i(r.get("LegalProbabilityClass")),
                    _s(r.get("ResponsibilityId")), _s(r.get("NomorPM")),
                    _s(r.get("Collective")), _s(r.get("MaintenancePlan")),
                    _s(r.get("Maintenanceitem")), _s(r.get("Notes")),
                    _s(r.get("PICPlanner")), _s(r.get("AssignToPICDate")),
                    _s(r.get("PICPlannerJobTitle")), _s(r.get("PICPlannerName")),
                    _i(r.get("IsAllIn")), _i(r.get("IsAspekDurasi")),
                    _i(r.get("IsAspekQuality")), _i(r.get("IsAspekSafety")),
                    _i(r.get("IsJasa")), _i(r.get("IsLLDII")), _i(r.get("IsMaterial")),
                    _s(r.get("NoJoblistDetail")),
                    _i(r.get("IsRequestFreezing")), _s(r.get("RequestFreezingDate")),
                    _s(r.get("PlanningStatusId")), _s(r.get("PlanningContractId")),
                    _s(r.get("PlanningJasaStatusId")), _s(r.get("PlanningMaterialStatusId")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── JOB DETAIL WORK ORDER ───────────────────────────────────
def bulk_replace_job_detail_work_order(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM job_detail_work_order WHERE is_deleted = 0")
            sql = """
                INSERT INTO job_detail_work_order
                (id, joblist_detail_id, notification, created_on, superior_order, "order",
                 description, equipment, functional_loc, location, revision,
                 system_status, user_status, wbs_ord_header,
                 total_plnnd_costs, totalact_costs,
                 planner_group, main_work_ctr, change_by,
                 bas_start_date, basic_fin_date, actual_release,
                 cost_center, entered_by, created, created_by,
                 is_deleted, modified, modified_by)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    joblist_detail_id=EXCLUDED.joblist_detail_id,
                    notification=EXCLUDED.notification,
                    "order"=EXCLUDED."order",
                    description=EXCLUDED.description,
                    equipment=EXCLUDED.equipment,
                    system_status=EXCLUDED.system_status,
                    user_status=EXCLUDED.user_status,
                    total_plnnd_costs=EXCLUDED.total_plnnd_costs,
                    totalact_costs=EXCLUDED.totalact_costs,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    is_deleted=EXCLUDED.is_deleted
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("JoblistDetailId")),
                    _s(r.get("Notification")), _s(r.get("CreatedOn")),
                    _s(r.get("SuperiorOrder")), _s(r.get("Order")),
                    _s(r.get("Description")), _s(r.get("Equipment")),
                    _s(r.get("FunctionalLoc")), _s(r.get("Location")),
                    _s(r.get("Revision")), _s(r.get("SystemStatus")),
                    _s(r.get("UserStatus")), _s(r.get("WBSordheader")),
                    _n(r.get("TotalPlnndCosts")), _n(r.get("Totalactcosts")),
                    _s(r.get("PlannerGroup")), _s(r.get("MainWorkCtr")),
                    _s(r.get("ChangeBy")), _s(r.get("Basstartdate")),
                    _s(r.get("Basicfindate")), _s(r.get("ActualRelease")),
                    _s(r.get("CostCenter")), _s(r.get("EnteredBy")),
                    _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── EQUIPMENT TAEX ──────────────────────────────────────────
def bulk_replace_equipment_taex(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM equipment_taex WHERE is_deleted = 0")
            sql = """
                INSERT INTO equipment_taex
                (id, plant, created, created_by, is_deleted, modified, modified_by,
                 unit_id, catalog_profile, criticallity, criticallity_text,
                 description_of_technical_object, disiplin, equipment_category,
                 equipment_no, functional_location, group_asset, location,
                 main_work_center, maintenance_plant, model_type, planning_plant,
                 catalog_profile_text, manufacturer_of_asset)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    plant=EXCLUDED.plant,
                    catalog_profile=EXCLUDED.catalog_profile,
                    criticallity=EXCLUDED.criticallity,
                    criticallity_text=EXCLUDED.criticallity_text,
                    description_of_technical_object=EXCLUDED.description_of_technical_object,
                    disiplin=EXCLUDED.disiplin,
                    equipment_category=EXCLUDED.equipment_category,
                    equipment_no=EXCLUDED.equipment_no,
                    functional_location=EXCLUDED.functional_location,
                    group_asset=EXCLUDED.group_asset,
                    location=EXCLUDED.location,
                    main_work_center=EXCLUDED.main_work_center,
                    maintenance_plant=EXCLUDED.maintenance_plant,
                    model_type=EXCLUDED.model_type,
                    planning_plant=EXCLUDED.planning_plant,
                    catalog_profile_text=EXCLUDED.catalog_profile_text,
                    manufacturer_of_asset=EXCLUDED.manufacturer_of_asset,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    is_deleted=EXCLUDED.is_deleted
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("Plant")),
                    _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                    _s(r.get("UnitId")), _s(r.get("CatalogProfile")),
                    _s(r.get("Criticallity")), _s(r.get("CriticallityText")),
                    _s(r.get("DescriptionofTechnicalObject")),
                    _s(r.get("Disiplin")), _s(r.get("EquipmentCategory")),
                    _s(r.get("EquipmentNo")), _s(r.get("FunctionalLocation")),
                    _s(r.get("GroupAsset")), _s(r.get("Location")),
                    _s(r.get("MainWorkCenter")), _s(r.get("MaintenancePlant")),
                    _s(r.get("ModelType")), _s(r.get("PlanningPlant")),
                    _s(r.get("CatalogProfileText")), _s(r.get("ManufacturerOfAsset")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── JOB AREA ────────────────────────────────────────────────
def bulk_replace_job_area(df) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM job_area WHERE is_deleted = 0")
            sql = """
                INSERT INTO job_area
                (id, area_name, plant, created, created_by, is_deleted,
                 modified, modified_by, area_alias_name)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    area_name=EXCLUDED.area_name,
                    plant=EXCLUDED.plant,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    area_alias_name=EXCLUDED.area_alias_name,
                    is_deleted=EXCLUDED.is_deleted
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("AreaName")), _s(r.get("Plant")),
                    _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                    _s(r.get("AreaAliasName")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback(); raise e
    finally:
        release_conn(conn)


# ─── JOB UNIT ────────────────────────────────────────────────
def bulk_replace_job_unit(df) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM job_unit WHERE is_deleted = 0")
            sql = """
                INSERT INTO job_unit
                (id, area_id, unit_name, plant, created, created_by, is_deleted,
                 modified, modified_by, unit_alias_name)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    area_id=EXCLUDED.area_id,
                    unit_name=EXCLUDED.unit_name,
                    plant=EXCLUDED.plant,
                    modified=EXCLUDED.modified,
                    modified_by=EXCLUDED.modified_by,
                    unit_alias_name=EXCLUDED.unit_alias_name,
                    is_deleted=EXCLUDED.is_deleted
            """
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("AreaId")), _s(r.get("UnitName")),
                    _s(r.get("Plant")), _s(r.get("Created")), _s(r.get("CreatedBy")),
                    int(r.get("IsDeleted") or 0),
                    _s(r.get("Modified")), _s(r.get("ModifiedBy")),
                    _s(r.get("UnitAliasName")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback(); raise e
    finally:
        release_conn(conn)

# ─── VW JOBLIST WO ───────────────────────────────────────────
def bulk_replace_vw_joblist_wo(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM vw_joblist_wo")
            sql = """
                INSERT INTO vw_joblist_wo
                (plant, equipment_no, disiplin, joblist_description,
                 planning_jasa_status, planning_material_status, code_name, is_lldii,
                 "order", notification, created_on, superior_order, description,
                 functional_loc, location, revision, system_status, user_status,
                 wbs_ord_header, total_plnnd_costs, totalact_costs,
                 planner_group, main_work_ctr, change_by,
                 bas_start_date, basic_fin_date, actual_release,
                 cost_center, entered_by)
                VALUES %s
            """
            def _i(v):
                try: return int(float(v)) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None
                except: return None

            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Plant")), _s(r.get("EquipmentNo")),
                    _s(r.get("Disiplin")), _s(r.get("JoblistDescription")),
                    _s(r.get("PlanningJasaStatusName")), _s(r.get("PlanningMaterialStatusName")),
                    _s(r.get("CodeName")), _i(r.get("IsLLDII")),
                    _s(r.get("Order")), _s(r.get("Notification")), _s(r.get("CreatedOn")),
                    _s(r.get("SuperiorOrder")), _s(r.get("Description")),
                    _s(r.get("FunctionalLoc")), _s(r.get("Location")),
                    _s(r.get("Revision")), _s(r.get("SystemStatus")), _s(r.get("UserStatus")),
                    _s(r.get("WBSordheader")),
                    _n(r.get("TotalPlnndCosts")), _n(r.get("Totalactcosts")),
                    _s(r.get("PlannerGroup")), _s(r.get("MainWorkCtr")), _s(r.get("ChangeBy")),
                    _s(r.get("Basstartdate")), _s(r.get("Basicfindate")), _s(r.get("ActualRelease")),
                    _s(r.get("CostCenter")), _s(r.get("EnteredBy")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback(); raise e
    finally:
        release_conn(conn)


# ─── VW JOBLIST DETAIL ───────────────────────────────────────
def bulk_replace_vw_joblist_detail(df: pd.DataFrame) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM vw_joblist_detail")
            sql = """
                INSERT INTO vw_joblist_detail
                (id, joblist_id, joblist_detail_desc, reason_name, doc_type_name,
                 no_document, is_mechanical_integrity, job_discipline_name,
                 nomor_pm, notes, plant, created, creator_name, creator_job_title,
                 is_deleted, joblist_description, no_joblist,
                 project_number, project_type_code, project_type_name,
                 start_date, finish_date, revision, description, project_status,
                 equipment_no, area_name, area_alias_name, unit_name, unit_alias_name,
                 functional_location, location, disiplin,
                 criticallity, criticallity_text, main_work_center,
                 is_all_in, is_jasa, is_lldii, is_material,
                 code_name, planning_jasa_status, planning_material_status,
                 lldi_status, is_freezing)
                VALUES %s
            """
            def _i(v):
                try: return int(float(v)) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None
                except: return None

            rows = []
            for _, r in df.iterrows():
                rows.append((
                    _s(r.get("Id")), _s(r.get("JoblistId")),
                    _s(r.get("JoblistDetailDescription")), _s(r.get("JoblistDetailReasonName")),
                    _s(r.get("DocumentJoblistTypeName")), _s(r.get("NoDocument")),
                    _i(r.get("IsMechanicalIntegrity")), _s(r.get("JobDisciplineName")),
                    _s(r.get("NomorPM")), _s(r.get("Notes")),
                    _s(r.get("Plant")), _s(r.get("Created")),
                    _s(r.get("CreatorName")), _s(r.get("CreatorJobTitle")),
                    _i(r.get("IsDeleted")) or 0,
                    _s(r.get("JoblistDescription")), _s(r.get("NoJoblist")),
                    _s(r.get("ProjectNumber")), _s(r.get("ProjectTypeCode")), _s(r.get("ProjectTypeName")),
                    _s(r.get("StartDate")), _s(r.get("FinishDate")),
                    _s(r.get("Revision")), _s(r.get("Description")), _s(r.get("ProjectStatus")),
                    _s(r.get("EquipmentNo")),
                    _s(r.get("AreaName")), _s(r.get("AreaAliasName")),
                    _s(r.get("UnitName")), _s(r.get("UnitAliasName")),
                    _s(r.get("FunctionalLocation")), _s(r.get("Location")),
                    _s(r.get("Disiplin")), _s(r.get("Criticallity")), _s(r.get("CriticallityText")),
                    _s(r.get("MainWorkCenter")),
                    _i(r.get("IsAllIn")), _i(r.get("IsJasa")),
                    _i(r.get("IsLLDII")), _i(r.get("IsMaterial")),
                    _s(r.get("CodeName")),
                    _s(r.get("PlanningJasaStatusName")), _s(r.get("PlanningMaterialStatusName")),
                    _s(r.get("LLDI status")), _i(r.get("isFreezing")),
                ))
            execute_values(cur, sql, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback(); raise e
    finally:
        release_conn(conn)