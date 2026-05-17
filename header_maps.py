"""
header_maps.py — Normalisasi nama kolom dari berbagai format Excel
"""

TAEX_HEADER_MAP = {
    'plpl': 'Plant', 'pl': 'Plant', 'plant': 'Plant',
    'equipment': 'Equipment',
    'order': 'Order',
    'reserv.no.': 'Reservno', 'reserv no': 'Reservno', 'reservno': 'Reservno',
    'reserv_no': 'Reservno', 'reservation no': 'Reservno',
    'revision': 'Revision',
    'material': 'Material',
    'itm': 'Itm', 'item no': 'Itm',
    'material description': 'Material_Description',
    'material_description': 'Material_Description',
    'reqmt qty': 'Qty_Reqmts', 'qty_reqmts': 'Qty_Reqmts',
    'reqmts qty': 'Qty_Reqmts', 'qty reqmts': 'Qty_Reqmts',
    'qty_stock': 'Qty_Stock', 'qty stock': 'Qty_Stock', 'qty_stock_onhand': 'Qty_Stock',
    'pr': 'PR', 'purchase req.no.': 'PR', 'purchreq': 'PR',
    'item': 'Item', 'it': 'Item',
    'qty_pr': 'Qty_PR', 'qty pr': 'Qty_PR',
    'cost ctrs': 'Cost_Ctrs', 'cost_ctrs': 'Cost_Ctrs', 'costctrs': 'Cost_Ctrs',
    'cost center': 'Cost_Ctrs', 'cost ctr': 'Cost_Ctrs',
    'sloc': 'SLoc', 'storage location': 'SLoc',
    'del': 'Del', 'deletion indicator': 'Del',
    'fis': 'FIs', 'fi': 'FIs',
    'ict': 'Ict', 'ic': 'Ict', 'ict.': 'Ict',
    'pg': 'PG',
    'recipient': 'Recipient',
    'unloading point': 'Unloading_point', 'unloading_point': 'Unloading_point',
    'reqmt date': 'Reqmts_Date', 'reqmts date': 'Reqmts_Date',
    'reqmts_date': 'Reqmts_Date', 'requirements date': 'Reqmts_Date',
    'qty. f. avail.check': 'Qty_f_avail_check', 'qty_f_avail_check': 'Qty_f_avail_check',
    'qty f avail check': 'Qty_f_avail_check', 'qty avail': 'Qty_f_avail_check',
    'qty withdrawn': 'Qty_Withdrawn', 'qty_withdrawn': 'Qty_Withdrawn',
    'bun': 'UoM', 'uom': 'UoM', 'un': 'UoM', 'base unit': 'UoM',
    'g/l acct': 'GL_Acct', 'gl_acct': 'GL_Acct', 'gl acct': 'GL_Acct',
    'price': 'Res_Price', 'res_price': 'Res_Price', 'res price': 'Res_Price',
    'per': 'Res_per', 'res_per': 'Res_per', 'res per': 'Res_per',
    'crcy': 'Res_Curr', 'res_curr': 'Res_Curr', 'currency': 'Res_Curr',
    'po': 'PO', 'purchase order': 'PO',
    'po_date': 'PO_Date', 'po date': 'PO_Date',
    'qty_deliv': 'Qty_Deliv', 'qty deliv': 'Qty_Deliv',
    'delivery_date': 'Delivery_Date', 'delivery date': 'Delivery_Date',
}

SAP_HEADER_MAP = {
    'plnt': 'Plant', 'plant': 'Plant',
    'purch.req.': 'PR', 'purch req': 'PR', 'purchreq': 'PR', 'pr': 'PR',
    'purchase req.no.': 'PR', 'purchase request': 'PR',
    'item': 'Item',
    'material': 'Material',
    'material description': 'Material_Description',
    'material_description': 'Material_Description', 'short text': 'Material_Description',
    'd': 'D', 'rel': 'R', 'r': 'R',
    'pgr': 'PGr', 'purch. group': 'PGr', 'purch group': 'PGr',
    's': 'S',
    'trackingno': 'TrackingNo', 'tracking_no': 'TrackingNo', 'tracking no': 'TrackingNo',
    'qty requested': 'Qty_PR', 'qty_pr': 'Qty_PR', 'qty_purchreq': 'Qty_PR',
    'qty purchreq': 'Qty_PR', 'quantity': 'Qty_PR',
    'un': 'Un', 'uom': 'Un', 'unit': 'Un',
    'req.date': 'Req_Date', 'req date': 'Req_Date', 'req_date': 'Req_Date',
    'reqdate': 'Req_Date', 'requirements date': 'Req_Date',
    'valn price': 'Valn_price', 'valn_price': 'Valn_price', 'valuation price': 'Valn_price',
    'crcy': 'PR_Curr', 'pr_curr': 'PR_Curr', 'currency': 'PR_Curr', 'pr curr': 'PR_Curr',
    'pr_per': 'PR_Per', 'pr per': 'PR_Per',
    'release dt': 'Release_Date', 'release_date': 'Release_Date', 'release date': 'Release_Date',
    'release dt.': 'Release_Date',
    'tracking': 'Tracking',
}

ORDER_HEADER_MAP = {
    'plant': 'Plant', 'order': 'Order',
    'superior order': 'Superior_Order', 'superior_order': 'Superior_Order',
    'notification': 'Notification',
    'created on': 'Created_On', 'created_on': 'Created_On', 'createdon': 'Created_On',
    'description': 'Description', 'revision': 'Revision', 'equipment': 'Equipment',
    'system status': 'System_Status', 'system_status': 'System_Status',
    'user status': 'User_Status', 'user_status': 'User_Status',
    'functional loc.': 'FunctLocation', 'functional loc': 'FunctLocation',
    'functlocation': 'FunctLocation', 'funct location': 'FunctLocation',
    'funct. location': 'FunctLocation', 'functional location': 'FunctLocation',
    'location': 'Location',
    'wbs ord. header': 'WBS_Ord_header', 'wbs ord header': 'WBS_Ord_header',
    'wbs_ord_header': 'WBS_Ord_header', 'wbsordheader': 'WBS_Ord_header',
    'cost center': 'CostCenter', 'costcenter': 'CostCenter', 'cost_center': 'CostCenter',
    'totalplnndcosts': 'Total_Plan_Cost', 'total plan cost': 'Total_Plan_Cost',
    'total_plan_cost': 'Total_Plan_Cost', 'total plnd costs': 'Total_Plan_Cost',
    'total act.costs': 'Total_Act_Cost', 'total act costs': 'Total_Act_Cost',
    'total_act_cost': 'Total_Act_Cost', 'totalactcosts': 'Total_Act_Cost',
    'planner group': 'Planner_Group', 'planner_group': 'Planner_Group',
    'main workctr': 'MainWorkCtr', 'main_workctr': 'MainWorkCtr', 'mainworkctr': 'MainWorkCtr',
    'main work ctr': 'MainWorkCtr', 'main work center': 'MainWorkCtr',
    'entered by': 'Entry_by', 'enteredby': 'Entry_by', 'entry_by': 'Entry_by', 'entry by': 'Entry_by',
    'changed by': 'Changed_by', 'changedby': 'Changed_by', 'changed_by': 'Changed_by',
    'bas. start date': 'Basic_start_date', 'bas start date': 'Basic_start_date',
    'basic start date': 'Basic_start_date', 'basic_start_date': 'Basic_start_date',
    'basic fin. date': 'Basic_finish_date', 'basic fin date': 'Basic_finish_date',
    'basic finish date': 'Basic_finish_date', 'basic_finish_date': 'Basic_finish_date',
    'actual release': 'Actual_Release', 'actual_release': 'Actual_Release',
}


def normalize_row(raw_row: dict, header_map: dict) -> dict:
    out = {}
    for k, v in raw_row.items():
        normalized = header_map.get(str(k).strip().lower())
        if normalized:
            out[normalized] = v
        else:
            out[k] = v
    return out


def normalize_taex(row): return normalize_row(row, TAEX_HEADER_MAP)
def normalize_sap(row):  return normalize_row(row, SAP_HEADER_MAP)
def normalize_order(row): return normalize_row(row, ORDER_HEADER_MAP)
