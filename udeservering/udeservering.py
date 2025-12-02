from flask import Blueprint, render_template, request, jsonify, current_app
from sqlalchemy import text
import datetime

MONTH_ORDER = {
    "Januar": 1, "Februar": 2, "Marts": 3, "April": 4,
    "Maj": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "December": 12
}

current_month_num = datetime.date.today().month

# All Danish month names that are >= current month
future_months = [m for m, num in MONTH_ORDER.items() if num >= current_month_num]

# SQL: (MaanederIndevaerende LIKE '%"Maj"%' OR MaanederIndevaerende LIKE '%"Juni"%')
future_month_like_sql = " OR ".join(
    [f"MaanederIndevaerende LIKE '%\"{m}\"%'" for m in future_months]
)

# For fremtidige år – any non-null OR any containing months
future_years_like_sql = "MaanederFremtidige IS NOT NULL AND MaanederFremtidige <> ''"

udeservering_bp = Blueprint("udeservering", __name__, template_folder="templates")

def get_engine():
    return current_app.config["ENGINE"]


# --------------------
# Page routes
# --------------------
@udeservering_bp.route("/applications")
def udeservering_applications_page():
    return render_template("applications.html", page_title="applications faktureringer")

@udeservering_bp.route("/til-fakturering")
def udeservering_tilfakturering_page():
    return render_template("tilfakturering.html", page_title="Til fakturering")

@udeservering_bp.route("/faktureret")
def udeservering_faktureret_page():
    return render_template("faktureret.html", page_title="Faktureret")

@udeservering_bp.route("/statistik")
def udeservering_statistik_page():
    return render_template("statistik.html", page_title="Statistik")


# --------------------
# API endpoints
# --------------------
@udeservering_bp.route("/api/applications")
def api_udeservering_applications():
    engine = get_engine()

    limit = int(request.args.get("limit", 25))
    offset = int(request.args.get("offset", 0))
    search = request.args.get("search", "")
    sort = request.args.get("sort", "Ansogningsdato")
    order = request.args.get("order", "desc")
    filter_mode = request.args.get("filter", "applications")  # <-- NEW

    valid_sort_columns = {
        "Id","Firmanavn","Adresse","CVR","Serveringszone",
        "Lokation","Serveringsareal","Facadelaengde",
        "Periodetype","Ansogningsdato"
    }
    if sort not in valid_sort_columns:
        sort = "Ansogningsdato"

    search_filter = ""
    params = {"limit": limit, "offset": offset}
    if search:
        search_filter = """
            AND (
                Firmanavn LIKE :search OR
                Adresse LIKE :search OR
                CVR LIKE :search OR
                Serveringszone LIKE :search OR
                Lokation LIKE :search
            )
        """
        params["search"] = f"%{search}%"

    # ------------------------------
    # Eligibility SQL block
    # ------------------------------
    ELIGIBLE_SQL = f"""
        (
            -- Indeværende år
            (
                Periodetype = 'Indeværende år'
                AND ({future_month_like_sql})
            )
            OR

            -- Fremtidige år
            (Periodetype = 'Fremtidige år')

            OR

            -- Indeværende og fremtidige år
            (
                Periodetype = 'Indeværende og fremtidige år'
                AND (
                    ({future_month_like_sql})
                    OR ({future_years_like_sql})
                )
            )
        )
        """


    # ------------------------------
    # Dynamic WHERE based on dropdown
    # ------------------------------
    if filter_mode == "aktive":
        where_sql = f"WHERE {ELIGIBLE_SQL}"
    elif filter_mode == "inaktive":
        where_sql = f"WHERE NOT {ELIGIBLE_SQL}"
    else:  # alle
        where_sql = ""

    query = f"""
        SELECT *
        FROM BrugAarhus_Udeservering
        {where_sql}
        {search_filter}
        ORDER BY {sort} {order}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    count_query = f"""
        SELECT COUNT(*) AS cnt
        FROM BrugAarhus_Udeservering
        {where_sql}
        {search_filter}
    """

    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        total = conn.execute(text(count_query), params).scalar()

    return jsonify({"total": total, "rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/til-fakturering")
def api_udeservering_tilfakturering():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT * FROM BrugAarhus_Udeservering
            WHERE FakturaStatus = 'TilFakturering'
            ORDER BY Id DESC
        """)).mappings().all()
    return jsonify({"total": len(rows), "rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/faktureret")
def api_udeservering_faktureret():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT * FROM BrugAarhus_Udeservering
            WHERE FakturaStatus = 'Faktureret'
            ORDER BY Id DESC
        """)).mappings().all()
    return jsonify({"total": len(rows), "rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/set-status", methods=["POST"])
def api_udeservering_set_status():
    data = request.get_json() or {}
    record_id = data.get("Id")
    new_status = data.get("NewStatus")

    if not record_id or not new_status:
        return jsonify({"success": False, "error": "Manglende Id eller NewStatus"}), 400

    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE BrugAarhus_Udeservering
            SET FakturaStatus = :status
            WHERE Id = :id
        """), {"status": new_status, "id": record_id})
        success = result.rowcount > 0

    return jsonify({"success": success})


@udeservering_bp.route("/api/statistik/table")
def api_udeservering_statistik_table():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT Id, Firmanavn, Adresse, CVR, COALESCE(FakturaStatus, 'Ny') AS FakturaStatus
            FROM BrugAarhus_Udeservering
            ORDER BY Id DESC
        """)).mappings().all()
    return jsonify({"total": len(rows), "rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/statistik/metrics")
def api_udeservering_statistik_metrics():
    engine = get_engine()
    with engine.begin() as conn:
        stats = conn.execute(text("""
            SELECT COALESCE(FakturaStatus, 'Ny') AS Status, COUNT(*) AS Cnt
            FROM BrugAarhus_Udeservering
            GROUP BY COALESCE(FakturaStatus, 'Ny')
        """)).mappings().all()

        totals = conn.execute(text("""
            SELECT COUNT(*) AS Rows, COUNT(DISTINCT Firmanavn) AS Firms
            FROM BrugAarhus_Udeservering
        """)).mappings().first()

    status_counts = {
        "Ny": 0, "TilFakturering": 0, "Faktureret": 0, "FakturerIkke": 0
    }
    for row in stats:
        status_counts[row["Status"]] = row["Cnt"]

    return jsonify({
        "status": status_counts,
        "totals": {"rows": totals["Rows"], "firms": totals["Firms"]}
    })
