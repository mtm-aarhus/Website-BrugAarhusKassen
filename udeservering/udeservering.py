from flask import Blueprint, render_template, request, jsonify, current_app
from sqlalchemy import text
import datetime
from functools import lru_cache
import requests
import os

MONTH_ORDER = {
    "Januar": 1, "Februar": 2, "Marts": 3, "April": 4,
    "Maj": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "December": 12
}

MONTH_NAME_TO_NUM = {
    m: i for m, i in [
        ("Januar",1),("Februar",2),("Marts",3),("April",4),
        ("Maj",5),("Juni",6),("Juli",7),("August",8),
        ("September",9),("Oktober",10),("November",11),("December",12),
    ]
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

@lru_cache(maxsize=1)
def load_prisdata():
    engine = get_engine()

    with engine.begin() as conn:
        # Load parameters
        params = conn.execute(text("""
            SELECT Noegle, VaerdiDecimal, VaerdiTekst
            FROM BrugAarhus_Udeservering_Parametre
        """)).mappings().all()

        # Load takster
        takster = conn.execute(text("""
            SELECT ZoneKode,
                   SommerPrisPrM2,
                   VinterPrisPrM2,
                   PSPElment,
                   MaterialeNr
            FROM BrugAarhus_Udeservering_Takster
        """)).mappings().all()

        # Load season
        saeson = conn.execute(text("""
            SELECT MaanedNr, Saeson
            FROM BrugAarhus_Udeservering_Saeson
        """)).mappings().all()

    return {
        "params": {p["Noegle"]: p["VaerdiDecimal"] for p in params},
        "takster": {t["ZoneKode"].upper(): dict(t) for t in takster},
        "saeson": {s["MaanedNr"]: s["Saeson"] for s in saeson}
    }


# --------------------
# Page routes
# --------------------
@udeservering_bp.route("/applications")
def applications():
    return render_template("applications.html", page_title="Ansøgninger")

@udeservering_bp.route("/fakturering")
def fakturering():
    return render_template("fakturering.html", page_title="Fakturering")

@udeservering_bp.route("/fakturer_ikke")
def fakturer_ikke():
    return render_template("fakturer_ikke.html", page_title="Fakturer ikke")


@udeservering_bp.route("/til_fakturering")
def til_fakturering_page():
    return render_template("til_fakturering.html", page_title="Til fakturering")


@udeservering_bp.route("/faktureret")
def faktureret_page():
    return render_template("faktureret.html", page_title="Faktureret")


@udeservering_bp.route("/statistik")
def statistik():
    return render_template("statistik.html", page_title="Statistik")

@udeservering_bp.route("/parametre")
def parametre_page():
    return render_template("parametre.html", page_title="Parametre")



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


@udeservering_bp.route("/api/fakturering")
def api_fakturering():
    status = request.args.get("status", "Ny")
    limit = int(request.args.get("limit", 25))
    offset = int(request.args.get("offset", 0))
    search = request.args.get("search", "")

    # NEW: Sorting support
    sort = request.args.get("sort", "FakturaAar")
    order = request.args.get("order", "asc")

    valid_sort_columns = {
        "FakturaDatoSort",
        "FakturaLinjeID",
        "DeskproID",
        "Firmanavn",
        "Adresse",
        "FakturaMaaned",
        "FakturaAar",
        "Lokation",
        "Serveringsareal",
        "Facadelaengde"
    }
    if sort not in valid_sort_columns:
        sort = "FakturaAar"

    if order.lower() not in ("asc", "desc"):
        order = "asc"

    engine = get_engine()

    base_where = "WHERE 1=1"
    params = {"limit": limit, "offset": offset}

    if status:
        base_where += " AND FakturaStatus = :status"
        params["status"] = status

    if search:
        base_where += """
            AND (
                  Firmanavn LIKE :search
               OR Adresse LIKE :search
               OR DeskproID LIKE :search
            )
        """
        params["search"] = f"%{search}%"

    query = f"""
        SELECT *
        FROM BrugAarhus_Udeservering_Fakturalinjer
        {base_where}
        ORDER BY {sort} {order}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    count_query = f"""
        SELECT COUNT(*) 
        FROM BrugAarhus_Udeservering_Fakturalinjer
        {base_where}
    """

    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        total = conn.execute(text(count_query), params).scalar()
        final_rows = []
        for r in rows:
            r = dict(r)  # make row mutable

            status = r.get("FakturaStatus", "")

            # A) If already priced and approved, NEVER recalc
            if status in ("Faktureret", "TilFakturering"):
                final_rows.append(r)
                continue

            # Extract needed fields
            zone = r.get("Serveringszone")
            lokation = r.get("Lokation")
            areal = float(r.get("Serveringsareal") or 0)
            facade = float(r.get("Facadelaengde") or 0)

            # Convert month name -> int
            month_name = r["FakturaMaaned"].split(" ")[0]
            month_num = MONTH_NAME_TO_NUM.get(month_name, 0)


            # Calculate price
            calc = beregn_pris(zone, lokation, areal, facade, month_num)

            if calc["ok"]:
                r["Pris"] = calc["belob"]
            else:
                r["Pris"] = None

            final_rows.append(r)

    return jsonify({"total": total, "rows": final_rows})

@udeservering_bp.route("/api/fakturering/reset", methods=["POST"])
def api_fakturering_reset():
    data = request.get_json()
    fid = data.get("FakturaLinjeID")

    if not fid:
        return jsonify({"success": False, "error": "Mangler ID"})

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM BrugAarhus_Udeservering_Fakturalinjer WHERE FakturaLinjeID = :id"),
            {"id": fid}
        )

    return jsonify({"success": True})

@udeservering_bp.route("/api/fakturering/bulk_status", methods=["POST"])
def api_fakturering_bulk_status():
    data = request.get_json() or {}
    ids = data.get("ids") or []
    action = data.get("Action")

    if not ids:
        return jsonify({"success": False, "error": "Ingen IDs modtaget."})

    status_map = {
        "godkend": "TilFakturering",
        "ikke": "FakturerIkke",
        "save": "Ny",
    }
    new_status = status_map.get(action)
    if not new_status:
        print(data)
        return jsonify({"success": False, "error": "Ugyldig handling"}), 400

    engine = get_engine()

    placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
    params = {f"id{i}": idv for i, idv in enumerate(ids)}

    sql = text(f"""
        UPDATE BrugAarhus_Udeservering_Fakturalinjer
        SET FakturaStatus = :status
        WHERE FakturaLinjeID IN ({placeholders})
    """)

    params["status"] = new_status

    with engine.begin() as conn:
        conn.execute(sql, params)

    return jsonify({"success": True})

@udeservering_bp.route("/api/fakturering/bulk_godkend", methods=["POST"])
def api_fakturering_bulk_godkend():
    data = request.get_json() or {}
    ids = data.get("ids") or []

    if not ids:
        return jsonify({"success": False, "error": "Ingen IDs modtaget."})

    engine = get_engine()

    with engine.begin() as conn:

        # Fetch all rows first
        placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
        params = {f"id{i}": idv for i, idv in enumerate(ids)}

        rows = conn.execute(text(f"""
            SELECT *
            FROM BrugAarhus_Udeservering_Fakturalinjer
            WHERE FakturaLinjeID IN ({placeholders})
        """), params).mappings().all()

        # Calculate price per row
        for r in rows:
            status = r["FakturaStatus"]

            # Skip recalculation for approved or completed rows
            if status in ("Faktureret", "TilFakturering"):
                continue

            # Extract fields
            zone = r["Serveringszone"]
            lokation = r["Lokation"]
            areal = float(r["Serveringsareal"] or 0)
            facade = float(r["Facadelaengde"] or 0)

            # Extract month fast
            month_name = r["FakturaMaaned"].split(" ")[0]
            month_num = MONTH_NAME_TO_NUM.get(month_name, 0)

            # Calculate price
            calc = beregn_pris(zone, lokation, areal, facade, month_num)
            r["Pris"] = calc["belob"] if calc["ok"] else None

        # rows is already the final list
        return jsonify({"total": total, "rows": rows})



@udeservering_bp.route("/api/fakturering/reset_bulk", methods=["POST"])
def api_fakturering_reset_bulk():
    data = request.get_json() or {}
    ids = data.get("ids") or []

    if not ids:
        return jsonify({"success": False, "error": "Ingen IDs modtaget."})

    engine = get_engine()

    placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
    params = {f"id{i}": idv for i, idv in enumerate(ids)}

    sql = text(f"""
        DELETE FROM BrugAarhus_Udeservering_Fakturalinjer
        WHERE FakturaLinjeID IN ({placeholders})
    """)

    with engine.begin() as conn:
        conn.execute(sql, params)

    return jsonify({"success": True, "deleted": len(ids)})



@udeservering_bp.route("/api/fakturering/<int:id>")
def api_fakturering_get(id):
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM BrugAarhus_Udeservering_Fakturalinjer WHERE FakturaLinjeID = :id"),
            {"id": id}
        ).mappings().first()

    if not row:
        return jsonify({"success": False, "error": "Fakturalinje ikke fundet"}), 404

    return jsonify({"success": True, "data": dict(row)})

@udeservering_bp.route("/api/fakturering/update", methods=["POST"])
def api_fakturering_update():
    data = request.get_json()
    
    fid = data.get("FakturaLinjeID")
    action = data.get("Action")

    if not fid:
        return jsonify({"success": False, "error": "Mangler FakturaLinjeID"})

    # Only editable fields
    editable_fields = {
        "Serveringsareal": data.get("Serveringsareal"),
        "Facadelaengde": data.get("Facadelaengde"),
        "Kommentar": data.get("Kommentar"),
        "Lokation": data.get("Lokation"),
    }

    # Remove None values so they don't overwrite DB
    editable_fields = {k: v for k, v in editable_fields.items() if v is not None}

    pris = data.get("Pris")

    status_map = {
        "save": "Ny",
        "godkend": "TilFakturering",
        "ikke": "FakturerIkke"
    }

    new_status = status_map.get(action)
    if not new_status:
        return jsonify({"success": False, "error": "Ugyldig handling"})

    engine = get_engine()

    # Build SET clause dynamically
    set_clause = ", ".join(f"{k} = :{k}" for k in editable_fields.keys())
    params = editable_fields.copy()
    params["id"] = fid
    params["status"] = new_status

    # Only write PRIS when godkend is used
    if action == "godkend":
        set_clause += ", Pris = :Pris"
        params["Pris"] = pris

    # Status is always updated
    set_clause += ", FakturaStatus = :status"

    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE BrugAarhus_Udeservering_Fakturalinjer
            SET {set_clause}
            WHERE FakturaLinjeID = :id
        """), params)

    return jsonify({"success": True})

@udeservering_bp.route("/api/parametre")
def api_parametre_list():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT Noegle, VaerdiDecimal, VaerdiTekst
            FROM BrugAarhus_Udeservering_Parametre
            ORDER BY Noegle
        """)).mappings().all()

    return jsonify({"rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/parametre/update", methods=["POST"])
def api_parametre_update():
    data = request.get_json() or {}

    rows = data.get("rows", [])
    engine = get_engine()

    with engine.begin() as conn:
        for r in rows:
            conn.execute(text("""
                UPDATE BrugAarhus_Udeservering_Parametre
                SET VaerdiDecimal = :VaerdiDecimal,
                    VaerdiTekst = :VaerdiTekst
                WHERE Noegle = :Noegle
            """), r)
    load_prisdata.cache_clear()
    return jsonify({"success": True})

@udeservering_bp.route("/api/takster")
def api_takster():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT Id,
                   ZoneKode,
                   ZoneBeskrivelse,
                   PSPElment,
                   MaterialeNr,
                   SommerPrisPrM2,
                   VinterPrisPrM2
            FROM BrugAarhus_Udeservering_Takster
            ORDER BY ZoneKode
        """)).mappings().all()

    return jsonify({"rows": [dict(r) for r in rows]})

@udeservering_bp.route("/api/takster/update/<int:id>", methods=["POST"])
def api_takster_update(id):
    data = request.get_json() or {}
    engine = get_engine()

    sql = text("""
        UPDATE BrugAarhus_Udeservering_Takster
        SET ZoneKode        = :ZoneKode,
            ZoneBeskrivelse = :ZoneBeskrivelse,
            PSPElment       = :PSPElment,
            MaterialeNr     = :MaterialeNr,
            SommerPrisPrM2  = :SommerPrisPrM2,
            VinterPrisPrM2  = :VinterPrisPrM2
        WHERE Id = :Id
    """)

    data["Id"] = id

    with engine.begin() as conn:
        conn.execute(sql, data)

    load_prisdata.cache_clear()

    return jsonify({"success": True})


@udeservering_bp.route("/api/saeson")
def api_saeson():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT Id, MaanedNr, Maanedsnavn, Saeson
            FROM BrugAarhus_Udeservering_Saeson
            ORDER BY MaanedNr
        """)).mappings().all()
    return jsonify({"rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/saeson/update/<int:id>", methods=["POST"])
def api_saeson_update(id):
    data = request.get_json() or {}
    sql = text("""
        UPDATE BrugAarhus_Udeservering_Saeson
        SET MaanedNr = :MaanedNr,
            Maanedsnavn = :Maanedsnavn,
            Saeson = :Saeson
        WHERE Id = :Id
    """)
    data["Id"] = id
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(sql, data)
    load_prisdata.cache_clear()

    return jsonify({"success": True})


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

def month_from_name(name: str) -> int:
    months = {
        "Januar": 1, "Februar": 2, "Marts": 3, "April": 4,
        "Maj": 5, "Juni": 6, "Juli": 7, "August": 8,
        "September": 9, "Oktober": 10, "November": 11, "December": 12
    }
    return months.get(name, 0)

def beregn_pris(zone, lokation, serveringsareal, facadelaengde, month):
    data = load_prisdata()

    params = data["params"]
    takster = data["takster"]
    saeson_map = data["saeson"]

    # Get season
    saeson = saeson_map.get(month)
    sommer = (saeson == "Sommer")

    # Get takst
    t = takster.get((zone).upper())
    if not t:
        return {"ok": False, "error": "Zone-takst ikke fundet."}

    pris_pr_m2 = float(t["SommerPrisPrM2"] if sommer else t["VinterPrisPrM2"])

    # Parameters
    facadebredde = float(params.get("Facadebredde i meter", 0.8))
    min_areal = float(params.get("Minimums opkrævningsareal", 1.0))
    min_belob = float(params.get("Minimums opkrævningsbeløb", 250.0))

    # Minimum areal
    brutto = max(serveringsareal, min_areal)

    # Facadefradrag
    if lokation == "Ved facade":
        netto = max(brutto - facadelaengde * facadebredde, 0)
    else:
        netto = brutto

    beloeb_raw = netto * pris_pr_m2
    beloeb = max(beloeb_raw, min_belob)

    return {
        "ok": True,
        "sommer": sommer,
        "pris_pr_m2": round(pris_pr_m2, 2),
        "faktureret_areal": round(netto, 2),
        "brutto_areal": round(brutto, 2),
        "belob": round(beloeb, 2),
        "minimum_applied": beloeb == min_belob and beloeb_raw < min_belob
    }


@udeservering_bp.route("/api/beregn_pris", methods=["POST"])
def api_beregn_pris():
    data = request.get_json()

    zone = data.get("Zone")
    lokation = data.get("Lokation")
    areal = float(data.get("Serveringsareal") or 0)
    facade = float(data.get("Facadelaengde") or 0)
    month = int(data.get("Month") or 0)

    result = beregn_pris(zone, lokation, areal, facade, month)

    if not result["ok"]:
        return jsonify({"success": False, "error": result["error"]})

    return jsonify({"success": True, "data": result})


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

@udeservering_bp.route("/api/run_refresh", methods=["POST"])
def api_run_refresh():
    url = "https://pyorchestrator.aarhuskommune.dk/api/trigger"

    payload = {
        "trigger_name": "BrugAarhusRefreshWebsiteTrigger",
        "process_status": "IDLE"
    }

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": os.getenv("PyOrchestratorAPIKey")
    }

    r = requests.post(url, json=payload, headers=headers)
    return jsonify({"success": True, "result": r.json()}), r.status_code
