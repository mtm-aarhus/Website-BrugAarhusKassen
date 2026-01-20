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


udeservering_bp = Blueprint("udeservering", __name__, template_folder="templates")

def get_engine():
    return current_app.config["ENGINE"]

@lru_cache(maxsize=100)
def load_prisdata_for_year(year: int):
    engine = get_engine()

    with engine.begin() as conn:

        # PARAMETRE
        params = {
            r["Noegle"]: (r["VaerdiDecimal"] if r["VaerdiDecimal"] is not None else r["VaerdiTekst"])
            for r in conn.execute(text("""
                SELECT Noegle, VaerdiDecimal, VaerdiTekst
                FROM BrugAarhus_Udeservering_Parametre
                WHERE [Year] = :y
            """), {"y": year}).mappings().all()
        }

        # TAKSTER (keyed by uppercase ZoneKode)
        takster = {
            r["ZoneKode"].upper(): dict(r)
            for r in conn.execute(text("""
                SELECT ZoneKode,
                       SommerPrisPrM2,
                       VinterPrisPrM2,
                       PSPElment,
                       MaterialeNr
                FROM BrugAarhus_Udeservering_Takster
                WHERE [Year] = :y
            """), {"y": year}).mappings().all()
        }

        # SÆSON (month → "Sommer"/"Vinter")
        saeson = {
            r["MaanedNr"]: r["Saeson"]
            for r in conn.execute(text("""
                SELECT MaanedNr, Saeson
                FROM BrugAarhus_Udeservering_Saeson
                WHERE [Year] = :y
            """), {"y": year}).mappings().all()
        }

    return {
        "params": params,
        "takster": takster,
        "saeson": saeson
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
    filter_mode = request.args.get("filter", "aktive")  # aktive | inaktive | alle

    valid_sort_columns = {
        "Id",
        "Firmanavn",
        "Adresse",
        "CVR",
        "Geo",
        "Serveringszone",
        "Lokation",
        "Ansogningsdato",
        "Serveringsareal",
        "Facadelaengde",
        "LokationOptionId",
        "ArealVarierer",
        "GaeldendeFra",
        "GaeldendeTilOgMed",
    }
    if sort not in valid_sort_columns:
        sort = "Ansogningsdato"

    if order.lower() not in ("asc", "desc"):
        order = "desc"

    params = {"limit": limit, "offset": offset}

    search_filter = ""
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

    # Month-granular "active now" check:
    # active if:
    #   GaeldendeFra month <= current month
    #   AND (GaeldendeTilOgMed is null OR current month <= GaeldendeTilOgMed month)
    active_expr = """
        (
            DATEFROMPARTS(YEAR(GaeldendeFra), MONTH(GaeldendeFra), 1)
            <= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
        )
        AND
        (
            GaeldendeTilOgMed IS NULL
            OR DATEFROMPARTS(YEAR(GaeldendeTilOgMed), MONTH(GaeldendeTilOgMed), 1)
               >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
        )
    """

    if filter_mode == "aktive":
        where_sql = f"WHERE {active_expr}"
    elif filter_mode == "inaktive":
        where_sql = f"WHERE NOT {active_expr}"
    else:
        where_sql = "WHERE 1=1"

    query = f"""
        SELECT *
        FROM dbo.BrugAarhus_Udeservering
        {where_sql}
        {search_filter}
        ORDER BY {sort} {order}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    count_query = f"""
        SELECT COUNT(*) AS cnt
        FROM dbo.BrugAarhus_Udeservering
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
            calc = beregn_pris(zone, lokation, areal, facade, month_num, r["FakturaAar"])

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

        # ---------------------------------------------
        # Load rows as MUTABLE dicts
        # ---------------------------------------------
        placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
        params = {f"id{i}": idv for i, idv in enumerate(ids)}

        rows = [
            dict(r) for r in conn.execute(text(f"""
                SELECT *
                FROM BrugAarhus_Udeservering_Fakturalinjer
                WHERE FakturaLinjeID IN ({placeholders})
            """), params).mappings().all()
        ]

        # ---------------------------------------------
        # Process and calculate price
        # ---------------------------------------------
        for row in rows:
            status = row["FakturaStatus"]

            # Skip if already approved or invoiced
            if status in ("Faktureret", "TilFakturering"):
                continue

            zone = row["Serveringszone"]
            lokation = row["Lokation"]
            areal = float(row["Serveringsareal"] or 0)
            facade = float(row["Facadelaengde"] or 0)

            # Month → number
            month_name = row["FakturaMaaned"].split(" ")[0]
            month_num = MONTH_NAME_TO_NUM.get(month_name, 0)

            # NEW (YEAR support)
            year = int(row["FakturaAar"])

            # Updated yearly price lookup
            calc = beregn_pris(zone, lokation, areal, facade, month_num, year)

            pris = calc["belob"] if calc.get("ok") else None

            # Update DB
            conn.execute(text("""
                UPDATE BrugAarhus_Udeservering_Fakturalinjer
                SET Pris = :pris,
                    FakturaStatus = 'TilFakturering'
                WHERE FakturaLinjeID = :id
            """), {"pris": pris, "id": row["FakturaLinjeID"]})

        # ---------------------------------------------
        # Done
        # ---------------------------------------------
        return jsonify({"success": True})



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
    year = request.args.get("year", type=int)

    with engine.begin() as conn:

        # If UI requests only the list of available years
        if request.args.get("years_only"):
            years = conn.execute(text("""
                SELECT DISTINCT [Year] 
                FROM BrugAarhus_Udeservering_Parametre
                ORDER BY [Year]
            """)).fetchall()
            return jsonify({"years": [y[0] for y in years]})

        if not year:
            year = datetime.date.today().year

        rows = conn.execute(text("""
            SELECT Noegle, VaerdiDecimal, VaerdiTekst, [Year]
            FROM BrugAarhus_Udeservering_Parametre
            WHERE [Year] = :year
            ORDER BY Noegle
        """), {"year": year}).mappings().all()

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
                    VaerdiTekst   = :VaerdiTekst
                WHERE Noegle = :Noegle
                  AND [Year] = :Year
            """), r)

    load_prisdata_for_year.cache_clear()
    return jsonify({"success": True})

@udeservering_bp.route("/api/takster")
def api_takster():
    engine = get_engine()
    year = request.args.get("year", type=int)

    if not year:
        year = datetime.date.today().year

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT Id,
                   ZoneKode,
                   ZoneBeskrivelse,
                   PSPElment,
                   MaterialeNr,
                   SommerPrisPrM2,
                   VinterPrisPrM2,
                   [Year]
            FROM BrugAarhus_Udeservering_Takster
            WHERE [Year] = :year
            ORDER BY ZoneKode
        """), {"year": year}).mappings().all()

    return jsonify({"rows": [dict(r) for r in rows]})

@udeservering_bp.route("/api/takster/update/<int:id>", methods=["POST"])
def api_takster_update(id):
    data = request.get_json() or {}
    engine = get_engine()

    data["Id"] = id

    sql = text("""
        UPDATE BrugAarhus_Udeservering_Takster
        SET ZoneKode        = :ZoneKode,
            ZoneBeskrivelse = :ZoneBeskrivelse,
            PSPElment       = :PSPElment,
            MaterialeNr     = :MaterialeNr,
            SommerPrisPrM2  = :SommerPrisPrM2,
            VinterPrisPrM2  = :VinterPrisPrM2
        WHERE Id    = :Id
          AND [Year] = :Year
    """)

    with engine.begin() as conn:
        conn.execute(sql, data)

    load_prisdata_for_year.cache_clear()
    return jsonify({"success": True})

@udeservering_bp.route("/api/saeson")
def api_saeson():
    engine = get_engine()
    year = request.args.get("year", type=int)

    if not year:
        year = datetime.date.today().year

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT Id, MaanedNr, Maanedsnavn, Saeson, [Year]
            FROM BrugAarhus_Udeservering_Saeson
            WHERE [Year] = :year
            ORDER BY MaanedNr
        """), {"year": year}).mappings().all()

    return jsonify({"rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/saeson/update/<int:id>", methods=["POST"])
def api_saeson_update(id):
    data = request.get_json() or {}
    data["Id"] = id

    sql = text("""
        UPDATE BrugAarhus_Udeservering_Saeson
        SET MaanedNr    = :MaanedNr,
            Maanedsnavn = :Maanedsnavn,
            Saeson      = :Saeson
        WHERE Id    = :Id
          AND [Year] = :Year
    """)

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(sql, data)

    load_prisdata_for_year.cache_clear()
    return jsonify({"success": True})

@udeservering_bp.route("/api/statistik/table")
def api_udeservering_statistik_table():
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT
                Id,
                Firmanavn,
                Adresse,
                CVR,
                Serveringszone,
                Lokation,
                Serveringsareal,
                Facadelaengde,
                MaanederJson,
                GaeldendeFra,
                GaeldendeTilOgMed
            FROM dbo.BrugAarhus_Udeservering
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

def beregn_pris(zone, lokation, serveringsareal, facadelaengde, month, year):
    data = load_prisdata_for_year(year)

    params = data["params"]
    takster = data["takster"]
    saeson_map = data["saeson"]

    # Season (Sommer/Vinter)
    saeson = saeson_map.get(month)
    sommer = (saeson == "Sommer")

    # Zone must be upper-case since takster dict uses uppercase keys
    t = takster.get((zone or "").upper())
    if not t:
        return {"ok": False, "error": f"Zone-takst ikke fundet for zone '{zone}'."}

    pris_pr_m2 = float(t["SommerPrisPrM2"] if sommer else t["VinterPrisPrM2"])

    # Parameters (with fallbacks)
    facadebredde = float(params.get("Facadebredde i meter", 0.8))
    min_areal = float(params.get("Minimums opkrævningsareal", 1.0))
    min_belob = float(params.get("Minimums opkrævningsbeløb", 250.0))

    # Minimum area
    brutto = max(float(serveringsareal or 0), min_areal)

    # Facadefradrag
    if lokation == "Ved facade":
        netto = max(brutto - float(facadelaengde or 0) * facadebredde, 0)
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
    data = request.get_json() or {}

    zone = data.get("Zone")
    lokation = data.get("Lokation")
    areal = float(data.get("Serveringsareal") or 0)
    facade = float(data.get("Facadelaengde") or 0)
    month = int(data.get("Month") or 0)
    year = int(data.get("Year") or datetime.date.today().year)

    result = beregn_pris(zone, lokation, areal, facade, month, year)

    return jsonify({"success": result["ok"], "data": result})



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

@udeservering_bp.route("/api/year/clone", methods=["POST"])
def api_clone_year():
    engine = get_engine()

    with engine.begin() as conn:
        # Determine last year
        last_year = conn.execute(text("""
            SELECT MAX([Year]) FROM BrugAarhus_Udeservering_Parametre
        """)).scalar()

        new_year = last_year + 1

        # Clone parametre
        conn.execute(text("""
            INSERT INTO BrugAarhus_Udeservering_Parametre (Noegle, VaerdiDecimal, VaerdiTekst, [Year])
            SELECT Noegle, VaerdiDecimal, VaerdiTekst, :new_year
            FROM BrugAarhus_Udeservering_Parametre
            WHERE [Year] = :last_year
        """), {"new_year": new_year, "last_year": last_year})

        # Clone takster
        conn.execute(text("""
            INSERT INTO BrugAarhus_Udeservering_Takster
                (ZoneKode, ZoneBeskrivelse, PSPElment, MaterialeNr, SommerPrisPrM2, VinterPrisPrM2, [Year])
            SELECT ZoneKode, ZoneBeskrivelse, PSPElment, MaterialeNr,
                   SommerPrisPrM2, VinterPrisPrM2, :new_year
            FROM BrugAarhus_Udeservering_Takster
            WHERE [Year] = :last_year
        """), {"new_year": new_year, "last_year": last_year})

        # Clone saeson
        conn.execute(text("""
            INSERT INTO BrugAarhus_Udeservering_Saeson
                (MaanedNr, Maanedsnavn, Saeson, [Year])
            SELECT MaanedNr, Maanedsnavn, Saeson, :new_year
            FROM BrugAarhus_Udeservering_Saeson
            WHERE [Year] = :last_year
        """), {"new_year": new_year, "last_year": last_year})

    return jsonify({"success": True, "new_year": new_year})
