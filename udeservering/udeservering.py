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
    m: i for m, i in MONTH_ORDER.items()
}


udeservering_bp = Blueprint("udeservering", __name__, template_folder="templates")


# Friendly status -> page-key mapping (for active-tab highlighting in navbar)
PAGE_KEYS = {
    "applications": "applications",
    "til_godkendelse": "til_godkendelse",
    "godkendte_fakturaer": "godkendte_fakturaer",
    "faktureret": "faktureret",
    "fakturer_ikke": "fakturer_ikke",
    "statistik": "statistik",
    "parametre": "parametre",
}


def get_engine():
    return current_app.config["ENGINE"]


def _to_decimal_or_none(val):
    """Treat empty strings as NULL for numeric fields; pass through real numbers/strings."""
    if val is None:
        return None
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def is_valid_cvr(cvr) -> bool:
    """Danish CVR mod-11 check (weights 2,7,6,5,4,3,2,1)."""
    if cvr is None:
        return False
    digits = "".join(ch for ch in str(cvr) if ch.isdigit())
    if len(digits) != 8:
        return False
    weights = (2, 7, 6, 5, 4, 3, 2, 1)
    total = sum(int(d) * w for d, w in zip(digits, weights))
    return total % 11 == 0


@lru_cache(maxsize=100)
def load_prisdata_for_year(year: int):
    engine = get_engine()

    with engine.begin() as conn:

        params = {
            r["Noegle"]: (r["VaerdiDecimal"] if r["VaerdiDecimal"] is not None else r["VaerdiTekst"])
            for r in conn.execute(text("""
                SELECT Noegle, VaerdiDecimal, VaerdiTekst
                FROM BrugAarhus_Udeservering_Parametre
                WHERE [Year] = :y
            """), {"y": year}).mappings().all()
        }

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
    return render_template(
        "applications.html",
        page_title="Ansøgninger",
        page_key="applications",
    )


@udeservering_bp.route("/til_godkendelse")
@udeservering_bp.route("/fakturering")  # legacy alias
def til_godkendelse():
    return render_template(
        "til_godkendelse.html",
        page_title="Til godkendelse",
        page_key="til_godkendelse",
    )


@udeservering_bp.route("/godkendte_fakturaer")
@udeservering_bp.route("/til_fakturering")  # legacy alias
def godkendte_fakturaer():
    return render_template(
        "godkendte_fakturaer.html",
        page_title="Godkendte fakturaer",
        page_key="godkendte_fakturaer",
    )


@udeservering_bp.route("/faktureret")
def faktureret_page():
    return render_template(
        "faktureret.html",
        page_title="Faktureret",
        page_key="faktureret",
    )


@udeservering_bp.route("/fakturer_ikke")
def fakturer_ikke():
    return render_template(
        "fakturer_ikke.html",
        page_title="Fakturer ikke",
        page_key="fakturer_ikke",
    )


@udeservering_bp.route("/statistik")
def statistik():
    return render_template(
        "statistik.html",
        page_title="Statistik",
        page_key="statistik",
    )


@udeservering_bp.route("/parametre")
def parametre_page():
    return render_template(
        "parametre.html",
        page_title="Parametre & takster",
        page_key="parametre",
    )


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
    zone = request.args.get("zone", "")
    lokation = request.args.get("lokation", "")
    periodetype = request.args.get("periodetype", "")

    valid_sort_columns = {
        "Id", "Firmanavn", "Adresse", "CVR", "Geo",
        "Serveringszone", "Lokation", "Ansogningsdato",
        "Serveringsareal", "Facadelaengde", "LokationOptionId",
        "GaeldendeFra", "GaeldendeTilOgMed", "Periodetype",
    }
    if sort not in valid_sort_columns:
        sort = "Ansogningsdato"

    if order.lower() not in ("asc", "desc"):
        order = "desc"

    params = {"limit": limit, "offset": offset}

    where_parts = []

    if search:
        where_parts.append("""
            (
                Firmanavn LIKE :search OR
                Adresse LIKE :search OR
                CVR LIKE :search OR
                Serveringszone LIKE :search OR
                Lokation LIKE :search
            )
        """)
        params["search"] = f"%{search}%"

    if zone:
        where_parts.append("Serveringszone = :zone")
        params["zone"] = zone

    if lokation:
        where_parts.append("Lokation = :lokation")
        params["lokation"] = lokation

    if periodetype:
        where_parts.append("Periodetype = :periodetype")
        params["periodetype"] = periodetype

    # Active = today between GaeldendeFra and GaeldendeTilOgMed (month granularity).
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
        where_parts.append(active_expr)
    elif filter_mode == "inaktive":
        where_parts.append(f"NOT ({active_expr})")

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    query = f"""
        SELECT *
        FROM dbo.BrugAarhus_Udeservering
        {where_sql}
        ORDER BY {sort} {order}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """

    count_query = f"""
        SELECT COUNT(*) AS cnt
        FROM dbo.BrugAarhus_Udeservering
        {where_sql}
    """

    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        total = conn.execute(text(count_query), params).scalar()

    return jsonify({"total": total, "rows": [dict(r) for r in rows]})


@udeservering_bp.route("/api/applications/filters")
def api_applications_filters():
    """Distinct values used to populate filter dropdowns."""
    engine = get_engine()
    with engine.begin() as conn:
        zones = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT Serveringszone
            FROM dbo.BrugAarhus_Udeservering
            WHERE Serveringszone IS NOT NULL AND Serveringszone <> ''
            ORDER BY Serveringszone
        """)).fetchall()]

        lokationer = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT Lokation
            FROM dbo.BrugAarhus_Udeservering
            WHERE Lokation IS NOT NULL AND Lokation <> ''
            ORDER BY Lokation
        """)).fetchall()]

    return jsonify({"zones": zones, "lokationer": lokationer})


@udeservering_bp.route("/api/fakturering")
def api_fakturering():
    status = request.args.get("status", "Ny")
    limit = int(request.args.get("limit", 25))
    offset = int(request.args.get("offset", 0))
    search = request.args.get("search", "")
    year = request.args.get("year", "")
    month = request.args.get("month", "")
    zone = request.args.get("zone", "")
    lokation = request.args.get("lokation", "")

    sort = request.args.get("sort", "FakturaDatoSort")
    order = request.args.get("order", "desc")

    valid_sort_columns = {
        "FakturaDatoSort", "FakturaLinjeID", "DeskproID", "Firmanavn",
        "Adresse", "FakturaMaaned", "FakturaAar", "Lokation",
        "Serveringszone", "Serveringsareal", "Facadelaengde", "Pris",
        "FakturaStatus",
    }
    if sort not in valid_sort_columns:
        sort = "FakturaDatoSort"

    if order.lower() not in ("asc", "desc"):
        order = "desc"

    engine = get_engine()

    params = {"limit": limit, "offset": offset}
    where_parts = ["1=1"]

    if status:
        where_parts.append("FakturaStatus = :status")
        params["status"] = status

    if search:
        where_parts.append("""
            (
                  Firmanavn LIKE :search
               OR Adresse LIKE :search
               OR DeskproID LIKE :search
               OR CVR LIKE :search
            )
        """)
        params["search"] = f"%{search}%"

    if year:
        where_parts.append("FakturaAar = :year")
        params["year"] = int(year)

    if month:
        where_parts.append("FakturaMaaned = :month")
        params["month"] = month

    if zone:
        where_parts.append("Serveringszone = :zone")
        params["zone"] = zone

    if lokation:
        where_parts.append("Lokation = :lokation")
        params["lokation"] = lokation

    base_where = "WHERE " + " AND ".join(where_parts)

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

    # For status=Ny, Pris is NULL in DB (computed on-read), so SUM(Pris) would be 0.
    # We compute the summary by fetching all matching rows and pricing them in Python.
    # For other statuses, Pris is locked in DB and we can SUM directly.
    sum_query_locked = f"""
        SELECT
            COUNT(*) AS cnt,
            COUNT(DISTINCT DeskproID) AS firms,
            COALESCE(SUM(Pris), 0) AS sum_pris
        FROM BrugAarhus_Udeservering_Fakturalinjer
        {base_where}
    """

    all_rows_for_ny_query = f"""
        SELECT Serveringszone, Lokation, Serveringsareal, Facadelaengde,
               FakturaMaaned, FakturaAar, DeskproID
        FROM BrugAarhus_Udeservering_Fakturalinjer
        {base_where}
    """

    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        total = conn.execute(text(count_query), params).scalar()

        if status == "Ny":
            all_rows = conn.execute(text(all_rows_for_ny_query), params).mappings().all()
            sum_pris = 0.0
            firms = set()
            cnt = 0
            for r in all_rows:
                cnt += 1
                firms.add(r["DeskproID"])
                month_num = MONTH_NAME_TO_NUM.get(
                    (r["FakturaMaaned"] or "").split(" ")[0], 0
                )
                calc = beregn_pris(
                    r["Serveringszone"],
                    r["Lokation"],
                    float(r["Serveringsareal"] or 0),
                    float(r["Facadelaengde"] or 0),
                    month_num,
                    r["FakturaAar"],
                )
                if calc.get("ok"):
                    sum_pris += calc["belob"]
            summary = {"lines": cnt, "firms": len(firms), "sum_pris": sum_pris}
        else:
            s = conn.execute(text(sum_query_locked), params).mappings().first()
            summary = {
                "lines": s["cnt"],
                "firms": s["firms"],
                "sum_pris": float(s["sum_pris"] or 0),
            }

        final_rows = []
        for r in rows:
            r = dict(r)
            row_status = r.get("FakturaStatus", "")

            # Don't recalc once priced/approved
            if row_status in ("Faktureret", "TilFakturering"):
                final_rows.append(r)
                continue

            zone_val = r.get("Serveringszone")
            lokation_val = r.get("Lokation")
            areal = float(r.get("Serveringsareal") or 0)
            facade = float(r.get("Facadelaengde") or 0)

            month_name = (r["FakturaMaaned"] or "").split(" ")[0]
            month_num = MONTH_NAME_TO_NUM.get(month_name, 0)

            calc = beregn_pris(zone_val, lokation_val, areal, facade, month_num, r["FakturaAar"])
            r["Pris"] = calc["belob"] if calc["ok"] else None
            final_rows.append(r)

    return jsonify({
        "total": total,
        "rows": final_rows,
        "summary": summary,
    })


@udeservering_bp.route("/api/fakturering/year_options")
def api_fakturering_year_options():
    """Distinct year + month combinations for filter dropdowns."""
    engine = get_engine()
    with engine.begin() as conn:
        years = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT FakturaAar
            FROM BrugAarhus_Udeservering_Fakturalinjer
            ORDER BY FakturaAar DESC
        """)).fetchall()]

        zones = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT Serveringszone
            FROM BrugAarhus_Udeservering_Fakturalinjer
            WHERE Serveringszone IS NOT NULL AND Serveringszone <> ''
            ORDER BY Serveringszone
        """)).fetchall()]

        lokationer = [r[0] for r in conn.execute(text("""
            SELECT DISTINCT Lokation
            FROM BrugAarhus_Udeservering_Fakturalinjer
            WHERE Lokation IS NOT NULL AND Lokation <> ''
            ORDER BY Lokation
        """)).fetchall()]

    return jsonify({"years": years, "zones": zones, "lokationer": lokationer})


@udeservering_bp.route("/api/fakturering/reset", methods=["POST"])
def api_fakturering_reset():
    data = request.get_json() or {}
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
        placeholders = ", ".join(f":id{i}" for i in range(len(ids)))
        params = {f"id{i}": idv for i, idv in enumerate(ids)}

        rows = [
            dict(r) for r in conn.execute(text(f"""
                SELECT *
                FROM BrugAarhus_Udeservering_Fakturalinjer
                WHERE FakturaLinjeID IN ({placeholders})
            """), params).mappings().all()
        ]

        # Refuse to approve anything if any selected row has an invalid CVR —
        # SAP will reject the invoice anyway and the user should fix it in Deskpro first.
        invalid_cvr_rows = [r for r in rows if not is_valid_cvr(r.get("CVR"))]
        if invalid_cvr_rows:
            return jsonify({
                "success": False,
                "error": (
                    f"Kan ikke godkende: {len(invalid_cvr_rows)} linje(r) har ugyldigt CVR-nummer. "
                    f"Ret CVR i Deskpro og kør synkronisering, eller fjern linjerne fra markeringen."
                ),
                "invalid_ids": [r["FakturaLinjeID"] for r in invalid_cvr_rows],
            }), 400

        approved = 0
        for row in rows:
            row_status = row["FakturaStatus"]

            if row_status in ("Faktureret", "TilFakturering"):
                continue

            zone = row["Serveringszone"]
            lokation = row["Lokation"]
            areal = float(row["Serveringsareal"] or 0)
            facade = float(row["Facadelaengde"] or 0)

            month_name = (row["FakturaMaaned"] or "").split(" ")[0]
            month_num = MONTH_NAME_TO_NUM.get(month_name, 0)
            year = int(row["FakturaAar"])

            calc = beregn_pris(zone, lokation, areal, facade, month_num, year)
            pris = calc["belob"] if calc.get("ok") else None

            conn.execute(text("""
                UPDATE BrugAarhus_Udeservering_Fakturalinjer
                SET Pris = :pris,
                    FakturaStatus = 'TilFakturering'
                WHERE FakturaLinjeID = :id
            """), {"pris": pris, "id": row["FakturaLinjeID"]})
            approved += 1

        return jsonify({"success": True, "approved": approved})


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
    data = request.get_json() or {}

    fid = data.get("FakturaLinjeID")
    action = data.get("Action")

    if not fid:
        return jsonify({"success": False, "error": "Mangler FakturaLinjeID"})

    status_map = {
        "save": "Ny",
        "godkend": "TilFakturering",
        "ikke": "FakturerIkke"
    }
    new_status = status_map.get(action)
    if not new_status:
        return jsonify({"success": False, "error": "Ugyldig handling"})

    # Block godkend if CVR isn't valid — SAP would reject the invoice anyway.
    if action == "godkend":
        engine = get_engine()
        with engine.begin() as conn:
            cvr_row = conn.execute(text("""
                SELECT CVR FROM BrugAarhus_Udeservering_Fakturalinjer WHERE FakturaLinjeID = :id
            """), {"id": fid}).first()
        if not cvr_row or not is_valid_cvr(cvr_row.CVR):
            return jsonify({
                "success": False,
                "error": "Ugyldigt CVR-nummer. Ret CVR i Deskpro og kør synkronisering før godkendelse.",
            }), 400

    # Editable fields - empty strings on numeric fields become NULL.
    editable_fields = {
        "Serveringsareal": _to_decimal_or_none(data.get("Serveringsareal")),
        "Facadelaengde": _to_decimal_or_none(data.get("Facadelaengde")),
        "Kommentar": data.get("Kommentar"),
        "Lokation": data.get("Lokation"),
    }

    # Only include keys explicitly provided
    editable_fields = {
        k: v for k, v in editable_fields.items() if k in data
    }

    pris = _to_decimal_or_none(data.get("Pris"))

    engine = get_engine()

    set_parts = [f"{k} = :{k}" for k in editable_fields.keys()]

    params = dict(editable_fields)
    params["id"] = fid
    params["status"] = new_status

    if action == "godkend":
        set_parts.append("Pris = :Pris")
        params["Pris"] = pris

    set_parts.append("FakturaStatus = :status")
    set_clause = ", ".join(set_parts)

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
                Periodetype,
                GaeldendeFra,
                GaeldendeTilOgMed
            FROM dbo.BrugAarhus_Udeservering
            ORDER BY Id DESC
        """)).mappings().all()

    return jsonify({"total": len(rows), "rows": [dict(r) for r in rows]})


def beregn_pris(zone, lokation, serveringsareal, facadelaengde, month, year):
    data = load_prisdata_for_year(year)

    params = data["params"]
    takster = data["takster"]
    saeson_map = data["saeson"]

    saeson = saeson_map.get(month)
    sommer = (saeson == "Sommer")

    t = takster.get((zone or "").upper())
    if not t:
        return {"ok": False, "error": f"Zone-takst ikke fundet for zone '{zone}'."}

    pris_pr_m2 = float(t["SommerPrisPrM2"] if sommer else t["VinterPrisPrM2"])

    facadebredde = float(params.get("Facadebredde i meter", 0.8))
    min_areal = float(params.get("Minimums opkrævningsareal", 1.0))
    min_belob = float(params.get("Minimums opkrævningsbeløb", 250.0))

    brutto = max(float(serveringsareal or 0), min_areal)

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
            FROM BrugAarhus_Udeservering_Fakturalinjer
            GROUP BY COALESCE(FakturaStatus, 'Ny')
        """)).mappings().all()

        totals = conn.execute(text("""
            SELECT COUNT(*) AS Rows, COUNT(DISTINCT Firmanavn) AS Firms
            FROM BrugAarhus_Udeservering
        """)).mappings().first()

        sum_pris = conn.execute(text("""
            SELECT
                COALESCE(SUM(CASE WHEN FakturaStatus = 'Ny' THEN Pris END), 0) AS sum_ny,
                COALESCE(SUM(CASE WHEN FakturaStatus = 'TilFakturering' THEN Pris END), 0) AS sum_tilfakt,
                COALESCE(SUM(CASE WHEN FakturaStatus = 'Faktureret' THEN Pris END), 0) AS sum_faktureret
            FROM BrugAarhus_Udeservering_Fakturalinjer
        """)).mappings().first()

        per_zone = conn.execute(text("""
            SELECT Serveringszone AS Zone,
                   COUNT(*) AS Cnt,
                   COALESCE(SUM(Pris), 0) AS SumPris
            FROM BrugAarhus_Udeservering_Fakturalinjer
            WHERE Serveringszone IS NOT NULL AND Serveringszone <> ''
            GROUP BY Serveringszone
            ORDER BY Serveringszone
        """)).mappings().all()

        per_year = conn.execute(text("""
            SELECT FakturaAar AS Year,
                   COUNT(*) AS Cnt,
                   COALESCE(SUM(Pris), 0) AS SumPris
            FROM BrugAarhus_Udeservering_Fakturalinjer
            GROUP BY FakturaAar
            ORDER BY FakturaAar DESC
        """)).mappings().all()

    status_counts = {
        "Ny": 0, "TilFakturering": 0, "Faktureret": 0, "FakturerIkke": 0
    }
    for row in stats:
        status_counts[row["Status"]] = row["Cnt"]

    return jsonify({
        "status": status_counts,
        "totals": {"rows": totals["Rows"], "firms": totals["Firms"]},
        "sums": {
            "ny": float(sum_pris["sum_ny"] or 0),
            "tilfakturering": float(sum_pris["sum_tilfakt"] or 0),
            "faktureret": float(sum_pris["sum_faktureret"] or 0),
        },
        "per_zone": [dict(r) | {"SumPris": float(r["SumPris"] or 0)} for r in per_zone],
        "per_year": [dict(r) | {"SumPris": float(r["SumPris"] or 0)} for r in per_year],
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
        last_year = conn.execute(text("""
            SELECT MAX([Year]) FROM BrugAarhus_Udeservering_Parametre
        """)).scalar()

        new_year = last_year + 1

        conn.execute(text("""
            INSERT INTO BrugAarhus_Udeservering_Parametre (Noegle, VaerdiDecimal, VaerdiTekst, [Year])
            SELECT Noegle, VaerdiDecimal, VaerdiTekst, :new_year
            FROM BrugAarhus_Udeservering_Parametre
            WHERE [Year] = :last_year
        """), {"new_year": new_year, "last_year": last_year})

        conn.execute(text("""
            INSERT INTO BrugAarhus_Udeservering_Takster
                (ZoneKode, ZoneBeskrivelse, PSPElment, MaterialeNr, SommerPrisPrM2, VinterPrisPrM2, [Year])
            SELECT ZoneKode, ZoneBeskrivelse, PSPElment, MaterialeNr,
                   SommerPrisPrM2, VinterPrisPrM2, :new_year
            FROM BrugAarhus_Udeservering_Takster
            WHERE [Year] = :last_year
        """), {"new_year": new_year, "last_year": last_year})

        conn.execute(text("""
            INSERT INTO BrugAarhus_Udeservering_Saeson
                (MaanedNr, Maanedsnavn, Saeson, [Year])
            SELECT MaanedNr, Maanedsnavn, Saeson, :new_year
            FROM BrugAarhus_Udeservering_Saeson
            WHERE [Year] = :last_year
        """), {"new_year": new_year, "last_year": last_year})

    return jsonify({"success": True, "new_year": new_year})
