import os
import json
import time
import urllib.request
import urllib.parse
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ.get("AIRTABLE_API_KEY")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID")

PG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", 5432)),
    "user": os.environ.get("PG_USER", "kuldar"),
    "password": os.environ.get("PG_PASSWORD", ""),
    "dbname": os.environ.get("PG_DATABASE", "defaultdb"),
    "sslmode": "require"
}

AIRTABLE_HEADERS = {"Authorization": f"Bearer {TOKEN}"}

TABLES = {
    "tblFfkTopBD2qI9do": "truss_airtable_sales_input",
    "tbl4oCpQjROHh2G0S": "truss_airtable_production_plan",
    "tbl1kCIthy7hnbf6z": "truss_airtable_resources",
    "tblJWSG0Jo5oH6sk8": "truss_airtable_valmistoodang",
    "tblz7a2tA4ZC8Wa6V": "truss_airtable_logi_valjavote",
    "tblgAV0d4WcIbXtQh": "truss_airtable_a_ryhm",
}

DDL = {
    "truss_airtable_sales_input": """
        CREATE TABLE IF NOT EXISTS truss_airtable_sales_input (
            airtable_id         TEXT PRIMARY KEY,
            name                TEXT,
            customer_name       TEXT,
            production_plan_ids JSONB,
            estimated_duration  NUMERIC,
            notes               TEXT,
            owner               TEXT,
            status              TEXT,
            requested_delivery  TEXT,
            date                DATE,
            budgeted_price      NUMERIC,
            sales_price         NUMERIC,
            pipedrive_id        BIGINT,
            pipedrive_url       TEXT,
            drive_url           TEXT,
            tegelik_m3          TEXT,
            moved_to_production TEXT,
            pipedrive_last_mod  TIMESTAMPTZ,
            synced_at           TIMESTAMPTZ DEFAULT NOW()
        );
    """,
    "truss_airtable_production_plan": """
        CREATE TABLE IF NOT EXISTS truss_airtable_production_plan (
            airtable_id             TEXT PRIMARY KEY,
            name                    TEXT,
            production_week         TEXT,
            staatus                 JSONB,
            tootmise_staatus        JSONB,
            pct_tehtud              NUMERIC,
            customer_name           TEXT,
            requested_delivery      TEXT,
            booked_h                NUMERIC,
            tegelik_tootmisaeg      NUMERIC,
            plan_aeg_teg_aeg        TEXT,
            solmed                  NUMERIC,
            plan_toot               NUMERIC,
            teg_toot                NUMERIC,
            available_h             NUMERIC,
            remaining_h             NUMERIC,
            estimated_prod_date     DATE,
            tootmisele_info         TEXT,
            notes                   TEXT,
            project_name_ids        JSONB,
            assignee                TEXT,
            to_production_h         NUMERIC,
            actual_h                NUMERIC,
            year                    TEXT,
            uni_nr                  TEXT,
            unikaalne_number        TEXT,
            ladu_valja              TEXT,
            moved_to_production     TEXT,
            pipedrive_id            TEXT,
            last_modified           TIMESTAMPTZ,
            sales_price             TEXT,
            status_last_modified    TIMESTAMPTZ,
            pipedrive_last_mod      TEXT,
            pamir_copy              TEXT,
            drive_url               TEXT,
            markmed_myygilt         TEXT,
            valmistoodang_ids       JSONB,
            created_at              TIMESTAMPTZ,
            synced_at               TIMESTAMPTZ DEFAULT NOW()
        );
    """,
    "truss_airtable_resources": """
        CREATE TABLE IF NOT EXISTS truss_airtable_resources (
            airtable_id     TEXT PRIMARY KEY,
            week_no         NUMERIC,
            year            NUMERIC,
            available_hours NUMERIC,
            calculation     TEXT,
            confirmed       BOOLEAN,
            synced_at       TIMESTAMPTZ DEFAULT NOW()
        );
    """,
    "truss_airtable_valmistoodang": """
        CREATE TABLE IF NOT EXISTS truss_airtable_valmistoodang (
            airtable_id         TEXT PRIMARY KEY,
            uni_nr              NUMERIC,
            tk                  NUMERIC,
            solmi_kokku         NUMERIC,
            plan_aeg            NUMERIC,
            teg_aeg             NUMERIC,
            plan_puit           NUMERIC,
            teg_puit_m3         NUMERIC,
            teg_puit_eur        NUMERIC,
            oga_m2              NUMERIC,
            oga_eur             NUMERIC,
            valmis_kuu          TEXT,
            klient              TEXT,
            arve                TEXT,
            projekt             TEXT,
            fermi_tahis         TEXT,
            solmi_1tk           NUMERIC,
            valmis_paev         TEXT,
            lisainfo            TEXT,
            link_tootmisele     TEXT,
            saada_sheetsi       BOOLEAN,
            tellimused          TEXT,
            tellimus_link_ids   JSONB,
            staatus             TEXT,
            pct_tehtud          NUMERIC,
            logi                TEXT,
            synced_at           TIMESTAMPTZ DEFAULT NOW()
        );
    """,
    "truss_airtable_logi_valjavote": """
        CREATE TABLE IF NOT EXISTS truss_airtable_logi_valjavote (
            airtable_id TEXT PRIMARY KEY,
            sisesta_kuupaev TEXT,
            liin            TEXT,
            algus           TEXT,
            lopp            TEXT,
            kulu            TEXT,
            kommentaar      TEXT,
            projekt         TEXT,
            synced_at       TIMESTAMPTZ DEFAULT NOW()
        );
    """,
    "truss_airtable_a_ryhm": """
        CREATE TABLE IF NOT EXISTS truss_airtable_a_ryhm (
            airtable_id         TEXT PRIMARY KEY,
            tahtaeg             DATE,
            staatus             JSONB,
            asukoht_masin       JSONB,
            uleanne_tapsemalt   TEXT,
            synced_at           TIMESTAMPTZ DEFAULT NOW()
        );
    """,
}

def airtable_fetch_all(table_id):
    """Tõmbab kõik read paginatsiooniga."""
    records = []
    offset = None
    while True:
        url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
        if offset:
            url += f"?offset={urllib.parse.quote(offset)}"
        req = urllib.request.Request(url, headers=AIRTABLE_HEADERS)
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.2)  # rate limit
    return records

def safe(fields, key, default=None):
    return fields.get(key, default)

def jsonb(val):
    if val is None:
        return None
    return json.dumps(val)

def first(val):
    """Lookup väljad tulevad listina, võtame esimese."""
    if isinstance(val, list):
        return val[0] if val else None
    return val

def to_numeric(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def map_sales_input(r):
    f = r["fields"]
    return (
        r["id"],
        safe(f, "Name"),
        safe(f, "Customer Name"),
        jsonb(safe(f, "Production plan")),
        to_numeric(safe(f, "Estimated Duration")),
        safe(f, "Notes"),
        safe(f, "Owner"),
        safe(f, "Status"),
        safe(f, "Requested Delivery Time"),
        safe(f, "Date"),
        to_numeric(safe(f, "Budgeted Price")),
        to_numeric(safe(f, "Sales Price")),
        safe(f, "Pipedrive ID"),
        safe(f, "pipedrive url"),
        safe(f, "drive url"),
        safe(f, "tegelik m3"),
        safe(f, "Moved to Production"),
        safe(f, "Pipedrive Last Modified"),
    )

def map_production_plan(r):
    f = r["fields"]
    return (
        r["id"],
        str(safe(f, "Name", "")),
        str(safe(f, "Production Week", "")),
        jsonb(safe(f, "STAATUS")),
        jsonb(safe(f, "Tootmise staatus")),
        to_numeric(safe(f, "% tehtud")),
        str(first(safe(f, "Customer Name (from Project Name)")) or ""),
        str(first(safe(f, "Requested Delivery Time (from Project Name)")) or ""),
        to_numeric(first(safe(f, "Booked (h)"))),
        to_numeric(safe(f, "Tegelik tootmisaeg")),
        str(safe(f, "Plan aeg-teg aeg", "")),
        to_numeric(safe(f, "Sõlmed")),
        to_numeric(safe(f, "Plan. TOOT")),
        to_numeric(safe(f, "Teg. TOOT")),
        to_numeric(safe(f, "Available (h)")),
        to_numeric(safe(f, "Remaining (h)")),
        safe(f, "Estimated Production Date"),
        safe(f, "Tootmisele info"),
        str(first(safe(f, "Notes (from Project Name) 2")) or ""),
        jsonb(safe(f, "Project Name")),
        str(first(safe(f, "Assignee (from Table 6)")) or ""),
        to_numeric(safe(f, "To production (h)")),
        to_numeric(safe(f, "Actual (h)")),
        str(safe(f, "Year", "")),
        str(first(safe(f, "Uni NR.")) or ""),
        str(first(safe(f, "Unikaalne number")) or ""),
        safe(f, "Ladu välja"),
        str(first(safe(f, "Moved to Production (from Project Name)")) or ""),
        str(first(safe(f, "pipedrive_id")) or ""),
        safe(f, "Last Modified"),
        str(first(safe(f, "Sales Price (from Project Name)")) or ""),
        safe(f, "Status Last Modified"),
        str(first(safe(f, "Pipedrive Last Modified (from Project Name)")) or ""),
        safe(f, "Pamir copy"),
        str(first(safe(f, "drive url (from Project Name)")) or ""),
        str(first(safe(f, "Märkmed müügilt")) or ""),
        jsonb(safe(f, "Valmistoodang 5")),
        safe(f, "Created"),
    )

def map_resources(r):
    f = r["fields"]
    return (
        r["id"],
        to_numeric(safe(f, "Week No")),
        to_numeric(safe(f, "Year")),
        to_numeric(safe(f, "Available Hours")),
        str(safe(f, "Calculation", "")),
        bool(safe(f, "Confirmed?", False)),
    )

def map_valmistoodang(r):
    f = r["fields"]
    return (
        r["id"],
        to_numeric(safe(f, "Uni NR.")),
        to_numeric(safe(f, "TK.")),
        to_numeric(safe(f, "Sõlmi kokku")),
        to_numeric(safe(f, "Plan aeg")),
        to_numeric(safe(f, "Teg. aeg")),
        to_numeric(safe(f, "Plan puit")),
        to_numeric(safe(f, "Teg. puit m3")),
        to_numeric(safe(f, "Teg. puit €")),
        to_numeric(safe(f, "Oga m2")),
        to_numeric(safe(f, "Oga €")),
        safe(f, "Valmis kuu"),
        safe(f, "Klient"),
        safe(f, "Arve"),
        safe(f, "Projekt"),
        safe(f, "Fermi tähis"),
        to_numeric(safe(f, "Sõlmi 1tk. kohta")),
        safe(f, "Valmis Päev"),
        safe(f, "Lisainfo tootmisele"),
        safe(f, "Link tootmisele"),
        bool(safe(f, "Saada Sheetsi", False)),
        safe(f, "Tellimused"),
        jsonb(safe(f, "Tellimus_Link")),
        safe(f, "Staatus"),
        to_numeric(safe(f, "% tehtud")),
        safe(f, "Logi"),
    )

def map_logi_valjavote(r):
    f = r["fields"]
    return (
        r["id"],
        safe(f, "Sisesta kuupäev esimesel reale"),
        safe(f, "Liin"),
        safe(f, "Algus"),
        safe(f, "Lõpp"),
        safe(f, "Kulu"),
        safe(f, "Kommentaar"),
        safe(f, "Projekt"),
    )

def map_a_ryhm(r):
    f = r["fields"]
    return (
        r["id"],
        safe(f, "Tähtaeg"),
        jsonb(safe(f, "Staatus")),
        jsonb(safe(f, "Asukoht/Masin")),
        safe(f, "Ülesanne täpsemalt"),
    )

UPSERT_SQL = {
    "truss_airtable_sales_input": """
        INSERT INTO truss_airtable_sales_input
            (airtable_id, name, customer_name, production_plan_ids,
             estimated_duration, notes, owner, status, requested_delivery,
             date, budgeted_price, sales_price, pipedrive_id,
             pipedrive_url, drive_url, tegelik_m3, moved_to_production,
             pipedrive_last_mod)
        VALUES %s
        ON CONFLICT (airtable_id) DO UPDATE SET
            name = EXCLUDED.name,
            customer_name = EXCLUDED.customer_name,
            production_plan_ids = EXCLUDED.production_plan_ids,
            estimated_duration = EXCLUDED.estimated_duration,
            notes = EXCLUDED.notes,
            owner = EXCLUDED.owner,
            status = EXCLUDED.status,
            requested_delivery = EXCLUDED.requested_delivery,
            date = EXCLUDED.date,
            budgeted_price = EXCLUDED.budgeted_price,
            sales_price = EXCLUDED.sales_price,
            pipedrive_id = EXCLUDED.pipedrive_id,
            pipedrive_url = EXCLUDED.pipedrive_url,
            drive_url = EXCLUDED.drive_url,
            tegelik_m3 = EXCLUDED.tegelik_m3,
            moved_to_production = EXCLUDED.moved_to_production,
            pipedrive_last_mod = EXCLUDED.pipedrive_last_mod,
            synced_at = NOW()
    """,
    "truss_airtable_production_plan": """
        INSERT INTO truss_airtable_production_plan
            (airtable_id, name, production_week, staatus, tootmise_staatus,
             pct_tehtud, customer_name, requested_delivery, booked_h,
             tegelik_tootmisaeg, plan_aeg_teg_aeg, solmed, plan_toot, teg_toot,
             available_h, remaining_h, estimated_prod_date, tootmisele_info,
             notes, project_name_ids, assignee, to_production_h, actual_h,
             year, uni_nr, unikaalne_number, ladu_valja, moved_to_production,
             pipedrive_id, last_modified, sales_price, status_last_modified,
             pipedrive_last_mod, pamir_copy, drive_url, markmed_myygilt,
             valmistoodang_ids, created_at)
        VALUES %s
        ON CONFLICT (airtable_id) DO UPDATE SET
            name = EXCLUDED.name,
            production_week = EXCLUDED.production_week,
            staatus = EXCLUDED.staatus,
            tootmise_staatus = EXCLUDED.tootmise_staatus,
            pct_tehtud = EXCLUDED.pct_tehtud,
            customer_name = EXCLUDED.customer_name,
            requested_delivery = EXCLUDED.requested_delivery,
            booked_h = EXCLUDED.booked_h,
            tegelik_tootmisaeg = EXCLUDED.tegelik_tootmisaeg,
            plan_aeg_teg_aeg = EXCLUDED.plan_aeg_teg_aeg,
            solmed = EXCLUDED.solmed,
            plan_toot = EXCLUDED.plan_toot,
            teg_toot = EXCLUDED.teg_toot,
            available_h = EXCLUDED.available_h,
            remaining_h = EXCLUDED.remaining_h,
            estimated_prod_date = EXCLUDED.estimated_prod_date,
            tootmisele_info = EXCLUDED.tootmisele_info,
            notes = EXCLUDED.notes,
            project_name_ids = EXCLUDED.project_name_ids,
            assignee = EXCLUDED.assignee,
            to_production_h = EXCLUDED.to_production_h,
            actual_h = EXCLUDED.actual_h,
            year = EXCLUDED.year,
            uni_nr = EXCLUDED.uni_nr,
            unikaalne_number = EXCLUDED.unikaalne_number,
            ladu_valja = EXCLUDED.ladu_valja,
            moved_to_production = EXCLUDED.moved_to_production,
            pipedrive_id = EXCLUDED.pipedrive_id,
            last_modified = EXCLUDED.last_modified,
            sales_price = EXCLUDED.sales_price,
            status_last_modified = EXCLUDED.status_last_modified,
            pipedrive_last_mod = EXCLUDED.pipedrive_last_mod,
            pamir_copy = EXCLUDED.pamir_copy,
            drive_url = EXCLUDED.drive_url,
            markmed_myygilt = EXCLUDED.markmed_myygilt,
            valmistoodang_ids = EXCLUDED.valmistoodang_ids,
            created_at = EXCLUDED.created_at,
            synced_at = NOW()
    """,
    "truss_airtable_resources": """
        INSERT INTO truss_airtable_resources
            (airtable_id, week_no, year, available_hours, calculation, confirmed)
        VALUES %s
        ON CONFLICT (airtable_id) DO UPDATE SET
            week_no = EXCLUDED.week_no,
            year = EXCLUDED.year,
            available_hours = EXCLUDED.available_hours,
            calculation = EXCLUDED.calculation,
            confirmed = EXCLUDED.confirmed,
            synced_at = NOW()
    """,
    "truss_airtable_valmistoodang": """
        INSERT INTO truss_airtable_valmistoodang
            (airtable_id, uni_nr, tk, solmi_kokku, plan_aeg, teg_aeg, plan_puit,
             teg_puit_m3, teg_puit_eur, oga_m2, oga_eur, valmis_kuu, klient,
             arve, projekt, fermi_tahis, solmi_1tk, valmis_paev, lisainfo,
             link_tootmisele, saada_sheetsi, tellimused, tellimus_link_ids,
             staatus, pct_tehtud, logi)
        VALUES %s
        ON CONFLICT (airtable_id) DO UPDATE SET
            uni_nr = EXCLUDED.uni_nr,
            tk = EXCLUDED.tk,
            solmi_kokku = EXCLUDED.solmi_kokku,
            plan_aeg = EXCLUDED.plan_aeg,
            teg_aeg = EXCLUDED.teg_aeg,
            plan_puit = EXCLUDED.plan_puit,
            teg_puit_m3 = EXCLUDED.teg_puit_m3,
            teg_puit_eur = EXCLUDED.teg_puit_eur,
            oga_m2 = EXCLUDED.oga_m2,
            oga_eur = EXCLUDED.oga_eur,
            valmis_kuu = EXCLUDED.valmis_kuu,
            klient = EXCLUDED.klient,
            arve = EXCLUDED.arve,
            projekt = EXCLUDED.projekt,
            fermi_tahis = EXCLUDED.fermi_tahis,
            solmi_1tk = EXCLUDED.solmi_1tk,
            valmis_paev = EXCLUDED.valmis_paev,
            lisainfo = EXCLUDED.lisainfo,
            link_tootmisele = EXCLUDED.link_tootmisele,
            saada_sheetsi = EXCLUDED.saada_sheetsi,
            tellimused = EXCLUDED.tellimused,
            tellimus_link_ids = EXCLUDED.tellimus_link_ids,
            staatus = EXCLUDED.staatus,
            pct_tehtud = EXCLUDED.pct_tehtud,
            logi = EXCLUDED.logi,
            synced_at = NOW()
    """,
    "truss_airtable_logi_valjavote": """
        INSERT INTO truss_airtable_logi_valjavote
            (airtable_id, sisesta_kuupaev, liin, algus, lopp, kulu, kommentaar, projekt)
        VALUES %s
        ON CONFLICT (airtable_id) DO UPDATE SET
            sisesta_kuupaev = EXCLUDED.sisesta_kuupaev,
            liin = EXCLUDED.liin,
            algus = EXCLUDED.algus,
            lopp = EXCLUDED.lopp,
            kulu = EXCLUDED.kulu,
            kommentaar = EXCLUDED.kommentaar,
            projekt = EXCLUDED.projekt,
            synced_at = NOW()
    """,
    "truss_airtable_a_ryhm": """
        INSERT INTO truss_airtable_a_ryhm
            (airtable_id, tahtaeg, staatus, asukoht_masin, uleanne_tapsemalt)
        VALUES %s
        ON CONFLICT (airtable_id) DO UPDATE SET
            tahtaeg = EXCLUDED.tahtaeg,
            staatus = EXCLUDED.staatus,
            asukoht_masin = EXCLUDED.asukoht_masin,
            uleanne_tapsemalt = EXCLUDED.uleanne_tapsemalt,
            synced_at = NOW()
    """,
}

MAPPERS = {
    "truss_airtable_sales_input": map_sales_input,
    "truss_airtable_production_plan": map_production_plan,
    "truss_airtable_resources": map_resources,
    "truss_airtable_valmistoodang": map_valmistoodang,
    "truss_airtable_logi_valjavote": map_logi_valjavote,
    "truss_airtable_a_ryhm": map_a_ryhm,
}

def sync():
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    # Loo tabelid
    print("Loon tabelid...")
    for pg_table, ddl in DDL.items():
        cur.execute(ddl)
        cur.execute(f"GRANT SELECT ON {pg_table} TO doadmin;")
    conn.commit()
    print("Tabelid loodud.\n")

    # Sync iga tabel
    for airtable_id, pg_table in TABLES.items():
        print(f"Tõmban: {pg_table} ...")
        records = airtable_fetch_all(airtable_id)
        print(f"  Kirjeid: {len(records)}")

        if not records:
            print("  Tühi, vahelan.\n")
            continue

        mapper = MAPPERS[pg_table]
        rows = []
        for r in records:
            try:
                rows.append(mapper(r))
            except Exception as e:
                print(f"  HOIATUS: kirje {r['id']} viga: {e}")

        upsert_sql = UPSERT_SQL[pg_table]
        # Lisa synced_at iga reale (viimane element)
        rows_with_ts = [row + ("NOW()",) if "synced_at" not in upsert_sql.split("VALUES")[0] else row for row in rows]

        execute_values(cur, upsert_sql, rows)
        conn.commit()
        print(f"  Salvestatud: {len(rows)} kirjet\n")

    cur.close()
    conn.close()
    print("Sync valmis!")

if __name__ == "__main__":
    sync()
