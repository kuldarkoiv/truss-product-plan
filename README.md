# truss-product-plan

Airtable → PostgreSQL sünkronisatsioon Truss tootmisplaneerimise andmete jaoks. Andmed on mõeldud Metabase'i dashboardide ja SQL-päringute tarbeks.

---

## Ülevaade

Skript tõmbab 6 Airtable'i tabelit REST API kaudu ja kirjutab need PostgreSQL-i (DigitalOcean managed). Iga sync on täielik upsert — olemasolevad read uuendatakse, uued lisatakse, kustutamisi ei toimu.

```
Airtable API
    └── sync_airtable.py
            ├── loob tabelid (CREATE TABLE IF NOT EXISTS)
            ├── annab SELECT õigused doadmin kasutajale
            └── upsert kõik kirjed → PostgreSQL
                    └── Metabase (lugemine doadmin kaudu)
```

---

## Sisendid

### Env muutujad (`.env` fail)

| Muutuja | Kirjeldus |
|---|---|
| `AIRTABLE_API_KEY` | Airtable Personal Access Token |
| `AIRTABLE_BASE_ID` | Airtable base'i ID (algab `app...`) |
| `POSTGRES_HOST` | PostgreSQL host |
| `POSTGRES_PORT` | PostgreSQL port (vaikimisi 5432) |
| `POSTGRES_USER` | PostgreSQL kasutaja |
| `POSTGRES_PASSWORD` | PostgreSQL parool |
| `POSTGRES_DB` | Andmebaasi nimi |

### Airtable tabelid (sisend)

| Airtable tabel ID | Kirjeldus |
|---|---|
| `tblFfkTopBD2qI9do` | Sales Input — müügitellimused |
| `tbl4oCpQjROHh2G0S` | Production Plan — tootmisplaan nädalate kaupa |
| `tbl1kCIthy7hnbf6z` | Resources — nädalate mahtude kapatsiteet |
| `tblJWSG0Jo5oH6sk8` | Valmistoodang — valmis toodangu logi |
| `tblz7a2tA4ZC8Wa6V` | Logi väljavõte — tootmisliini ajakulud |
| `tblgAV0d4WcIbXtQh` | A-rühm — prioriteetsed ülesanded |

---

## Väljundid

### PostgreSQL tabelid

| Tabel | Põhiväliad |
|---|---|
| `truss_airtable_sales_input` | `airtable_id`, `name`, `customer_name`, `owner`, `status`, `sales_price`, `date`, `pipedrive_id` |
| `truss_airtable_production_plan` | `airtable_id`, `name`, `production_week`, `year`, `staatus`, `booked_h`, `tegelik_tootmisaeg`, `sales_price`, `customer_name`, `project_name_ids` |
| `truss_airtable_resources` | `airtable_id`, `week_no`, `year`, `available_hours`, `confirmed` |
| `truss_airtable_valmistoodang` | `airtable_id`, `uni_nr`, `tk`, `solmi_kokku`, `plan_aeg`, `teg_aeg`, `klient`, `staatus` |
| `truss_airtable_logi_valjavote` | `airtable_id`, `sisesta_kuupaev`, `liin`, `algus`, `lopp`, `kulu`, `projekt` |
| `truss_airtable_a_ryhm` | `airtable_id`, `tahtaeg`, `staatus`, `asukoht_masin`, `uleanne_tapsemalt` |

Kõigil tabelitel on `synced_at TIMESTAMPTZ` — viimase upsert'i aeg.

Kõigile tabelitele antakse automaatselt `GRANT SELECT ON ... TO doadmin` (Metabase'i lugemisõigus).

---

## Loogika

### sync_airtable.py

1. Loeb env muutujaid (`.env`) — Airtable token + base ID + PG ühendusandmed
2. Loob PG tabelid `CREATE TABLE IF NOT EXISTS` DDL-ga (idempotentne)
3. Annab igale tabelile `GRANT SELECT TO doadmin`
4. Iga Airtable tabeli kohta:
   - Tõmbab kõik read paginatsiooniga (`offset`-põhine, 0.2s paus rate limit jaoks)
   - Mapib Airtable'i `fields` objekt PG tuple'iks (mapper funktsioon per tabel)
   - Teeb `INSERT ... ON CONFLICT (airtable_id) DO UPDATE` — upsert
5. JSONB väliad (lookup-linked kirjed, staatus-listid) serialiseeritakse `json.dumps`-ga
6. Lookup väliad, mis tulevad listina, võetakse esimene element (`first()`)
7. Numbrilised väärtused konverteeritakse `to_numeric()` — `None` kui ei ole arv

### fact_production_weekly.sql

Metabase'i / analüüsi päring, mis koondab tootmisplaani andmed nädalate kaupa:

- **Allikas:** `truss_airtable_production_plan` LEFT JOIN `truss_airtable_sales_input`
- **Filter:** aasta 2026, `production_week` on number, `name NOT ILIKE 'Summary%'`
- **Väljundid:** `aasta`, `week_label` (nt "N14"), `record_type` = "actual", `product` = "Ferm", `trader`, `country`, `value` (müügihind), `plan_hours`, `actual_hours`, `status`, `project_ref`, `customer`, `pipedrive_id`
- **Trader normaliseerimine:** "Sigrid" → "Sigrid Piirioja"
- **Riik:** LV kui owner on Roberts Slaukstins / Reinis / Kristiana Borarosova, muidu EE

---

## Käivitamine

```bash
pip install -r requirements.txt
cp .env.example .env
# täida .env väärtused

python sync_airtable.py
```

### Cron (iga öösel kell 3)

```
0 3 * * * cd /path/to/truss-product-plan && python sync_airtable.py >> /var/log/truss_sync.log 2>&1
```

---

## Sõltuvused

```
psycopg2-binary
python-dotenv
```

Stdlib: `os`, `json`, `time`, `urllib.request`, `urllib.parse`
