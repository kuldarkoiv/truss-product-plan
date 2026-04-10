-- ============================================================
-- Tootmise fact tabel — nädalate kaupa, budget aruande jaoks
-- record_type = 'actual', product = 'Ferm'
-- ============================================================

SELECT
    -- Aasta eraldi veerus
    year::int                                           AS aasta,

    -- Nädala label eesti formaadis: "N2", "N14"
    'N' || production_week::int                         AS week_label,

    'actual'                                            AS record_type,
    'Ferm'                                              AS product,

    -- Trader (owner sales_input tabelist, normaliseeritud)
    CASE
        WHEN si.owner = 'Sigrid' THEN 'Sigrid Piirioja'
        ELSE si.owner
    END                                                 AS trader,

    -- Riik owner nime järgi
    CASE
        WHEN si.owner IN ('Roberts Slaukstins', 'Reinis', 'Kristiana Borarosova')
        THEN 'LV'
        ELSE 'EE'
    END                                                 AS country,

    -- Rahaline väärtus
    CASE
        WHEN pp.sales_price ~ '^[0-9]+(\.[0-9]+)?$'
        THEN pp.sales_price::numeric
        ELSE NULL
    END                                                 AS value,

    -- Tunnid lisainfona
    pp.booked_h                                         AS plan_hours,
    pp.tegelik_tootmisaeg                               AS actual_hours,

    -- Staatus
    (pp.staatus->>0)                                    AS status,

    -- Tehingu viide
    pp.name                                             AS project_ref,
    pp.customer_name                                    AS customer,
    pp.pipedrive_id                                     AS pipedrive_id

FROM truss_airtable_production_plan pp
LEFT JOIN truss_airtable_sales_input si
    ON si.airtable_id = ANY(
        SELECT jsonb_array_elements_text(pp.project_name_ids)
    )

WHERE pp.year = '2026'
  AND pp.production_week ~ '^[0-9]+$'
  AND pp.production_week IS NOT NULL
  AND pp.name NOT ILIKE 'Summary%'

ORDER BY
    week_label,
    country,
    trader,
    pp.name;
