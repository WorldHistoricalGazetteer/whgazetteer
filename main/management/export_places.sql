COPY (
    SELECT p.id, p.title, 
           COALESCE(NULLIF(p.attestation_year::text, ''), NULL) AS attestation_year,
           p.ccodes, p.dataset, p.fclasses, p.flag, p.idx_pub, p.indexed, 
           CASE WHEN p.minmax IS NULL THEN NULL ELSE p.minmax END AS minmax,
           COALESCE(NULLIF(p.review_tgn::text, ''), NULL) AS review_tgn,
           COALESCE(NULLIF(p.review_wd::text, ''), NULL) AS review_wd,
           COALESCE(NULLIF(p.review_whg::text, ''), NULL) AS review_whg,
           p.src_id, p.timespans, d.create_date, false AS idx_builder
    FROM places p
    JOIN datasets d ON p.dataset = d.label
    WHERE dataset IN (SELECT label FROM datasets)
    ORDER BY id
) TO '/home/whgadmin/sites/data_dumps/exported_places_20240525.csv' WITH CSV HEADER NULL '\N';

