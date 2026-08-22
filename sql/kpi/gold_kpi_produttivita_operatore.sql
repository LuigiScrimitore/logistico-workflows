-- =============================================================================
-- KPI: PRODUTTIVITA' OPERATORE (PREPARATORI)  (v3.0  2026-06-08)
-- Sorgente: gold_prod.logistica.F_PREP_SPED (misure su CARTONI/QUINTALI, OP-27).
-- Lookup: LU_OPERATORE, LU_SITO (gold_prod.logistica).
-- Produttivita' = TOT_CARTONI_PREP / ORE_PRODUTTIVE (NON colli/ora, non disponibili).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_produttivita_operatore AS
WITH agg_op AS (
    SELECT
        p.PREPARATORE_COD,
        p.SITO_COD,
        DATE_FORMAT(p.DATA_PREPARAZ, 'yyyyMM')         AS ANNO_MESE,
        COUNT(DISTINCT p.DATA_PREPARAZ)                AS GIORNI_LAVORATI,
        SUM(p.TOT_CARTONI_PREP)                        AS CARTONI_TOTALI,
        SUM(p.TOT_QUINTALI_PREP)                       AS QUINTALI_TOTALI,
        SUM(p.ORE_PRODUTTIVE)                          AS ORE_PRODUTTIVE_TOT,
        SUM(p.ORE_LAVORATE)                            AS ORE_LAVORATE_TOT,
        AVG(p.PRODUTTIVITA_CARTONI_ORA)                AS AVG_PROD_CARTONI_ORA,
        MAX(p.PRODUTTIVITA_CARTONI_ORA)                AS MAX_PROD_CARTONI_ORA,
        PERCENTILE_APPROX(p.PRODUTTIVITA_CARTONI_ORA, 0.5) AS MEDIANA_PROD_CARTONI_ORA,
        SUM(p.TOT_CARTONI_PREP) / NULLIF(SUM(p.ORE_PRODUTTIVE), 0) AS PROD_CUMULATA_CARTONI_ORA
    FROM gold_prod.logistica.F_PREP_SPED p
    GROUP BY p.PREPARATORE_COD, p.SITO_COD, DATE_FORMAT(p.DATA_PREPARAZ, 'yyyyMM')
)
SELECT
    a.PREPARATORE_COD,
    o.DESCRIZIONE                                       AS OPERATORE_DESC,
    a.SITO_COD,
    s.SITO_DESC,
    a.ANNO_MESE,
    a.GIORNI_LAVORATI,
    a.CARTONI_TOTALI,
    a.QUINTALI_TOTALI,
    a.ORE_PRODUTTIVE_TOT,
    a.ORE_LAVORATE_TOT,
    a.AVG_PROD_CARTONI_ORA,
    a.MAX_PROD_CARTONI_ORA,
    a.MEDIANA_PROD_CARTONI_ORA,
    a.PROD_CUMULATA_CARTONI_ORA,
    RANK() OVER (PARTITION BY a.SITO_COD, a.ANNO_MESE
                 ORDER BY a.PROD_CUMULATA_CARTONI_ORA DESC NULLS LAST) AS RANK_PRODUTTIVITA,
    PERCENT_RANK() OVER (PARTITION BY a.SITO_COD, a.ANNO_MESE
                         ORDER BY a.PROD_CUMULATA_CARTONI_ORA ASC NULLS FIRST) * 100
                                                       AS PERCENTILE_PRODUTTIVITA
FROM agg_op a
LEFT JOIN gold_prod.logistica.LU_OPERATORE o
       ON a.PREPARATORE_COD = o.OPERATORE_COD
      AND a.SITO_COD = o.SITO_COD
      AND o.TIPO_OPERATORE = 'PREPARATORE'
LEFT JOIN gold_prod.logistica.LU_SITO s ON a.SITO_COD = s.SITO_COD;
