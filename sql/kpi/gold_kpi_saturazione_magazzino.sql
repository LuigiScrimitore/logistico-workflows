-- =============================================================================
-- KPI: SATURAZIONE MAGAZZINO  (v3.0  2026-06-08)
-- Sorgenti: gold_prod.logistica.F_GIACENZE_DAILY + LU_TOPOGRAFIA.
-- Le giacenze sono per MAG_COD (non per cella) -> aggreghiamo per (DATA, MAG_COD).
-- Una saturazione per CELLA puntuale non e' ricavabile dai dati attuali (OP).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_saturazione_magazzino AS
WITH giac AS (
    SELECT
        DATA_FOTO,
        MAG_COD,
        SUM(QTA_PEZZI)            AS QTA_PEZZI_TOT,
        SUM(QTA_UF)               AS QTA_UF_TOT,
        COUNT(DISTINCT ART_RADICE) AS NUM_ARTICOLI
    FROM gold_prod.logistica.F_GIACENZE_DAILY
    GROUP BY DATA_FOTO, MAG_COD
),
celle AS (
    SELECT
        MAG_COD,
        COUNT(*) AS NUM_CELLE,
        SUM(CASE WHEN STATO_POSPA IS NOT NULL THEN 1 ELSE 0 END) AS NUM_CELLE_VALORIZZATE
    FROM gold_prod.logistica.LU_TOPOGRAFIA
    GROUP BY MAG_COD
)
SELECT
    g.DATA_FOTO,
    g.MAG_COD,
    g.QTA_PEZZI_TOT,
    g.QTA_UF_TOT,
    g.NUM_ARTICOLI,
    c.NUM_CELLE,
    c.NUM_CELLE_VALORIZZATE
FROM giac g
LEFT JOIN celle c ON g.MAG_COD = c.MAG_COD;
