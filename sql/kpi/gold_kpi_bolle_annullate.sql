-- =============================================================================
-- KPI: BOLLE ANNULLATE  (v3.0  2026-06-08)
-- Sorgenti: gold_prod.logistica.F_TRACCIABILITA_LOTTI (NUM_ANNULLATE su etichette CE178)
--           gold_prod.logistica.F_TRASPORTO          (STATO = annullato/resi via MTV_COD)
-- NOTA OP-27: il concetto di "bolla annullata" non e' direttamente disponibile come flag univoco;
--             usiamo due proxy complementari (etichette annullate e movimentazioni di reso/storno).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_bolle_annullate AS
WITH lotti AS (
    SELECT
        SITO_COD,
        ANNO_MESE,
        SUM(NUM_ETICHETTE)  AS NUM_ETICHETTE_TOT,
        SUM(NUM_ANNULLATE)  AS NUM_ETICHETTE_ANNULLATE
    FROM gold_prod.logistica.F_TRACCIABILITA_LOTTI
    GROUP BY SITO_COD, ANNO_MESE
),
trasp AS (
    SELECT
        SITO_COD,
        DATE_FORMAT(DATA_BOLLA, 'yyyyMM') AS ANNO_MESE,
        COUNT(*)                                                          AS NUM_TRASPORTI,
        SUM(CASE WHEN UPPER(STATO) IN ('ANNULLATA','ANNULLATO','RESO')
                 THEN 1 ELSE 0 END)                                       AS NUM_TRASPORTI_ANNULLATI_PROXY
    FROM gold_prod.logistica.F_TRASPORTO
    GROUP BY SITO_COD, DATE_FORMAT(DATA_BOLLA, 'yyyyMM')
)
SELECT
    COALESCE(l.SITO_COD, t.SITO_COD)             AS SITO_COD,
    s.SITO_DESC,
    COALESCE(l.ANNO_MESE, t.ANNO_MESE)           AS ANNO_MESE,
    l.NUM_ETICHETTE_TOT,
    l.NUM_ETICHETTE_ANNULLATE,
    CASE WHEN l.NUM_ETICHETTE_TOT > 0
         THEN l.NUM_ETICHETTE_ANNULLATE * 1.0 / l.NUM_ETICHETTE_TOT END AS TASSO_ETICHETTE_ANNULLATE,
    t.NUM_TRASPORTI,
    t.NUM_TRASPORTI_ANNULLATI_PROXY,
    CASE WHEN t.NUM_TRASPORTI > 0
         THEN t.NUM_TRASPORTI_ANNULLATI_PROXY * 1.0 / t.NUM_TRASPORTI END AS TASSO_TRASPORTI_ANNULLATI_PROXY
FROM lotti l
FULL OUTER JOIN trasp t
       ON l.SITO_COD = t.SITO_COD AND l.ANNO_MESE = t.ANNO_MESE
LEFT JOIN gold_prod.logistica.LU_SITO s
       ON COALESCE(l.SITO_COD, t.SITO_COD) = s.SITO_COD;
