-- =============================================================================
-- KPI: COSTO TRASPORTO  (v3.0  2026-06-08)
-- Sorgente: gold_prod.logistica.F_TRASPORTO.
-- Lookup: LU_CORRIERE, LU_SITO.
-- NOTA: COSTO_STIMATO_EUR e' un PLACEHOLDER (peso*0.15) — da sostituire con il listino reale
--       quando disponibile (OP). PESO/VOLUME reali non disponibili nel sorgente attuale (OP-27).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_costo_trasporto AS
SELECT
    t.CORRIERE_COD,
    c.RAGIONE_SOCIALE                                    AS CORRIERE_DESC,
    t.SITO_COD,
    s.SITO_DESC,
    DATE_FORMAT(t.DATA_BOLLA, 'yyyyMM')                  AS ANNO_MESE,
    COUNT(*)                                             AS NUM_TRASPORTI,
    SUM(t.QTA)                                           AS QTA_TOT,
    SUM(t.COSTO_STIMATO_EUR)                             AS COSTO_STIMATO_EUR_TOT,  -- placeholder OP
    AVG(t.LEAD_TIME_GG)                                  AS AVG_LEAD_TIME_GG
FROM gold_prod.logistica.F_TRASPORTO t
LEFT JOIN gold_prod.logistica.LU_CORRIERE c ON t.CORRIERE_COD = c.CORRIERE_COD
LEFT JOIN gold_prod.logistica.LU_SITO     s ON t.SITO_COD     = s.SITO_COD
GROUP BY t.CORRIERE_COD, c.RAGIONE_SOCIALE, t.SITO_COD, s.SITO_DESC,
         DATE_FORMAT(t.DATA_BOLLA, 'yyyyMM');
