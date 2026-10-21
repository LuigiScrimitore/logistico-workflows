-- =============================================================================
-- KPI: RESA CORRIERI (Puntualita')  (v3.0  2026-06-08)
-- Sorgente: gold_prod.logistica.F_TRASPORTO.
-- NOTA OP-27: il sorgente non ha FLG_RITARDO ne' DATA_CONSEGNA_EFFETTIVA. Usiamo come proxy
--             LEAD_TIME_GG vs DATA_CONSEGNA_PREV: ritardato se DATA_AZIONE > DATA_CONSEGNA_PREV.
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_resa_corrieri AS
WITH base AS (
    SELECT
        t.VETTORE_COD,
        t.SITO_COD,
        DATE_FORMAT(t.DATA_BOLLA, 'yyyyMM') AS ANNO_MESE,
        t.LEAD_TIME_GG,
        CASE
            WHEN t.DATA_AZIONE IS NULL OR t.DATA_CONSEGNA_PREV IS NULL THEN NULL
            WHEN t.DATA_AZIONE > t.DATA_CONSEGNA_PREV THEN 1
            ELSE 0
        END AS FLG_RITARDO_PROXY
    FROM gold_prod.logistica.F_TRASPORTO t
)
SELECT
    b.VETTORE_COD,
    c.RAGIONE_SOCIALE                                          AS CORRIERE_DESC,
    b.SITO_COD,
    s.SITO_DESC,
    b.ANNO_MESE,
    COUNT(*)                                                   AS NUM_TRASPORTI,
    SUM(CASE WHEN b.FLG_RITARDO_PROXY = 1 THEN 1 ELSE 0 END)   AS NUM_RITARDI,
    SUM(CASE WHEN b.FLG_RITARDO_PROXY = 0 THEN 1 ELSE 0 END)   AS NUM_PUNTUALI,
    AVG(b.LEAD_TIME_GG)                                        AS AVG_LEAD_TIME_GG,
    SUM(CASE WHEN b.FLG_RITARDO_PROXY = 0 THEN 1 ELSE 0 END) * 1.0
       / NULLIF(SUM(CASE WHEN b.FLG_RITARDO_PROXY IS NOT NULL THEN 1 ELSE 0 END), 0)
                                                               AS PERC_PUNTUALITA,
    RANK() OVER (PARTITION BY b.SITO_COD, b.ANNO_MESE
                 ORDER BY (SUM(CASE WHEN b.FLG_RITARDO_PROXY = 0 THEN 1 ELSE 0 END) * 1.0
                          / NULLIF(SUM(CASE WHEN b.FLG_RITARDO_PROXY IS NOT NULL THEN 1 ELSE 0 END), 0)) DESC NULLS LAST)
                                                               AS RANK_PUNTUALITA
FROM base b
LEFT JOIN gold_prod.logistica.LU_VETTORE c ON b.VETTORE_COD = c.VETTORE_COD
LEFT JOIN gold_prod.logistica.LU_SITO     s ON b.SITO_COD     = s.SITO_COD
GROUP BY b.VETTORE_COD, c.RAGIONE_SOCIALE, b.SITO_COD, s.SITO_DESC, b.ANNO_MESE;
