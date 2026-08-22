-- =============================================================================
-- KPI: AGING ARTICOLI  (v3.0  2026-06-08)
-- Sorgente: gold_prod.logistica.F_GIACENZE_DAILY.
-- Aging = CURRENT_DATE - MIN(DATA_FOTO) per (ART_RADICE, MAG_COD).
-- Join master Retail LU_ART_RADICE OPZIONALE (OP-02).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_aging_articoli AS
SELECT
    g.ART_RADICE,
    -- a.DESCRIZIONE      AS ART_DESC,   -- abilitare quando LU_ART_RADICE disponibile (OP-02)
    g.MAG_COD,
    MIN(g.DATA_FOTO)                            AS DATA_PRIMA_GIACENZA,
    MAX(g.DATA_FOTO)                            AS DATA_ULTIMA_GIACENZA,
    DATEDIFF(CURRENT_DATE(), MIN(g.DATA_FOTO))  AS GIORNI_AGING,
    AVG(g.QTA_PEZZI)                            AS AVG_QTA_PEZZI,
    AVG(g.QTA_IN_SCADENZA)                      AS AVG_QTA_IN_SCADENZA
FROM gold_prod.logistica.F_GIACENZE_DAILY g
-- LEFT JOIN gold_prod.condiviso.LU_ART_RADICE a ON g.ART_RADICE = a.ART_RADICE_COD  -- OP-02
GROUP BY g.ART_RADICE, g.MAG_COD;
