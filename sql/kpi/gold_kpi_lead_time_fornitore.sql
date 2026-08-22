-- =============================================================================
-- KPI: VOLUMI INBOUND FORNITORE  (v4.0  2026-07-04)
-- Sorgente: gold_prod.logistica_dm.A_INBOUND_MENSILE (aggregato mensile gia' calcolato).
-- Lookup master (Retail, OP-02): LU_FORNITORE -- join OPZIONALE (commentata se non disponibile).
-- Lookup logistica: LU_SITO (gold_prod.logistica).
-- ⚠️ RIALLINEATO ad A_INBOUND_MENSILE v4.0 (grain etichetta F_CARICO). Le misure di scarto/
--    quantita' ordinata NON sono piu' disponibili (dipendono da OP-CAR-3). Esponiamo i volumi
--    di carico (qta/peso/volume/pallet). Il lead time puntuale resta non disponibile (OP-27).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_volumi_inbound_fornitore AS
SELECT
    a.FORNITORE_COD,
    -- f.RAGIONE_SOCIALE        AS FORNITORE_DESC,        -- abilitare quando LU_FORNITORE disponibile (OP-02)
    a.SITO_COD,
    s.SITO_DESC,
    a.ANNO_MESE,
    a.NUM_CARICHI,
    a.NUM_ETICHETTE,
    a.QTA_CARICO_TOT,
    a.QTA_UF_CARICO_TOT,
    a.PESO_CARICO_TOT,
    a.VOL_CARICO_TOT,
    a.NUM_PLT_TOT,
    a.NUM_IMB_TOT
FROM gold_prod.logistica_dm.A_INBOUND_MENSILE a
LEFT JOIN gold_prod.logistica.LU_SITO s ON a.SITO_COD = s.SITO_COD
-- LEFT JOIN gold_prod.condiviso.LU_FORNITORE f ON a.FORNITORE_COD = f.FORNITORE_COD   -- OP-02
;
