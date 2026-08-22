-- =============================================================================
-- KPI: QUALITA' RICEVIMENTO  (v4.0  2026-07-04)
-- Sorgente: gold_prod.logistica_dm.A_INBOUND_MENSILE (aggregato mensile).
-- Ammanco ricevimento = quantità ordinata − quantità ricevuta (misura di business,
--   NON scarto-di-record). Ex "scarto" rinominato in "ammanco" per chiarezza.
--   Riabilitata dopo OP-CAR-3 (QTA_ORD_FORN ora popolata → SUM ordinato disponibile).
--   Calcolo a livello aggregato (il grain etichetta non consente il per-riga).
-- Lookup logistica: LU_SITO (gold_prod.logistica).
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_qualita_ricevimento AS
SELECT
    a.FORNITORE_COD,
    a.SITO_COD,
    s.SITO_DESC,
    a.ANNO_MESE,
    a.NUM_CARICHI,
    a.QTA_ORDINATA_TOT,
    a.QTA_CARICO_TOT                                    AS QTA_RICEVUTA_TOT,
    a.AMMANCO_QTA_TOT,
    a.TASSO_AMMANCO
FROM gold_prod.logistica_dm.A_INBOUND_MENSILE a
LEFT JOIN gold_prod.logistica.LU_SITO s ON a.SITO_COD = s.SITO_COD;
