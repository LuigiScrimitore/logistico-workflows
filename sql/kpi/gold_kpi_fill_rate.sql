-- =============================================================================
-- KPI: FILL RATE ORDINI  (v3.0  2026-06-08)
-- Sorgente: gold_prod.logistica_dm.A_OUTBOUND_MENSILE.
-- NOTA OP-27: F_ORDINI non ha quantita' ordinate/consegnate (sono nel dettaglio carico).
--             Il fill-rate "quantitativo" canonico non e' calcolabile sui dati attuali;
--             esponiamo il rapporto NUM_TRASPORTI/NUM_ORDINI come proxy di servizio.
-- Lookup: LU_SITO, LU_CORRIERE.
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_fill_rate AS
SELECT
    a.SITO_COD,
    s.SITO_DESC,
    a.CORRIERE_COD,
    c.RAGIONE_SOCIALE                                  AS CORRIERE_DESC,
    a.ANNO_MESE,
    a.NUM_ORDINI,
    a.NUM_CARICHI,
    a.NUM_TRASPORTI,
    a.NUM_BOLLE,
    a.NUM_TRASFERITI,
    a.QTA_TRASPORTATA_TOT,
    -- Proxy di copertura: trasporti / ordini (NULL se NUM_ORDINI = 0)
    CASE WHEN a.NUM_ORDINI > 0 THEN a.NUM_TRASPORTI * 1.0 / a.NUM_ORDINI END AS FILL_RATE_PROXY,
    a.AVG_LEAD_TIME_GG,
    a.COSTO_STIMATO_EUR_TOT
FROM gold_prod.logistica_dm.A_OUTBOUND_MENSILE a
LEFT JOIN gold_prod.logistica.LU_SITO     s ON a.SITO_COD     = s.SITO_COD
LEFT JOIN gold_prod.logistica.LU_CORRIERE c ON a.CORRIERE_COD = c.CORRIERE_COD;
