-- =============================================================================
-- SCRIPT OTTIMIZZAZIONE TABELLE GOLD — Logistico 2.0  (v3.0  2026-06-08)
-- Frequenza consigliata: MENSILE (prima esecuzione dopo chiusura mese).
-- Allineato al naming Gold v3: LU_* (lookup), F_* (fact), A_* (aggregati datamart).
-- =============================================================================

-- ── Lookup logistiche (gold_prod.logistica) ──────────────────────────────────
OPTIMIZE gold_prod.logistica.LU_SITO
  ZORDER BY (SITO_COD);

OPTIMIZE gold_prod.logistica.LU_OPERATORE
  ZORDER BY (OPERATORE_COD, SITO_COD, TIPO_OPERATORE);

OPTIMIZE gold_prod.logistica.LU_CORRIERE
  ZORDER BY (CORRIERE_COD);

OPTIMIZE gold_prod.logistica.LU_TOPOGRAFIA
  ZORDER BY (CELLA_COD, MAG_COD);

OPTIMIZE gold_prod.logistica.LU_AREA_MERCL_LOGIS
  ZORDER BY (COD_AREA_MERC);

-- ── Fact tables (gold_prod.logistica) ────────────────────────────────────────
OPTIMIZE gold_prod.logistica.F_CARICO
  ZORDER BY (SITO_COD, FORNITORE_COD, MSI_COD);

OPTIMIZE gold_prod.logistica.F_GIACENZE_DAILY
  ZORDER BY (MAG_COD, ART_RADICE);

OPTIMIZE gold_prod.logistica.F_PREP_SPED
  ZORDER BY (SITO_COD, PREPARATORE_COD, RIEPILOGO_NRO);

OPTIMIZE gold_prod.logistica.F_ORDINI
  ZORDER BY (SITO_COD, FORNITORE_COD, CORRIERE_COD);

OPTIMIZE gold_prod.logistica.F_TRASPORTO
  ZORDER BY (CORRIERE_COD, SITO_COD, TRASPORTO_ID);

OPTIMIZE gold_prod.logistica.F_TRACCIABILITA_LOTTI
  ZORDER BY (SITO_COD, MSI_COD, CARICO_NRO);

OPTIMIZE gold_prod.logistica.F_MOVIMENTAZIONE_CARRELLISTI
  ZORDER BY (CARRELLISTA_COD, SITO_COD);

-- ── Aggregati DataMart (gold_prod.logistica_dm) ──────────────────────────────
OPTIMIZE gold_prod.logistica_dm.A_INBOUND_MENSILE
  ZORDER BY (FORNITORE_COD, SITO_COD);

OPTIMIZE gold_prod.logistica_dm.A_GIACENZE_MONTHLY
  ZORDER BY (ART_RADICE, MAG_COD);

OPTIMIZE gold_prod.logistica_dm.A_STOCK_MENSILE
  ZORDER BY (ART_RADICE, MAG_COD);

OPTIMIZE gold_prod.logistica_dm.A_OUTBOUND_MENSILE
  ZORDER BY (SITO_COD, CORRIERE_COD);

OPTIMIZE gold_prod.logistica_dm.A_PRODUTTIVITA_MENSILE
  ZORDER BY (SITO_COD);

OPTIMIZE gold_prod.logistica_dm.A_TURNO_PREP_SITO
  ZORDER BY (SITO_COD);

-- ── VACUUM (opzionale, oltre default 7 giorni) ───────────────────────────────
-- VACUUM gold_prod.logistica.F_CARICO RETAIN 720 HOURS;
