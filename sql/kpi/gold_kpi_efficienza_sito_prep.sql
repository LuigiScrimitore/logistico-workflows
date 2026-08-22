-- =============================================================================
-- KPI: EFFICIENZA SITO PREPARAZIONE  (v3.0  2026-06-08)
-- Sorgente: gold_prod.logistica_dm.A_PRODUTTIVITA_MENSILE.
-- Lookup: LU_SITO.
-- =============================================================================
CREATE OR REPLACE VIEW gold_prod.logistica.kpi_efficienza_sito_prep AS
SELECT
    a.SITO_COD,
    s.SITO_DESC,
    a.ANNO_MESE,
    a.TOT_CARTONI_PREP,
    a.TOT_QUINTALI_PREP,
    a.TOT_CARTONI_INEVASI,
    a.ORE_PRODUTTIVE_TOT,
    a.ORE_LAVORATE_TOT,
    a.OPERATORI_DISTINTI,
    a.NUM_RIEPILOGHI,
    a.PRODUTTIVITA_CARTONI_ORA_MEDIA,
    a.PRODUTTIVITA_CARTONI_ORA_MAX,
    a.PRODUTTIVITA_CARTONI_ORA_MEDIANA,
    a.PRODUTTIVITA_CARTONI_ORA_AGGR,
    a.PERC_ORE_ATTREZZAGGIO
FROM gold_prod.logistica_dm.A_PRODUTTIVITA_MENSILE a
LEFT JOIN gold_prod.logistica.LU_SITO s ON a.SITO_COD = s.SITO_COD;
