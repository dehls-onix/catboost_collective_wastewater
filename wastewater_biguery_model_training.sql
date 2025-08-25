--   ____      _   ____                  _       ____      _ _           _   _           
--  / ___|__ _| |_| __ )  ___   ___  ___| |_    / ___|___ | | | ___  ___| |_(_)_   _____ 
-- | |   / _` | __|  _ \ / _ \ / _ \/ __| __|  | |   / _ \| | |/ _ \/ __| __| \ \ / / _ \
-- | |__| (_| | |_| |_) | (_) | (_) \__ \ |_   | |__| (_) | | |  __/ (__| |_| |\ V /  __/
--  \____\__,_|\__|____/ \___/ \___/|___/\__|___\____\___/|_|_|\___|\___|\__|_| \_/ \___|
--                                         |_____|                                       


-- TRAIN THE GRADIENT BOOSTED PREDICTIVE MODEL 
CREATE OR REPLACE MODEL `prj-ox-int-g-looker.kingfisher_hack.wastewater_threat_predictor`
OPTIONS(
  model_type='BOOSTED_TREE_CLASSIFIER',
  input_label_cols=['threat_level_class'],
  auto_class_weights=TRUE,
  enable_global_explain=TRUE
) AS

-- unified and CLEANED training set within the model creation statement.
WITH UnifiedTrainingData AS (

  # REAL DATA
  SELECT
    'Real' AS data_source,
    sample_id,
    sample_date,
    cast(zipcode as string) zipcode,
    city,
    state,
    sars_cov_2_signal,
    influenza_a_signal,
    rsv_signal, 
    norovirus_signal,
    h5n1_avian_flu_signal,
    measles_signal,
    polio_wild_type_detected,
    candida_auris_detected,
    opioid_metabolite_proxy,
    stimulant_metabolite_proxy,
    population_stress_index,
    regional_variant_marker,
    environmental_risk_factor,
    precipitation_mm_last_72h,
    avg_air_temp_celsius,
    is_near_weekend,
    threat_level_class
  FROM `prj-ox-int-g-looker.kingfisher_hack.public_health_sentinel`

  UNION ALL

  -- SYNTHETIC KINGFISHER DATA
  SELECT
    'Synthetic' AS data_source,
    CAST(sample_id AS INT64) AS sample_id, 
    CAST(sample_date AS DATE) AS sample_date, 
    cast(zipcode as string) zipcode,
    city,
    state,
    sars_cov_2_signal,
    influenza_a_signal,
    rsv_signal, 
    norovirus_signal,
    h5n1_avian_flu_signal,
    measles_signal,
    polio_wild_type_detected,
    candida_auris_detected,
    opioid_metabolite_proxy,
    stimulant_metabolite_proxy,
    population_stress_index,
    regional_variant_marker,
    environmental_risk_factor,
    precipitation_mm_last_72h,
    avg_air_temp_celsius,
    is_near_weekend,
    threat_level_class
  FROM `prj-ox-int-g-looker.kingfisher_hack.public_health_sentinel_kf`
)

# prediction
SELECT
  sars_cov_2_signal,
  influenza_a_signal,
  rsv_signal,
  h5n1_avian_flu_signal,
  measles_signal,
  polio_wild_type_detected,
  opioid_metabolite_proxy,
  population_stress_index,
  regional_variant_marker,
  avg_air_temp_celsius,
  threat_level_class
FROM UnifiedTrainingData
WHERE data_source = 'Synthetic' OR MOD(ABS(FARM_FINGERPRINT(CAST(sample_id AS STRING))), 10) < 7;