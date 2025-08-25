--   ____      _   ____                  _       ____      _ _           _   _           
--  / ___|__ _| |_| __ )  ___   ___  ___| |_    / ___|___ | | | ___  ___| |_(_)_   _____ 
-- | |   / _` | __|  _ \ / _ \ / _ \/ __| __|  | |   / _ \| | |/ _ \/ __| __| \ \ / / _ \
-- | |__| (_| | |_| |_) | (_) | (_) \__ \ |_   | |__| (_) | | |  __/ (__| |_| |\ V /  __/
--  \____\__,_|\__|____/ \___/ \___/|___/\__|___\____\___/|_|_|\___|\___|\__|_| \_/ \___|
--                                         |_____|                                       


-- Creates KingFisher seed table
CREATE OR REPLACE TABLE `prj-ox-int-g-looker.kingfisher_hack.public_health_sentinel` AS

-- Step 1: Use our corrected, geographically distributed zip code selector.
WITH zipcode_base AS (
  WITH RankedZips AS (
    SELECT
      zipcode, city, state_code, latitude, longitude,
      ROW_NUMBER() OVER(PARTITION BY state_code ORDER BY area_land_meters DESC) as state_rank
    FROM `bigquery-public-data.utility_us.zipcode_area`
    WHERE state_code IN ('AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY') AND area_land_meters > 0
  )
  SELECT zipcode, city, state_code AS state, latitude, longitude FROM RankedZips WHERE state_rank <= 60
),

-- Step 2: Generate a 52-week time series for our seed data.
date_series AS (
  SELECT sample_date FROM UNNEST(GENERATE_DATE_ARRAY('2025-08-13', '2025-08-23', INTERVAL 7 DAY)) AS sample_date
),

-- Step 3: Create the base by cross-joining real locations with our time series.
base_cross_join AS (
  SELECT d.sample_date, z.* FROM date_series d CROSS JOIN zipcode_base z
),

-- Step 4: FIRST LEVEL CTE - Generate all raw feature signals.
-- All aliases created here will be available to the next level of the query.
GeneratedFeatures AS (
  SELECT
    -- Foundational Identifiers
    FARM_FINGERPRINT(CONCAT(CAST(b.sample_date AS STRING), b.zipcode)) AS sample_id,
    b.sample_date,
    b.zipcode,
    b.city,
    b.state,

    -- Core Viral Panel
    ABS(ROUND(50 + (250 * SIN(EXTRACT(DAYOFYEAR FROM b.sample_date) * 3.14159 / 182.5)) + (RAND() * 50), 2)) AS sars_cov_2_signal,
    ABS(ROUND(40 + (300 * SIN(EXTRACT(DAYOFYEAR FROM b.sample_date) * 3.14159 / 182.5 + 3.14159)) + (RAND() * 40), 2)) AS influenza_a_signal,
    ABS(ROUND(30 + (200 * SIN(EXTRACT(DAYOFYEAR FROM b.sample_date) * 3.14159 / 182.5 + 3.14159)) + (RAND() * 30), 2)) AS rsv_signal,
    CASE WHEN RAND() < 0.1 THEN ABS(ROUND(100 + (RAND() * 50), 2)) ELSE 0 END AS norovirus_signal,

    -- High-Threat Biothreat Panel
    CASE WHEN RAND() < 0.01 THEN ABS(ROUND(10 + (RAND() * 20), 2)) ELSE 0 END AS h5n1_avian_flu_signal,
    CASE WHEN RAND() < 0.005 THEN ABS(ROUND(5 + (RAND() * 10), 2)) ELSE 0 END AS measles_signal,
    CASE WHEN RAND() < 0.001 THEN 1 ELSE 0 END AS polio_wild_type_detected,
    CASE WHEN RAND() < 0.02 THEN 1 ELSE 0 END AS candida_auris_detected,

    -- Community Stress & Behavior Panel
    ABS(ROUND(250 + MOD(FARM_FINGERPRINT(b.zipcode), 200) + (RAND() * 50), 2)) AS opioid_metabolite_proxy,
    ABS(ROUND(80 + MOD(FARM_FINGERPRINT(b.city), 50) + (RAND() * 20), 2)) AS stimulant_metabolite_proxy,
    ABS(ROUND(100 + (20 * SIN(EXTRACT(DAYOFYEAR FROM b.sample_date) * 3.14159 / 120)) + (RAND() * 15), 2)) AS population_stress_index,

    -- Latent & Environmental Features
    CONCAT('V-REG-0', MOD(ABS(CAST(FARM_FINGERPRINT(b.state) AS INT64)), 4) + 1) AS regional_variant_marker,
    MOD(ABS(CAST(FARM_FINGERPRINT(b.zipcode) AS INT64)), 10) / 10 AS environmental_risk_factor,
    ABS(ROUND(RAND() * 150, 2)) AS precipitation_mm_last_72h,
    ROUND(15 + (15 * SIN(EXTRACT(DAYOFYEAR FROM b.sample_date) * 3.14159/182.5 - 1.57)) - (b.latitude - 39), 1) AS avg_air_temp_celsius,
    CASE WHEN EXTRACT(DAYOFWEEK FROM b.sample_date) IN (1, 2, 6, 7) THEN 1 ELSE 0 END AS is_near_weekend
  FROM base_cross_join b
)

# final
SELECT
  *,
  -- THE TARGET VARIABLE (Multi-Class Classification)
  CASE
    WHEN polio_wild_type_detected = 1 OR h5n1_avian_flu_signal > 25 THEN 'Level 5: Critical Biosecurity Threat'
    WHEN measles_signal > 10 OR (influenza_a_signal > 350 AND population_stress_index > 120) THEN 'Level 4: Severe Community Outbreak'
    WHEN sars_cov_2_signal > 280 OR influenza_a_signal > 300 THEN 'Level 3: Heightened Viral Activity'
    WHEN norovirus_signal > 100 OR opioid_metabolite_proxy > 400 THEN 'Level 2: Elevated Local Concern'
    ELSE 'Level 1: Endemic Baseline'
  END AS threat_level_class
FROM GeneratedFeatures;