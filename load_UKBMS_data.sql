
VACUUM ANALYZE;
-- set database memory allocation to 1GB for performance
SET work_mem TO '1GB';
SET maintenance_work_mem TO '1GB';

-- Cleanup existing entries for the given bms_id
DELETE FROM ebms.b_count USING ebms.m_visit WHERE ebms.m_visit.bms_id = 'UKBMS' AND ebms.b_count.visit_id = ebms.m_visit.visit_id;
DELETE FROM ebms.m_visit WHERE bms_id = 'UKBMS';
DELETE FROM ebms.b_recorder WHERE bms_id = 'UKBMS';
DELETE FROM ebms.m_site_geo WHERE site_id IN (SELECT site_id FROM ebms.m_site WHERE bms_id = 'UKBMS');
DELETE FROM ebms.m_site WHERE bms_id = 'UKBMS';
DELETE FROM ebms.bms_detail WHERE bms_id = 'UKBMS';

-- Drop indexes if they already exist
DROP INDEX IF EXISTS bms_site_id_uk_count;
DROP INDEX IF EXISTS visit_id_uk_count;

-- Index creation for performance
CREATE INDEX bms_site_id_uk_count ON uk_bms.uk_count (bms_site_id);
CREATE INDEX visit_id_uk_count ON uk_bms.uk_count (bms_visit_id);

-- Insert details into bms_detail
INSERT INTO ebms.bms_detail (bms_id, country_iso3, country_name, contact_name, contact_email)
VALUES ('UKBMS', 'GBR', 'United Kingdom of Great Britain and Northern Ireland', 'CONTACT NAME', 'contact@email');

-- Insert data into m_site
INSERT INTO ebms.m_site (bms_id, transect_id, transect_length, section_id, section_length, section_transect_length_equal, monitoring_type, site_info_quality, bms_site_id)
SELECT DISTINCT
  d.bms_id,
  d.bms_id || '.' || s.transect_id AS transect_id,
  s.transect_length,
  s.section_id,
  s.section_length,
  s.section_transect_length_equal,
  s.monitoring_type,
  s.site_info_quality,
  s.bms_site_id
FROM
  uk_bms.uk_site AS s
CROSS JOIN
  (SELECT bms_id FROM ebms.bms_detail WHERE bms_id = 'UKBMS') AS d
ORDER BY
  transect_id, section_id;

-- Insert data into b_recorder
INSERT INTO ebms.b_recorder (bms_id, bms_observer_id)
SELECT DISTINCT
  d.bms_id,
  bms_observer_id as bms_observer_id
FROM
  uk_bms.uk_visit,
  (SELECT bms_id FROM ebms.bms_detail WHERE bms_id = 'UKBMS') AS d
ORDER BY bms_observer_id;

-- Insert data into m_visit
INSERT INTO ebms.m_visit (bms_visit_id, bms_id, recorder_id, transect_id, visit_date, visit_start, visit_end, visit_cloud, visit_wind, completed)
SELECT
  v.bms_visit_id,
  d.bms_id,
  r.recorder_id,
  d.bms_id || '.' || transect_id,
  TO_DATE(v.visit_date, 'YYYY-MM-DD') AS DATE,
  CAST(to_timestamp(v.start_time, 'HH24:MI') AS TIME),
  CAST(to_timestamp(v.end_time, 'HH24:MI') AS TIME),
  v.visit_cloud,
  v.visit_wind,
  CAST(CAST(v.completed AS TEXT) AS BOOLEAN)
FROM
  (SELECT bms_id FROM ebms.bms_detail WHERE bms_id = 'UKBMS') AS d,
  (SELECT DISTINCT ON (bms_visit_id) * FROM uk_bms.uk_visit ORDER BY bms_visit_id, bms_observer_id) AS v
  LEFT JOIN (SELECT recorder_id, bms_observer_id FROM ebms.b_recorder WHERE bms_id = 'UKBMS') as r ON CAST(v.bms_observer_id as INTEGER) = CAST(r.bms_observer_id as INTEGER) 
ORDER BY bms_visit_id, visit_date, transect_id;

-- Create temporary table for counts
DROP TABLE IF EXISTS temp_count;

CREATE TABLE temp_count AS
SELECT
  v.visit_id,
  s.site_id,
  c.species_id,
  c.count
FROM
  uk_bms.uk_count AS c
JOIN
  (SELECT * FROM ebms.m_visit WHERE bms_id = 'UKBMS') AS v ON v.bms_visit_id = c.bms_visit_id
JOIN
  (SELECT * FROM ebms.m_site WHERE bms_id = 'UKBMS') AS s ON s.bms_site_id = c.bms_site_id
ORDER BY visit_id, site_id, species_id;

-- Insert into b_count in batches, for efficient memory usage
DO $do$
DECLARE
  n_insert INTEGER := 100000;
  max_insert INTEGER := (SELECT COUNT(*) FROM temp_count) + n_insert;
BEGIN
  FOR i IN 0..max_insert BY n_insert LOOP
    INSERT INTO ebms.b_count (visit_id, site_id, species_id, butterfly_count)
    SELECT *
    FROM temp_count
    ORDER BY site_id, visit_id, species_id
    LIMIT n_insert OFFSET i;
    RAISE NOTICE 'Counter: %', i;
  END LOOP;
END
$do$;

DROP TABLE IF EXISTS temp_count;

-- Add and update geometry columns
ALTER TABLE uk_bms.uk_site ADD COLUMN IF NOT EXISTS site_id INTEGER;
UPDATE uk_bms.uk_site SET site_id = s.site_id
FROM
  (SELECT site_id, bms_site_id FROM ebms.m_site WHERE bms_id = 'UKBMS') AS s
WHERE uk_site.bms_site_id = s.bms_site_id;

ALTER TABLE uk_bms.uk_site DROP COLUMN IF EXISTS centroid_geom;
ALTER TABLE uk_bms.uk_site ADD COLUMN centroid_geom GEOMETRY(Point, 3035);

UPDATE uk_bms.uk_site
SET centroid_geom = ST_SetSRID(ST_MakePoint(longitude::NUMERIC, latitude::NUMERIC), 3035);

ALTER TABLE uk_bms.uk_site DROP COLUMN IF EXISTS section_geom_true;
ALTER TABLE uk_bms.uk_site ADD COLUMN section_geom_true BOOLEAN;

UPDATE uk_bms.uk_site
SET section_geom_true = TRUE
WHERE geo_info_level = 'section_centroid';

UPDATE uk_bms.uk_site
SET section_geom_true = FALSE
WHERE geo_info_level = 'transect_centroid';

INSERT INTO ebms.m_site_geo (site_id, centroid_geom, section_geom_true)
SELECT DISTINCT site_id, centroid_geom, section_geom_true
FROM uk_bms.uk_site;

