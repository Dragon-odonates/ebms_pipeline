#' Create and Execute SQL Tables for Butterfly Monitoring Data
#'
#' This function constructs and executes an SQL script that inserts data into tables the eBMS database tables.
#' 
#' @param db_name A string specifying the name of the database.
#' @param db_port A numeric value specifying the database port.
#' @param db_user A string specifying the database username.
#' @param db_psw A string specifying the database password.
#' @param bms_val A string representing the BMS (Butterfly Monitoring Scheme) ID used to filter and manage data.
#' @param bms_table_name A string specifying the schema or table name prefix for butterfly monitoring data.
#' @param count_name A string specifying the name of the table containing species counts.
#' @param visit_name A string specifying the name of the table containing visit records.
#' @param site_name A string specifying the name of the table containing site information.
#' @param country_iso3 A string representing the ISO3 country code.
#' @param country_name A string specifying the name of the country.
#' @param contact_name A string specifying the contact person's name for the monitoring program.
#' @param contact_email A string specifying the contact person's email address.
#' @import glue
#'
#' @details
#' This function:
#' \itemize{
#'   \item Deletes old records related to the given BMS ID.
#'   \item Creates indexes for performance optimisation.
#'   \item Inserts new records into multiple tables (`ebms.bms_detail`, `ebms.m_site`, `ebms.b_recorder`, `ebms.m_visit`, etc.).
#'   \item Processes species count data and inserts them into `ebms.b_count` in batches.
#'   \item Updates geometry columns and site information.
#'   \item Generates an SQL script file for logging and execution.
#'   \item Executes the SQL script using the PostgreSQL `psql` command.
#' }
#'
#' The SQL script is saved under the path `sql_script/2023/sql_scripts_log/` with a filename formatted as `load_<bms_val>_data.sql`.
#'
#' @return This function does not return an R object. It generates and executes SQL queries in a PostgreSQL database.
#'
#' @export
#' 
Create_tables_SQL <- function(
  db_name,
  db_port,
  db_user,
  db_psw,
  bms_val,
  bms_table_name, 
  count_name, 
  visit_name,
  site_name,
  country_iso3, 
  country_name, 
  contact_name, 
  contact_email
){

  # SQL Query Construction
  sql_query <- glue::glue("

  VACUUM ANALYZE;

  SET work_mem TO '1GB';
  SET maintenance_work_mem TO '1GB';

  -- Cleanup existing entries for the given bms_id
  DELETE FROM ebms.b_count USING ebms.m_visit WHERE ebms.m_visit.bms_id = '{bms_val}' AND ebms.b_count.visit_id = ebms.m_visit.visit_id;
  DELETE FROM ebms.m_visit WHERE bms_id = '{bms_val}';
  DELETE FROM ebms.b_recorder WHERE bms_id = '{bms_val}';
  DELETE FROM ebms.m_site_geo WHERE site_id IN (SELECT site_id FROM ebms.m_site WHERE bms_id = '{bms_val}');
  DELETE FROM ebms.m_site WHERE bms_id = '{bms_val}';
  DELETE FROM ebms.bms_detail WHERE bms_id = '{bms_val}';

  -- Drop indexes if they already exist
  DROP INDEX IF EXISTS bms_site_id_{count_name};
  DROP INDEX IF EXISTS visit_id_{count_name};

  -- Index creation for performance
  CREATE INDEX bms_site_id_{count_name} ON {bms_table_name}.{count_name} (bms_site_id);
  CREATE INDEX visit_id_{count_name} ON {bms_table_name}.{count_name} (bms_visit_id);

  -- Insert details into bms_detail
  INSERT INTO ebms.bms_detail (bms_id, country_iso3, country_name, contact_name, contact_email)
  VALUES ('{bms_val}', '{country_iso3}', '{country_name}', '{contact_name}', '{contact_email}');

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
    {bms_table_name}.{site_name} AS s
  CROSS JOIN
    (SELECT bms_id FROM ebms.bms_detail WHERE bms_id = '{bms_val}') AS d
  ORDER BY
    transect_id, section_id;

  -- Insert data into b_recorder
  INSERT INTO ebms.b_recorder (bms_id, bms_observer_id)
  SELECT DISTINCT
    d.bms_id,
    bms_observer_id as bms_observer_id
  FROM
    {bms_table_name}.{visit_name},
    (SELECT bms_id FROM ebms.bms_detail WHERE bms_id = '{bms_val}') AS d
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
    (SELECT bms_id FROM ebms.bms_detail WHERE bms_id = '{bms_val}') AS d,
    (SELECT DISTINCT ON (bms_visit_id) * FROM {bms_table_name}.{visit_name} ORDER BY bms_visit_id, bms_observer_id) AS v
    LEFT JOIN (SELECT recorder_id, bms_observer_id FROM ebms.b_recorder WHERE bms_id = '{bms_val}') as r ON CAST(v.bms_observer_id as INTEGER) = CAST(r.bms_observer_id as INTEGER) 
  ORDER BY bms_visit_id, visit_date, transect_id;

  -- VACUUM ANALYZE;

  -- Create temporary table for counts
  DROP TABLE IF EXISTS temp_count;

  CREATE TABLE temp_count AS
  SELECT
    v.visit_id,
    s.site_id,
    c.species_id,
    c.count
  FROM
    {bms_table_name}.{count_name} AS c
  JOIN
    (SELECT * FROM ebms.m_visit WHERE bms_id = '{bms_val}') AS v ON v.bms_visit_id = c.bms_visit_id
  JOIN
    (SELECT * FROM ebms.m_site WHERE bms_id = '{bms_val}') AS s ON s.bms_site_id = c.bms_site_id
  ORDER BY visit_id, site_id, species_id;

  -- Insert into b_count in batches
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
  ALTER TABLE {bms_table_name}.{site_name} ADD COLUMN IF NOT EXISTS site_id INTEGER;
  UPDATE {bms_table_name}.{site_name} SET site_id = s.site_id
  FROM
    (SELECT site_id, bms_site_id FROM ebms.m_site WHERE bms_id = '{bms_val}') AS s
  WHERE {site_name}.bms_site_id = s.bms_site_id;

  ALTER TABLE {bms_table_name}.{site_name} DROP COLUMN IF EXISTS centroid_geom;
  ALTER TABLE {bms_table_name}.{site_name} ADD COLUMN centroid_geom GEOMETRY(Point, 3035);

  UPDATE {bms_table_name}.{site_name}
  SET centroid_geom = ST_SetSRID(ST_MakePoint(longitude::NUMERIC, latitude::NUMERIC), 3035);

  ALTER TABLE {bms_table_name}.{site_name} DROP COLUMN IF EXISTS section_geom_true;
  ALTER TABLE {bms_table_name}.{site_name} ADD COLUMN section_geom_true BOOLEAN;

  UPDATE {bms_table_name}.{site_name}
  SET section_geom_true = TRUE
  WHERE geo_info_level = 'section_centroid';

  UPDATE {bms_table_name}.{site_name}
  SET section_geom_true = FALSE
  WHERE geo_info_level = 'transect_centroid';

  INSERT INTO ebms.m_site_geo (site_id, centroid_geom, section_geom_true)
  SELECT DISTINCT site_id, centroid_geom, section_geom_true
  FROM {bms_table_name}.{site_name};

  ")

sql_file <- file.path("sql_script/2023","sql_scripts_log",(paste0("load_",bms_val,"_data.sql")))

fileConn<-file(sql_file)
writeLines(sql_query, fileConn)
close(fileConn)

# Database connection details
  Sys.setenv(PGPASSWORD = db_psw)
  
  db_details <- paste0('-h localhost -U ', db_user, ' -d ', db_name,' -p ', db_port)

  system2('psql', args = paste(db_details, "-c", "VACUUM;"))

  # Execute the psql command
  system2('psql', 
          args = paste(db_details, '-f', sql_file),
          stderr = TRUE, 
          stdout = TRUE)

  system2('psql', args = paste(db_details, "-c", "VACUUM;"))

}
