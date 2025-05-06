/*
Create embs database schema
Author: Reto Schmucki - retoschm@ceh.ac.uk
Date: 07/10/2024
*/


/*
-- NOTE: with new species checklist - Wiemer et al. 2019
-- Need to connect first for interactive session

psql -h localhost -U "ebms_user" -p 5432 -d ebms_v6_1
\connect ebms_v6_1

*/

CREATE OR REPLACE FUNCTION try_cast_to_time(text) RETURNS time AS $$
  BEGIN
      return cast(rtrim(LEFT(replace(replace(replace(regexp_replace($1,'[^0-9\.\:\,\-]+', '', 'g'),',',':'),'.',':'),'-',':')||':00:00',8),':')::VARCHAR AS TIME);
  EXCEPTION
      WHEN OTHERS THEN
          RETURN NULL;
  END;
$$ LANGUAGE plpgsql immutable;

CREATE OR REPLACE FUNCTION try_extract_integer(text,integer) RETURNS integer AS $$
BEGIN
	return cast(LEFT(ceil(split_part(LEFT(replace(regexp_replace($1,'[^0-9\.\,\-]+', '', 'g'),',','.'),4),'-',1)::numeric)::varchar,$2) AS INTEGER);
EXCEPTION
	WHEN OTHERS THEN
		RETURN NULL;
END;
$$ LANGUAGE plpgsql immutable;

DROP SCHEMA IF EXISTS ebms CASCADE;

CREATE SCHEMA ebms
       AUTHORIZATION ebms_user;

-- METADATA table with bms_id as primary key (e.g. United Kingdom = UKBMS)

DROP TABLE IF EXISTS ebms.bms_detail CASCADE;

CREATE TABLE ebms.bms_detail
(
	bms_id varchar(255),
	country_iso3 varchar(255),
	country_name varchar(255),
	contact_name varchar(255),
	contact_email varchar(255),

	CONSTRAINT pk_bms_detail PRIMARY KEY (bms_id)
)
;

ALTER TABLE ebms.bms_detail
  OWNER TO ebms_user;

COMMENT ON TABLE ebms.bms_detail
  IS 'Identification and contact details for each national BMS participating in eBMS';

COMMENT ON COLUMN ebms.bms_detail.bms_id IS 'Code used to identify the national scheme (e.g. UKBMS)';
COMMENT ON COLUMN ebms.bms_detail.country_iso3 IS 'ISO 3-letter code of the country for a national scheme';
COMMENT ON COLUMN ebms.bms_detail.country_name IS 'Full name of the country for a national scheme';
COMMENT ON COLUMN ebms.bms_detail.contact_name IS 'Name of the main contact person that signed the agreement for the national scheme';
COMMENT ON COLUMN ebms.bms_detail.contact_email IS 'Email of the main contact person that signed the agreement for the national scheme';

-- Habitat

DROP TABLE IF EXISTS ebms.habitat_thesaurus CASCADE;

CREATE TABLE ebms.habitat_thesaurus
(
	habitat_id SERIAL,
	bms_habitat text,
	eunis_code varchar(255),
	eunis_level_1 varchar(255),
	eunis_level_2 varchar(255),
	eunis_level_3 varchar(255),

	CONSTRAINT pk_habitat_thesaurus PRIMARY KEY (habitat_id)
)
;

ALTER TABLE ebms.habitat_thesaurus
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.habitat_thesaurus
  IS 'Thesaurus of habitat type using EUNIS classification as standard and the original classification used in the National BMS';

COMMENT ON COLUMN ebms.habitat_thesaurus.habitat_id IS 'Unique numeric code for a specific habitat in the defined in national BMS';
COMMENT ON COLUMN ebms.habitat_thesaurus.bms_habitat IS 'Name of the habitat used in the national BMS';
COMMENT ON COLUMN ebms.habitat_thesaurus.eunis_code IS 'EUNIS 3 levels habitat code';
COMMENT ON COLUMN ebms.habitat_thesaurus.eunis_level_1 IS 'EUNIS level 1 habitat name';
COMMENT ON COLUMN ebms.habitat_thesaurus.eunis_level_2 IS 'EUNIS level 2 habitat name';
COMMENT ON COLUMN ebms.habitat_thesaurus.eunis_level_3 IS 'EUNIS level 3 habitat name';

-- B_SPECIES_id

DROP TABLE IF EXISTS ebms.b_species_id CASCADE;

CREATE TABLE ebms.b_species_id
(
	species_id integer UNIQUE,
	species_acpt_sci_name varchar(255) UNIQUE,
	aggregate boolean,

	CONSTRAINT pk_b_species_id PRIMARY KEY (species_id)
)
;

ALTER TABLE ebms.b_species_id
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.b_species_id
	IS 'List of accepted species name from Wiemer et al. 2019 European Checklist (https://zookeys.pensoft.net/article/28712/list/13/) with their Systematic_Order used as id code in the eBMS database';

COMMENT ON COLUMN ebms.b_species_id.species_id IS 'Unique code for recorded species as for the accepted name based on the Systematic_Order in Wiemer et al. 2019 and id above 1000 for aggregates';
COMMENT ON COLUMN ebms.b_species_id.species_acpt_sci_name IS 'Accepted scientific name based on Wiemer et al. 2019 European Checklist'; 
COMMENT ON COLUMN ebms.b_species_id.aggregate IS 'TRUE if the concept refers to an aggregate of species with species_id larger 1000';

--- B_SPECIES THESAURUS

DROP TABLE IF EXISTS ebms.species_thesaurus CASCADE;

CREATE TABLE ebms.species_thesaurus
(
	species_sci_name varchar(255),
	species_acpt_sci_name varchar(255),
	species_acpt_sci_authority varchar(255),
	species_english_name varchar(255),
	species_dutch_name varchar(255),
	species_german_name varchar(255),
	species_spanish_name varchar(255),
	species_finish_name varchar(255),
	species_french_name varchar(255),
	species_swedish_name varchar(255),
	species_id integer,

  CONSTRAINT pk_species_thesaurus PRIMARY KEY (species_sci_name),
  CONSTRAINT fk_species_thesaurus_species_id FOREIGN KEY (species_id) REFERENCES ebms.b_species_id (species_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

ALTER TABLE ebms.species_thesaurus
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.species_thesaurus
	IS 'List of species used by national BMS, with synonyms, Fauna Europea accepted scientific name and common name used in multiple languages';

COMMENT ON COLUMN ebms.species_thesaurus.species_sci_name IS 'Used scientific name in the national BMS';
COMMENT ON COLUMN ebms.species_thesaurus.species_acpt_sci_name IS 'Accepted scientific name based on Fauna Europea';
COMMENT ON COLUMN ebms.species_thesaurus.species_acpt_sci_authority IS 'Accepted authority based on Fauna Europea';
COMMENT ON COLUMN ebms.species_thesaurus.species_english_name IS 'Common name used in English';
COMMENT ON COLUMN ebms.species_thesaurus.species_dutch_name IS 'Common name used in Dutch';
COMMENT ON COLUMN ebms.species_thesaurus.species_german_name IS 'Common name used in German';
COMMENT ON COLUMN ebms.species_thesaurus.species_spanish_name IS 'Common name used in Spanish';
COMMENT ON COLUMN ebms.species_thesaurus.species_finish_name IS 'Common name used in Finish';
COMMENT ON COLUMN ebms.species_thesaurus.species_french_name IS 'Common name used in French';
COMMENT ON COLUMN ebms.species_thesaurus.species_swedish_name IS 'Common name used in Swedish';
COMMENT ON COLUMN ebms.species_thesaurus.species_id IS 'Unique code for recorded species as for the accepted name';

-- B_Recorder

DROP TABLE IF EXISTS ebms.b_recorder CASCADE;

CREATE TABLE ebms.b_recorder
(
	recorder_id SERIAL,
	bms_id varchar(255),
	bms_observer_id varchar(255),

  CONSTRAINT pk_b_recorder PRIMARY KEY (recorder_id),
  CONSTRAINT fk_b_recorder_bms_id FOREIGN KEY (bms_id) REFERENCES ebms.bms_detail (bms_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

ALTER TABLE ebms.b_recorder
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.b_recorder
  IS 'Identification of the recorder and link to the National BMS id';

COMMENT ON COLUMN ebms.b_recorder.recorder_id IS 'Unique anonymized identifier for the eBMS';
COMMENT ON COLUMN ebms.b_recorder.bms_id IS 'Name of the National BMS';
COMMENT ON COLUMN ebms.b_recorder.bms_observer_id IS 'Identification code for the recorder in the original National BMS';


-- M_Visit table

DROP TABLE IF EXISTS ebms.m_visit CASCADE;

CREATE TABLE ebms.m_visit
(
	visit_id SERIAL NOT NULL,
	bms_visit_id integer,
	recorder_id integer,
	bms_id varchar(255),
	transect_id varchar(255),
	visit_date date,
	visit_start time,
	visit_end time,
	visit_temp integer,
	visit_cloud integer,
	visit_wind integer,
	completed boolean,

  CONSTRAINT pk_m_visit PRIMARY KEY (visit_id),
  CONSTRAINT fk_m_visit_recorder_id FOREIGN KEY (recorder_id) REFERENCES ebms.b_recorder (recorder_id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_m_visit_bms_id FOREIGN KEY (bms_id) REFERENCES ebms.bms_detail (bms_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

ALTER TABLE ebms.m_visit
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.m_visit
  IS 'Identify condition for each visit which refer to a specific monitoring event';

COMMENT ON COLUMN ebms.m_visit.visit_id IS 'Unique id within eBMS for a specific monitoring event';
COMMENT ON COLUMN ebms.m_visit.bms_visit_id IS 'Unique id used in the original BMS for a specific monitoring event';
COMMENT ON COLUMN ebms.m_visit.recorder_id IS 'eBMS anonymous unique code for recorder';
COMMENT ON COLUMN ebms.m_visit.bms_id IS 'Name of the national scheme (e.g. UKBMS)';
COMMENT ON COLUMN ebms.m_visit.transect_id IS 'Identification of the transect where the visit took place (e.g. UKBMS.45)';
COMMENT ON COLUMN ebms.m_visit.visit_date IS 'Date with no time of day (YYYY-MM-DD)';
COMMENT ON COLUMN ebms.m_visit.visit_start IS 'Time of day when specific monitoring event started';
COMMENT ON COLUMN ebms.m_visit.visit_end IS 'Time of day when specific monitoring event ended';
COMMENT ON COLUMN ebms.m_visit.visit_temp IS 'Temperature in Celsius when specific monitoring event started';
COMMENT ON COLUMN ebms.m_visit.visit_cloud IS 'Percentage cloud cover (0-100) when specific monitoring event started';
COMMENT ON COLUMN ebms.m_visit.visit_wind IS 'Wind speed measured with the Beaufort wind scale (0-12) when specific monitoring event started';
COMMENT ON COLUMN ebms.m_visit.completed IS 'TRUE or FALSE if the monitoring walk has been completed in the visit';

-- M_Site Table

DROP TABLE IF EXISTS ebms.m_site CASCADE;

CREATE TABLE ebms.m_site
(
	site_id SERIAl NOT NULL,
	bms_id varchar(255),
	transect_id varchar(255),
	section_id varchar(255),
	monitoring_type varchar(255),
	site_info_quality integer,
	transect_length numeric,
	section_length numeric,
	section_transect_length_equal boolean,
	site_area numeric,
	bms_site_id integer,

	CONSTRAINT pk_m_site PRIMARY KEY (site_id),
	CONSTRAINT fk_m_site_bms_id FOREIGN KEY (bms_id) REFERENCES ebms.bms_detail (bms_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

ALTER TABLE ebms.m_site
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.m_site
  IS 'Identify and characterize a monitoring site and its components';

COMMENT ON COLUMN ebms.m_site.site_id IS 'Unique identifier for monitoring site in the eBMS database';
COMMENT ON COLUMN ebms.m_site.bms_id IS 'Name of the national bms (e.g. UKBMS)';
COMMENT ON COLUMN ebms.m_site.transect_id IS 'Unique identifier for a complete monitoring transect (e.g. UKBMS.368)';
COMMENT ON COLUMN ebms.m_site.section_id IS 'Identifier for a specific section along a transect';
COMMENT ON COLUMN ebms.m_site.monitoring_type IS 'Monitoring protocol used where 1=point, 2=area, 31=normal transect, 32=single species transect, 4=egg count plot, 5=time count';
COMMENT ON COLUMN ebms.m_site.site_info_quality IS 'Quality level of spatial data provided where 1 = section length and coordinates available, 2 = section length available but only transect centroid coordinates, 3 = no section length or coordinates available or not permanent section';
COMMENT ON COLUMN ebms.m_site.transect_length IS 'Length of the transect in meter';
COMMENT ON COLUMN ebms.m_site.section_length IS 'Length of the section in meter';
COMMENT ON COLUMN ebms.m_site.section_transect_length_equal IS 'Whether the total section length and is equal to the recorded transect +/- 10 metres. Boolean';
COMMENT ON COLUMN ebms.m_site.site_area IS 'Area of the site in square meter';
COMMENT ON COLUMN ebms.m_site.bms_site_id IS 'unique site ID (transect-section) within national BMS';

DROP TABLE IF EXISTS ebms.m_site_habitat CASCADE;

CREATE TABLE ebms.m_site_habitat
(
	site_id integer,
	year_stamp date,
	habitat_side1 integer,
	habitat_side2 integer,

  CONSTRAINT fk_m_site_habitat_site_id FOREIGN KEY (site_id) REFERENCES ebms.m_site (site_id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_m_site_habitat_habitat_side1 FOREIGN KEY (habitat_side1) REFERENCES ebms.habitat_thesaurus (habitat_id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_m_site_habitat_habitat_side2 FOREIGN KEY (habitat_side2) REFERENCES ebms.habitat_thesaurus (habitat_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

ALTER TABLE ebms. m_site_habitat
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.m_site_habitat
  IS 'Describe the habitat of monitoring site at a specific year';

COMMENT ON COLUMN ebms.m_site_habitat.site_id IS 'Site id in the eBMS database at the section level';
COMMENT ON COLUMN ebms.m_site_habitat.year_stamp IS 'The year site habitat classification correspond to';
COMMENT ON COLUMN ebms.m_site_habitat.habitat_side1 IS 'Habitat classification found on one side of the monitoring site';
COMMENT ON COLUMN ebms.m_site_habitat.habitat_side2 IS 'Habitat classification found on second side of the monitoring site (can be the same as the other)';

-- M_Site_Geo Table

DROP TABLE IF EXISTS ebms.m_site_geo CASCADE;
CREATE TABLE ebms.m_site_geo
(
	site_id integer,
	section_geom_true boolean,

  CONSTRAINT fk_m_site_geo_site_id FOREIGN KEY (site_id) REFERENCES ebms.m_site (site_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

SELECT AddGeometryColumn ('ebms','m_site_geo','centroid_geom',3035,'POINT',2);
SELECT AddGeometryColumn ('ebms','m_site_geo','start_geom',3035,'POINT',2);
SELECT AddGeometryColumn ('ebms','m_site_geo','end_geom',3035,'POINT',2);


ALTER TABLE ebms.m_site_geo
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.m_site_geo
  IS 'Spatial object of a monitoring site and its components using projection EPSG:3035 (ETRS89 / ETRS-LAEA)';

COMMENT ON COLUMN ebms.m_site_geo.site_id IS 'Unique identifier for monitoring site as in m_site table';
COMMENT ON COLUMN ebms.m_site_geo.section_geom_true IS 'TRUE if the geom is at section level, else FALSE';
COMMENT ON COLUMN ebms.m_site_geo.centroid_geom IS 'geometry of the centroid of a monitoring unit (e.g. points, section, area)';
COMMENT ON COLUMN ebms.m_site_geo.start_geom IS 'geometry of the starting point of a section';
COMMENT ON COLUMN ebms.m_site_geo.end_geom IS 'geometry of the ending point of a section';


-- B_Count

DROP TABLE IF EXISTS ebms.b_count CASCADE;

CREATE TABLE ebms.b_count
(
	count_id SERIAL NOT NULL,
	visit_id integer,
	site_id integer,
	species_id integer,
	butterfly_count integer NOT NULL,

  CONSTRAINT pk_b_count PRIMARY KEY (count_id),
  CONSTRAINT fk_b_count_visit_id FOREIGN KEY (visit_id) REFERENCES ebms.m_visit (visit_id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_b_count_site_id FOREIGN KEY (site_id) REFERENCES ebms.m_site (site_id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_b_count_species_id FOREIGN KEY (species_id) REFERENCES ebms.b_species_id (species_id) ON DELETE CASCADE ON UPDATE CASCADE
)
;

ALTER TABLE ebms.b_count
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.b_count
	IS 'Count of butterfly per visit';

COMMENT ON COLUMN ebms.b_count.count_id IS 'Unique identifier for a butterfly count';
COMMENT ON COLUMN ebms.b_count.visit_id IS 'Unique code for a visit from table m_visit';
COMMENT ON COLUMN ebms.b_count.site_id IS 'Unique code for a site from table m_site';
COMMENT ON COLUMN ebms.b_count.species_id IS 'Unique code for species from table b_species';
COMMENT ON COLUMN ebms.b_count.butterfly_count IS 'Number of butterfly observed in a specific section during a specific visit';

-- European checklist Wiemer et al. 2019 for species reference

DROP TABLE IF EXISTS ebms.species_checklist2019_gbif CASCADE;
CREATE TABLE ebms.species_checklist2019_gbif
(
    "Search_Genus" varchar(255),
    "Search_Species_Epithet" varchar(255),
    "Search_Name" varchar(255),
    "Accepted_Name" varchar(255),
    "Systematic_Order" integer,
    "Family" varchar(255),
    "Subfamily" varchar(255),
    "Genus" varchar(255),
    "Name" varchar(255),
    "Species_Epithet" varchar(255),
    "Author_Year" varchar(255),
    "Author" varchar(255),
    "Year" integer,
    "Full_Name" varchar(255),
    "Aggregate" boolean,
    "Synonym_Search_Name" boolean,
    "Synonym_Source" varchar(255),
    "Species_Search_Name" boolean,
    "GBIFusageKey" integer,
    "GBIFscientificName" varchar(255),
    "GBIFrank" varchar(255),
    "GBIForder" varchar(255),
    "GBIFmatchType" varchar(255),
    "GBIFphylum" varchar(255),
    "GBIFkingdom" varchar(255),
    "GBIFgenus" varchar(255),
    "GBIFclass" varchar(255),
    "GBIFconfidence" integer,
    "GBIFsynonym" boolean,
    "GBIFstatus" varchar(255),
    "GBIFfamily" varchar(255)
)
;

ALTER TABLE ebms.species_checklist2019_gbif
	OWNER TO ebms_user;

COMMENT ON TABLE ebms.species_checklist2019_gbif IS 'List of accepted species name in Wiemer et al 2019 checklist (https://zookeys.pensoft.net/article/28712/list/13/) published in Zookeys and since june 2020 available as open data on the Global Biodiversity Information Facility (GBIF): Maes et al. (2020, https://doi.org/10.15468/ye7whj). It includes the 496 species, as well as their presence and red list status in nearly all European countries. Systematic_Order is used as species_id in the eBMS database. In addition to the taxonomic change included in Wiemer et al 2019, we also used the synonyms from kudrna et al. (2011): distribution atlas of european butterflies and schweiger et al. (2014): climber as well as some additional one that where missing but in usage in national BMS';

COMMENT ON COLUMN ebms.species_checklist2019_gbif."Search_Name" IS 'Species name used in national BMS that we used to aligned with the taxonmy in Wiemer et al. 2019';
COMMENT ON COLUMN ebms.species_checklist2019_gbif."Systematic_Order" IS 'Unique code for recorded species as for the accepted name, with 1000 and above for aggregates';
COMMENT ON COLUMN ebms.species_checklist2019_gbif."Accepted_Name" IS 'Accepted scientific name based on Wiemer et al. 2019 checklist (https://zookeys.pensoft.net/article/28712/list/13/)';
COMMENT ON COLUMN ebms.species_checklist2019_gbif."Full_Name" IS 'Accepted scientific name and authority based on Wiemer et al. 2019 checklist';
COMMENT ON COLUMN ebms.species_checklist2019_gbif."Aggregate" IS '1 if this is an aggregate of species with species_id larger 1000';

\copy  ebms.species_checklist2019_gbif FROM 'data/taxonomic_resolution_checklist2019_gbif.csv' delimiter as ',' csv quote as '"' HEADER;


-- Populate table species_id

INSERT INTO ebms.b_species_id
SELECT DISTINCT
"Systematic_Order" as species_id,
"Accepted_Name" as species_acpt_sci_name,
"Aggregate" as aggregate
FROM
ebms.species_checklist2019_gbif
ORDER BY
species_id;


-- ADD INDEX for Foreign keys
CREATE INDEX CONCURRENTLY species_id_species_thesaurus ON ebms.species_thesaurus (species_id);
CREATE INDEX CONCURRENTLY species_id_b_species ON ebms.b_species_id (species_id);
CREATE INDEX CONCURRENTLY species_acpt_sci_name_b_species ON ebms.b_species_id (species_acpt_sci_name);

CREATE INDEX CONCURRENTLY bms_id_b_recorder ON ebms.b_recorder (bms_id);

CREATE INDEX CONCURRENTLY recorder_id_m_visit ON ebms.m_visit (recorder_id);
CREATE INDEX CONCURRENTLY bms_id_m_visit ON ebms.m_visit (bms_id);
CREATE INDEX CONCURRENTLY bms_visit_id_m_site ON ebms.m_visit (bms_visit_id);

CREATE INDEX CONCURRENTLY bms_id_m_site ON ebms.m_site (bms_id);
CREATE INDEX CONCURRENTLY bms_site_id_m_site ON ebms.m_site (bms_site_id);

CREATE INDEX CONCURRENTLY site_id_m_site_habitat ON ebms.m_site_habitat (site_id);
CREATE INDEX CONCURRENTLY habitat_side1_m_site_habitat ON ebms.m_site_habitat (habitat_side1);
CREATE INDEX CONCURRENTLY habitat_side2_m_site_habitat ON ebms.m_site_habitat (habitat_side2);

CREATE INDEX CONCURRENTLY site_id_m_site_geo ON ebms.m_site_geo (site_id);

VACUUM analyze;

\q
