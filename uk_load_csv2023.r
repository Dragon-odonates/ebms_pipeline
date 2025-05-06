## update ebms v4.0 [21.10.2023]
## R --vanilla

library('RPostgreSQL')
library("data.table")
library("stringr")
library("lubridate")
library("dplyr")
library("sf")

# =================

if(!exists("db_name")) db_name <- "ebms_v6_0"

data_path <- file.path(getwd(), "data", "update2023", "uk")

## build site section table
uk_count <- fread(file.path(data_path, 'count_table_2023.csv'))
uk_species <- fread(file.path(data_path, 'species_table_2023.csv'))
uk_section <- fread(file = file.path(data_path, 'section_table_2023.csv'))
uk_site <- fread(file.path(data_path, "site_table_2023.csv"))
uk_visit <- fread(file.path(data_path, "visit_table_2023.csv"))

# ... CLEAN, HARMONISE AND ARRANGE RAW DATA 

# Assign species to the SPECIES ID used in the database 
checklist2019_gbif <- fread(file.path("data/taxonomic_resolution_checklist2019_gbif.csv"))[!duplicated(Search_Name), ]
uk_count <- merge(uk_count, checklist2019_gbif[, .(Search_Name, Accepted_Name, Systematic_Order)], by.x = "sci_name", by.y = "Search_Name", all.x = TRUE)


uk_visit$start_time =  ifelse(uk_visit$start_time == "00:00"| uk_visit$start_time == "0"| uk_visit$start_time == "00:00:00", "00:00:00", format(parse_date_time(gsub("S", "", gsub("H |M ", ":", hm(uk_visit$start_time))), "H:M:S"), format = "%H:%M:%S"))
uk_visit$end_time =  ifelse(uk_visit$end_time == "00:00"| uk_visit$end_time == "0"| uk_visit$end_time == "00:00:00", "00:00:00", format(parse_date_time(gsub("S", "", gsub("H |M ", ":", hm(uk_visit$end_time))), "H:M:S"), format = "%H:%M:%S"))
uk_visit[is.na(recorder_code), recorder_code := 9999]


## Load to eBMS database in its own schema (not ebms)
bms_table_name = "uk_bms" # schema
count_name = "uk_count"
visit_name = 'uk_visit'
site_name = 'uk_site'
bms_val = "UKBMS"

dbcon <- dbConnect(dbDriver("PostgreSQL"), dbname = db_name, host = "localhost", port = db_port, user = db_user, password = db_psw)

dbSendStatement(dbcon, paste0("DROP SCHEMA IF EXISTS ", bms_table_name , " CASCADE;"))
dbSendStatement(dbcon, paste0("CREATE SCHEMA IF NOT EXISTS ", bms_table_name , " AUTHORIZATION ebms_user;"))

dbWriteTable(dbcon, c(bms_table_name, count_name), as.data.frame(uk_count), overwrite = TRUE)
dbWriteTable(dbcon, c(bms_table_name, visit_name), as.data.frame(uk_visit), overwrite = TRUE)
dbWriteTable(dbcon, c(bms_table_name, site_name), as.data.frame(uk_site), overwrite = TRUE)

dbDisconnect(dbcon)

# Use this function to create and run the SQL code that will insert the new data into the table in the ebms schema
Create_tables_SQL(
  db_name = db_name,
  db_port = db_port,
  db_user = db_user,
  db_psw = db_psw,
  bms_val = bms_val,
  bms_table_name = bms_table_name, 
  count_name = count_name, 
  visit_name = visit_name,
  site_name = site_name,
  country_iso3 = "GBR", 
  country_name = 'United Kingdom of Great Britain and Northern Ireland', 
  contact_name = "CONTACT NAME", 
  contact_email = "contact@email")


# this can be followed looking at what was pushed to the database and how many lines have been added to ebms schema
