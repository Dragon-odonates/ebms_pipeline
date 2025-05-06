R --vanilla

# Load theall packages needed by your script
library('RPostgreSQL')
library('data.table')
library('openxlsx')
library('stringr')
library('sf')
library("dplyr")
library("lubridate")
library("glue")
library("logr")

# Load Create_table_SQL.r for the SQL upload function.
devtools::load_all("Create_table_SQL.r") 

## credentials are in database_credential.csv 
## IMPORTANT: use .gitignore to not track the credentials
db_name <- "ebms_v6_1"
db_port <- as.integer(fread("database_credential.csv")[1, "db_port"])
db_user <-  as.character(fread("database_credential.csv")[1, "db_user"])
db_psw <- as.character(fread("database_credential.csv")[1, "db_psw"]) 

# Set the PGPASSWORD environment variable
Sys.setenv(PGPASSWORD = db_psw)

db_details <- paste0('-h localhost -U ', db_user, ' -d ', db_name,' -p ', db_port)

# initialise the ebms schema in the database
system2('psql', args = paste0(db_details, ' -f \"create_ebms_schema.sql\"'))

# Initialise log file
data_upload_log <- file.path(getwd(), "data", "update2023","data_upload_log", 
                            paste0(db_name, gsub(" ", "_", format(Sys.time(), "%a %b %d %X %Y")), 
                            "_data_upload_log.log"))
lf <- log_open(data_upload_log)

## load BMS data
source('uk_load_csv2023.r') # load UK dataset

# ... repeat for all datasets ...

log_close(footer = TRUE)
q()

## ===================
## QUIT R AND USE SQL
## ===================

db_user='ebms_user'
db_name='ebms_v6_1'
db_port='5432'

psql -h localhost -U $db_user -d $db_name -p $db_port

-- send command to postgresql to clean and index the newly created database

VACUUM FULL;

CREATE INDEX visit_id_b_count ON ebms.b_count (visit_id);
CREATE INDEX site_id_b_count ON ebms.b_count (site_id);
CREATE INDEX species_id_b_count ON ebms.b_count (species_id);

VACUUM ANALYZE;

\q

## from your terminal

db_user='ebms_user'
db_name='ebms_v6_1'
db_port='5432'
format='.sql'

pg_dump -h localhost -U $db_user -p $db_port -n 'ebms' $db_name > ../$db_name$format