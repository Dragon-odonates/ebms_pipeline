/*
Create embs database
Author: Reto Schmucki - retoschm@ceh.ac.uk
*/

psql -h localhost -U "ebms_user" -p 5432 -d ebms_v5_0

DROP DATABASE IF EXISTS ebms_v6_0;
--
VACUUM;
CREATE DATABASE ebms_v6_1
  WITH ENCODING = 'UTF8'
       OWNER = ebms_user
       TEMPLATE = ebms_template
       CONNECTION LIMIT = -1;

\connect ebms_v6_1

VACUUM;

\q
