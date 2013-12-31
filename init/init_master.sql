DROP TABLE IF EXISTS DAT_Master;

CREATE TABLE DAT_Master(
       master_row_id SERIAL,
       start_date DATE,
       end_date DATE DEFAULT NULL,
       current_flag BOOLEAN DEFAULT true,
       Location POINT,
       LATITUDE FLOAT8,
       LONGITUDE FLOAT8,
       obs_date DATE,
       obs_ts TIMESTAMP,
       geoTag1 VARCHAR(50),
       geoTag2 VARCHAR(50),
       geoTag3 VARCHAR(50),
       dataset_name VARCHAR(50),
       dataset_row_id INT,
       PRIMARY KEY(master_row_id) 
);


