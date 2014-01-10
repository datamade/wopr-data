\timing
DROP TABLE IF EXISTS DUP_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS DUP_chicago_crimes_all(
dup_chicago_crimes_all_row_id SERIAL,
ID INTEGER,
Case_Number VARCHAR(10),
Orig_Date TIMESTAMP,
Block VARCHAR(50),
IUCR VARCHAR(10),
Primary_Type VARCHAR(100),
Description VARCHAR(100),
Location_Description VARCHAR(50),
Arrest BOOLEAN,
Domestic BOOLEAN,
Beat VARCHAR(10),
District VARCHAR(5),
Ward INTEGER,
Community_Area VARCHAR(10),
FBI_Code VARCHAR(10),
X_Coordinate INTEGER,
Y_Coordinate INTEGER,
Year INTEGER,
Updated_On TIMESTAMP,
Latitude FLOAT8,
Longitude FLOAT8,
Location POINT,
PRIMARY KEY(dup_chicago_crimes_all_row_id));

COPY DUP_chicago_crimes_all
(ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
)
FROM '/project/evtimov/wopr/data/chicago-crimes-all_2013-10-31.csv'
WITH DELIMITER ','
CSV HEADER;

--
-- remove duplicates based on ID
--
DROP TABLE IF EXISTS DEDUP_chicago_crimes_all;

CREATE TABLE DEDUP_chicago_crimes_all(
dup_chicago_crimes_all_row_id INTEGER PRIMARY KEY
);

INSERT INTO DEDUP_chicago_crimes_all
SELECT MAX(dup_chicago_crimes_all_row_id)
FROM DUP_chicago_crimes_all
GROUP BY ID;

--
-- create a SRC table for chicago_crimes_all and
--   load the initial source file
--
DROP TABLE IF EXISTS SRC_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS SRC_chicago_crimes_all(
ID INTEGER,
Case_Number VARCHAR(10),
Orig_Date TIMESTAMP,
Block VARCHAR(50),
IUCR VARCHAR(10),
Primary_Type VARCHAR(100),
Description VARCHAR(100),
Location_Description VARCHAR(50),
Arrest BOOLEAN,
Domestic BOOLEAN,
Beat VARCHAR(10),
District VARCHAR(5),
Ward INTEGER,
Community_Area VARCHAR(10),
FBI_Code VARCHAR(10),
X_Coordinate INTEGER,
Y_Coordinate INTEGER,
Year INTEGER,
Updated_On TIMESTAMP,
Latitude FLOAT8,
Longitude FLOAT8,
Location POINT,
PRIMARY KEY(ID));

INSERT INTO SRC_chicago_crimes_all
SELECT 
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM DUP_chicago_crimes_all
JOIN DEDUP_chicago_crimes_all USING (dup_chicago_crimes_all_row_id);

DROP TABLE IF EXISTS NEW_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS NEW_chicago_crimes_all(
  ID INTEGER,
  PRIMARY KEY(ID)
);

--
-- find new records in source data
--
INSERT INTO NEW_chicago_crimes_all
SELECT ID
FROM SRC_chicago_crimes_all
LEFT OUTER JOIN DAT_chicago_crimes_all USING (ID)
WHERE chicago_crimes_all_row_id IS NULL;


-- insert new records in database
INSERT INTO DAT_chicago_crimes_all(
start_date,
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location)
SELECT 
'2013-10-31',
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM SRC_chicago_crimes_all
JOIN NEW_chicago_crimes_all USING (ID);

-- 
-- insert new records into DAT_master
--
INSERT INTO DAT_Master(
  start_date,
  end_date,
  current_flag,
  Location,
  LATITUDE, 
  LONGITUDE,
  obs_date,
  obs_ts,
  dataset_name,
  dataset_row_id)
SELECT
  start_date,
  end_date,
  current_flag,
  Location,
  Latitude, 
  Longitude,
  Orig_date AS obs_date,
  NULL AS obs_ts,
  'chicago_crimes_all' AS dataset_name,
  chicago_crimes_all_row_id AS dataset_row_id
FROM
  DAT_chicago_crimes_all
JOIN
  NEW_chicago_crimes_all USING (ID);


--
-- find all changes to existing records
--
DROP TABLE IF EXISTS CHG_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS CHG_chicago_crimes_all(
  ID INTEGER,
  PRIMARY KEY(ID)
);

INSERT INTO CHG_chicago_crimes_all
SELECT ID
FROM SRC_chicago_crimes_all S
JOIN DAT_chicago_crimes_all D
USING (ID)
WHERE D.current_flag = true
AND ((S.ID IS NOT NULL OR D.ID IS NOT NULL) AND S.ID <> D.ID)
OR ((S.Case_Number IS NOT NULL OR D.Case_Number IS NOT NULL) AND S.Case_Number <> D.Case_Number)
OR ((S.Orig_Date IS NOT NULL OR D.Orig_Date IS NOT NULL) AND S.Orig_Date <> D.Orig_Date)
OR ((S.Block IS NOT NULL OR D.Block IS NOT NULL) AND S.Block <> D.Block)
OR ((S.IUCR IS NOT NULL OR D.IUCR IS NOT NULL) AND S.IUCR <> D.IUCR)
OR ((S.Primary_Type IS NOT NULL OR D.Primary_Type IS NOT NULL) AND S.Primary_Type <> D.Primary_Type)
OR ((S.Description IS NOT NULL OR D.Description IS NOT NULL) AND S.Description <> D.Description)
OR ((S.Location_Description IS NOT NULL OR D.Location_Description IS NOT NULL) AND S.Location_Description <> D.Location_Description)
OR ((S.Arrest IS NOT NULL OR D.Arrest IS NOT NULL) AND S.Arrest <> D.Arrest)
OR ((S.Domestic IS NOT NULL OR D.Domestic IS NOT NULL) AND S.Domestic <> D.Domestic)
OR ((S.Beat IS NOT NULL OR D.Beat IS NOT NULL) AND S.Beat <> D.Beat)
OR ((S.District IS NOT NULL OR D.District IS NOT NULL) AND S.District <> D.District)
OR ((S.Ward IS NOT NULL OR D.Ward IS NOT NULL) AND S.Ward <> D.Ward)
OR ((S.Community_Area IS NOT NULL OR D.Community_Area IS NOT NULL) AND S.Community_Area <> D.Community_Area)
OR ((S.FBI_Code IS NOT NULL OR D.FBI_Code IS NOT NULL) AND S.FBI_Code <> D.FBI_Code)
OR ((S.X_Coordinate IS NOT NULL OR D.X_Coordinate IS NOT NULL) AND S.X_Coordinate <> D.X_Coordinate)
OR ((S.Y_Coordinate IS NOT NULL OR D.Y_Coordinate IS NOT NULL) AND S.Y_Coordinate <> D.Y_Coordinate)
OR ((S.Year IS NOT NULL OR D.Year IS NOT NULL) AND S.Year <> D.Year)
OR ((S.Updated_On IS NOT NULL OR D.Updated_On IS NOT NULL) AND S.Updated_On <> D.Updated_On)
OR ((S.Latitude IS NOT NULL OR D.Latitude IS NOT NULL) AND S.Latitude <> D.Latitude)
OR ((S.Longitude IS NOT NULL OR D.Longitude IS NOT NULL) AND S.Longitude <> D.Longitude)
OR ((S.Location IS NOT NULL OR D.Location IS NOT NULL) AND S.Location <> D.Location);

--
-- update all existing records that are being changed with end_date and current_flag
--
UPDATE DAT_chicago_crimes_all D
SET end_date = '2013-10-30',
current_flag = FALSE
FROM CHG_chicago_crimes_all C
WHERE D.ID = C.ID
AND D.current_flag = TRUE;

-- update all existing records in DAT_master that are being changed with end_date and current_flag
UPDATE DAT_Master M
SET 
  current_flag = FALSE,
  end_date = '2013-10-30'
FROM
  DAT_chicago_crimes_all D
WHERE 
  M.dataset_row_id = D.chicago_crimes_all_row_id 
  AND D.current_flag = FALSE
  AND D.end_date = '2013-10-30';

--
-- insert the change records with start_date set to today
--
INSERT INTO DAT_chicago_crimes_all(
start_date,
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location)
SELECT
'2013-10-31',
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM SRC_chicago_crimes_all
JOIN CHG_chicago_crimes_all USING (ID);

DROP TABLE IF EXISTS DUP_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS DUP_chicago_crimes_all(
dup_chicago_crimes_all_row_id SERIAL,
ID INTEGER,
Case_Number VARCHAR(10),
Orig_Date TIMESTAMP,
Block VARCHAR(50),
IUCR VARCHAR(10),
Primary_Type VARCHAR(100),
Description VARCHAR(100),
Location_Description VARCHAR(50),
Arrest BOOLEAN,
Domestic BOOLEAN,
Beat VARCHAR(10),
District VARCHAR(5),
Ward INTEGER,
Community_Area VARCHAR(10),
FBI_Code VARCHAR(10),
X_Coordinate INTEGER,
Y_Coordinate INTEGER,
Year INTEGER,
Updated_On TIMESTAMP,
Latitude FLOAT8,
Longitude FLOAT8,
Location POINT,
PRIMARY KEY(dup_chicago_crimes_all_row_id));

COPY DUP_chicago_crimes_all
(ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
)
FROM '/project/evtimov/wopr/data/chicago-crimes-all_2013-11-01.csv'
WITH DELIMITER ','
CSV HEADER;

--
-- remove duplicates based on ID
--
DROP TABLE IF EXISTS DEDUP_chicago_crimes_all;

CREATE TABLE DEDUP_chicago_crimes_all(
dup_chicago_crimes_all_row_id INTEGER PRIMARY KEY
);

INSERT INTO DEDUP_chicago_crimes_all
SELECT MAX(dup_chicago_crimes_all_row_id)
FROM DUP_chicago_crimes_all
GROUP BY ID;

--
-- create a SRC table for chicago_crimes_all and
--   load the initial source file
--
DROP TABLE IF EXISTS SRC_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS SRC_chicago_crimes_all(
ID INTEGER,
Case_Number VARCHAR(10),
Orig_Date TIMESTAMP,
Block VARCHAR(50),
IUCR VARCHAR(10),
Primary_Type VARCHAR(100),
Description VARCHAR(100),
Location_Description VARCHAR(50),
Arrest BOOLEAN,
Domestic BOOLEAN,
Beat VARCHAR(10),
District VARCHAR(5),
Ward INTEGER,
Community_Area VARCHAR(10),
FBI_Code VARCHAR(10),
X_Coordinate INTEGER,
Y_Coordinate INTEGER,
Year INTEGER,
Updated_On TIMESTAMP,
Latitude FLOAT8,
Longitude FLOAT8,
Location POINT,
PRIMARY KEY(ID));

INSERT INTO SRC_chicago_crimes_all
SELECT 
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM DUP_chicago_crimes_all
JOIN DEDUP_chicago_crimes_all USING (dup_chicago_crimes_all_row_id);

DROP TABLE IF EXISTS NEW_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS NEW_chicago_crimes_all(
  ID INTEGER,
  PRIMARY KEY(ID)
);

--
-- find new records in source data
--
INSERT INTO NEW_chicago_crimes_all
SELECT ID
FROM SRC_chicago_crimes_all
LEFT OUTER JOIN DAT_chicago_crimes_all USING (ID)
WHERE chicago_crimes_all_row_id IS NULL;


-- insert new records in database
INSERT INTO DAT_chicago_crimes_all(
start_date,
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location)
SELECT 
'2013-11-01',
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM SRC_chicago_crimes_all
JOIN NEW_chicago_crimes_all USING (ID);

-- 
-- insert new records into DAT_master
--
INSERT INTO DAT_Master(
  start_date,
  end_date,
  current_flag,
  Location,
  LATITUDE, 
  LONGITUDE,
  obs_date,
  obs_ts,
  dataset_name,
  dataset_row_id)
SELECT
  start_date,
  end_date,
  current_flag,
  Location,
  Latitude, 
  Longitude,
  Orig_date AS obs_date,
  NULL AS obs_ts,
  'chicago_crimes_all' AS dataset_name,
  chicago_crimes_all_row_id AS dataset_row_id
FROM
  DAT_chicago_crimes_all
JOIN
  NEW_chicago_crimes_all USING (ID);


--
-- find all changes to existing records
--
DROP TABLE IF EXISTS CHG_chicago_crimes_all;

CREATE TABLE IF NOT EXISTS CHG_chicago_crimes_all(
  ID INTEGER,
  PRIMARY KEY(ID)
);

INSERT INTO CHG_chicago_crimes_all
SELECT ID
FROM SRC_chicago_crimes_all S
JOIN DAT_chicago_crimes_all D
USING (ID)
WHERE D.current_flag = true
AND ((S.ID IS NOT NULL OR D.ID IS NOT NULL) AND S.ID <> D.ID)
OR ((S.Case_Number IS NOT NULL OR D.Case_Number IS NOT NULL) AND S.Case_Number <> D.Case_Number)
OR ((S.Orig_Date IS NOT NULL OR D.Orig_Date IS NOT NULL) AND S.Orig_Date <> D.Orig_Date)
OR ((S.Block IS NOT NULL OR D.Block IS NOT NULL) AND S.Block <> D.Block)
OR ((S.IUCR IS NOT NULL OR D.IUCR IS NOT NULL) AND S.IUCR <> D.IUCR)
OR ((S.Primary_Type IS NOT NULL OR D.Primary_Type IS NOT NULL) AND S.Primary_Type <> D.Primary_Type)
OR ((S.Description IS NOT NULL OR D.Description IS NOT NULL) AND S.Description <> D.Description)
OR ((S.Location_Description IS NOT NULL OR D.Location_Description IS NOT NULL) AND S.Location_Description <> D.Location_Description)
OR ((S.Arrest IS NOT NULL OR D.Arrest IS NOT NULL) AND S.Arrest <> D.Arrest)
OR ((S.Domestic IS NOT NULL OR D.Domestic IS NOT NULL) AND S.Domestic <> D.Domestic)
OR ((S.Beat IS NOT NULL OR D.Beat IS NOT NULL) AND S.Beat <> D.Beat)
OR ((S.District IS NOT NULL OR D.District IS NOT NULL) AND S.District <> D.District)
OR ((S.Ward IS NOT NULL OR D.Ward IS NOT NULL) AND S.Ward <> D.Ward)
OR ((S.Community_Area IS NOT NULL OR D.Community_Area IS NOT NULL) AND S.Community_Area <> D.Community_Area)
OR ((S.FBI_Code IS NOT NULL OR D.FBI_Code IS NOT NULL) AND S.FBI_Code <> D.FBI_Code)
OR ((S.X_Coordinate IS NOT NULL OR D.X_Coordinate IS NOT NULL) AND S.X_Coordinate <> D.X_Coordinate)
OR ((S.Y_Coordinate IS NOT NULL OR D.Y_Coordinate IS NOT NULL) AND S.Y_Coordinate <> D.Y_Coordinate)
OR ((S.Year IS NOT NULL OR D.Year IS NOT NULL) AND S.Year <> D.Year)
OR ((S.Updated_On IS NOT NULL OR D.Updated_On IS NOT NULL) AND S.Updated_On <> D.Updated_On)
OR ((S.Latitude IS NOT NULL OR D.Latitude IS NOT NULL) AND S.Latitude <> D.Latitude)
OR ((S.Longitude IS NOT NULL OR D.Longitude IS NOT NULL) AND S.Longitude <> D.Longitude)
OR ((S.Location IS NOT NULL OR D.Location IS NOT NULL) AND S.Location <> D.Location);

--
-- update all existing records that are being changed with end_date and current_flag
--
UPDATE DAT_chicago_crimes_all D
SET end_date = '2013-10-31',
current_flag = FALSE
FROM CHG_chicago_crimes_all C
WHERE D.ID = C.ID
AND D.current_flag = TRUE;

-- update all existing records in DAT_master that are being changed with end_date and current_flag
UPDATE DAT_Master M
SET 
  current_flag = FALSE,
  end_date = '2013-10-31'
FROM
  DAT_chicago_crimes_all D
WHERE 
  M.dataset_row_id = D.chicago_crimes_all_row_id 
  AND D.current_flag = FALSE
  AND D.end_date = '2013-10-31';

--
-- insert the change records with start_date set to today
--
INSERT INTO DAT_chicago_crimes_all(
start_date,
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location)
SELECT
'2013-11-01',
ID,
Case_Number,
Orig_Date,
Block,
IUCR,
Primary_Type,
Description,
Location_Description,
Arrest,
Domestic,
Beat,
District,
Ward,
Community_Area,
FBI_Code,
X_Coordinate,
Y_Coordinate,
Year,
Updated_On,
Latitude,
Longitude,
Location
FROM SRC_chicago_crimes_all
JOIN CHG_chicago_crimes_all USING (ID);
