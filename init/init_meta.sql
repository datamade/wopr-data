DROP TABLE IF EXISTS DAT_Meta;

CREATE TABLE DAT_Meta(
      dataset_name VARCHAR(50),
      human_name VARCHAR(75),
      description TEXT,
      obs_from DATE,
      obs_to DATE,
      bbox GEOMETRY(POLYGON,4326),
      source_url VARCHAR(100),
      update_freq VARCHAR(50),
      PRIMARY KEY(dataset_name),
);


