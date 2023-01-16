CREATE TABLE IF NOT EXISTS fifa.dim_nationality (
    nationality_id INT,
    name STRING
)
PARTITIONED BY (last_update DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
