CREATE TABLE IF NOT EXISTS fifa.dim_date (
    date_id INT,
    year INT,
    month INT,
    day INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
