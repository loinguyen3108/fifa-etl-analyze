CREATE TABLE IF NOT EXISTS fifa.dim_club (
    club_id INT,
    name STRING,
    league_name String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
