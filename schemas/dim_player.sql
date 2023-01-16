CREATE TABLE IF NOT EXISTS fifa.dim_player (
    player_id BIGINT,
    short_name VARCHAR(60),
    long_name VARCHAR(60),
    dob DATE,
    gender BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
