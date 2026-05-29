CREATE TABLE IF NOT EXISTS airline_success_stat (
    stat_hour VARCHAR(13) NOT NULL COMMENT 'Hour bucket, for example 2018-08-30 19',
    airline_code VARCHAR(10) NOT NULL COMMENT 'Airline code, for example CA',
    success_count BIGINT NOT NULL DEFAULT 0 COMMENT 'Accumulated booking success count',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stat_hour, airline_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
