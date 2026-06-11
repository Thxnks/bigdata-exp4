CREATE DATABASE IF NOT EXISTS bigdata_exp4
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE bigdata_exp4;

CREATE TABLE IF NOT EXISTS stat_result (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    stat_hour VARCHAR(50)  NOT NULL COMMENT '统计时间段，如 2024-05-01 10',
    airline_code VARCHAR(20) NOT NULL COMMENT '航空公司代码，如 CA、MU、CZ',
    success_count INT NOT NULL DEFAULT 0 COMMENT '预订成功数量',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每小时每航空公司预订成功统计结果';

-- 为 Spark 写入创建远程用户（密码请修改）
-- CREATE USER 'spark'@'%' IDENTIFIED BY 'password123';
-- GRANT ALL PRIVILEGES ON bigdata_exp4.* TO 'spark'@'%';
-- FLUSH PRIVILEGES;
