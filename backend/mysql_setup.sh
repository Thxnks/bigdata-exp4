#!/bin/bash
# Role C: initialize MySQL on node1 for bigdata-exp4
# - start mysqld, set root password to 'root'
# - create database bigdata_exp4 + table stat_result
# - insert demo rows so the API has data before processor is ready
# - allow remote access (for Spark writes / testing) + open firewall
set -u

echo "===== 1. 启动 mysqld ====="
sudo systemctl enable --now mysqld
sleep 6
echo -n "mysqld 状态: "; sudo systemctl is-active mysqld

echo "===== 2. 读取临时 root 密码 ====="
TEMP=$(sudo grep 'temporary password' /var/log/mysqld.log | tail -1 | sed 's/.*localhost: //')
echo "临时密码 = [$TEMP]"

echo "===== 3. 修改过期临时密码 -> 临时强密码 ====="
mysql --connect-expired-password -uroot -p"$TEMP" \
  -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'Root@Temp123';" \
  && echo "  临时强密码已设置" || { echo "  FAILED at step 3"; exit 1; }

echo "===== 4. 卸载密码强度组件(允许弱密码 root) ====="
mysql -uroot -p'Root@Temp123' --force \
  -e "UNINSTALL COMPONENT 'file://component_validate_password';" 2>/dev/null \
  && echo "  validate_password 已卸载" || echo "  (组件可能未安装, 忽略)"

echo "===== 5. 设置 root 密码为 root + 开放远程 ====="
mysql -uroot -p'Root@Temp123' <<'SQL'
ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'root';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SQL
echo "  root 密码 = root (localhost 和 % 都可登录)"

echo "===== 6. 建库 + 建表 + 示例数据 ====="
mysql -uroot -p'root' <<'SQL'
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

-- 示例数据(占位, processor 完成后可清空: TRUNCATE TABLE stat_result;)
INSERT INTO stat_result (stat_hour, airline_code, success_count) VALUES
 ('2018-08-30 19', 'CA', 128),
 ('2018-08-30 19', 'CX', 96),
 ('2018-08-30 19', 'AE', 47),
 ('2018-08-30 19', 'MU', 73),
 ('2018-08-30 19', 'CZ', 55),
 ('2018-08-30 20', 'CA', 142),
 ('2018-08-30 20', 'CX', 88),
 ('2018-08-30 20', 'AE', 39),
 ('2018-08-30 20', 'MU', 81),
 ('2018-08-30 20', 'CZ', 64),
 ('2018-08-30 21', 'CA', 110),
 ('2018-08-30 21', 'MU', 69);
SQL
echo "  建库建表 + 示例数据完成"

echo "===== 7. 验证数据 ====="
mysql -uroot -p'root' -e "SELECT stat_hour, airline_code, success_count FROM bigdata_exp4.stat_result ORDER BY stat_hour, airline_code;"

echo "===== 8. 防火墙放行 3306 / 8080 ====="
if sudo systemctl is-active firewalld >/dev/null 2>&1; then
  sudo firewall-cmd --permanent --add-port=3306/tcp >/dev/null
  sudo firewall-cmd --permanent --add-port=8080/tcp >/dev/null
  sudo firewall-cmd --reload >/dev/null
  echo "  firewalld 已放行 3306/8080"
else
  echo "  firewalld 未运行, 无需放行"
fi

echo "===== MySQL 初始化全部完成 ====="
