# 角色 C 部署手册（MySQL + 后端接口，跑在 node1）

> 数据流里 C 的位置：processor 把统计结果写入 **MySQL `stat_result`** → 本**后端**查询该表 → 暴露 `GET /api/stat/list` 给前端。

## 集群中 C 的环境（已在 node1 装好）

| 组件 | 位置 / 版本 | 说明 |
|------|------------|------|
| MySQL | node1 8.0.46，端口 3306 | root/root，库 `bigdata_exp4`，表 `stat_result` |
| JDK 17 | `/export/server/jdk17` | 专给 Spring Boot 3 用，不动 Hadoop 的 JDK 8 |
| 后端 jar | `/home/hadoop/backend.jar` | Spring Boot，端口 8080 |
| API | `http://node1:8080/api/stat/list` | 返回统计结果 JSON |

CentOS 7 已 EOL，yum 源已改指阿里云 vault 镜像（`/etc/yum.repos.d/CentOS-Base.repo`，旧文件备份在 `backup/`）。

---

## 一、开机后启动顺序（答辩当天照这个来）

```bash
# 1. MySQL（已设开机自启，一般会自动起；没起就手动）
sudo systemctl status mysqld        # 看是否 active
sudo systemctl start mysqld         # 没起就启动

# 2. 启动后端（用 JDK17，后台运行，日志写到 backend.log）
cd /home/hadoop
nohup /export/server/jdk17/bin/java -jar backend.jar > backend.log 2>&1 &

# 3. 等约 6 秒，确认启动成功
grep "Started BackendApplication" backend.log
```

停止后端：
```bash
# 找到进程并杀掉
jps | grep backend     # 或: ps -ef | grep backend.jar
kill <pid>
```

---

## 二、四个答辩截图（角色 C 需要提交，放 screenshots/backend/）

**截图① 建库建表成功**
```bash
mysql -uroot -proot -e "USE bigdata_exp4; SHOW TABLES; DESC stat_result;"
```

**截图② 表里的数据**
```bash
mysql -uroot -proot -e "SELECT * FROM bigdata_exp4.stat_result;"
```

**截图③ 后端启动日志**（显示连上 MySQL + Tomcat 8080）
```bash
grep -E "HikariPool|Tomcat started|Started BackendApplication" /home/hadoop/backend.log
```

**截图④ 接口返回 JSON**
- 在 node1：`curl http://localhost:8080/api/stat/list`
- 或在 Windows 浏览器打开：`http://192.168.88.101:8080/api/stat/list`

---

## 三、答辩可能被问的点（提前准备好答案）

- **表结构为什么这么设计？** `stat_hour`(时间段) + `airline_code`(航司) + `success_count`(成功数)，正好对应"每小时每个航空公司预订成功数量"的统计目标；`id` 自增主键，`created_at` 记录写入时间。
- **后端怎么连数据库的？** Spring Boot + Spring Data JPA，`application.properties` 里配 `jdbc:mysql://localhost:3306/bigdata_exp4`，用 HikariCP 连接池；`ddl-auto=validate` 表示启动时**校验**实体和表结构一致（不自动改表）。
- **`/api/stat/list` 怎么实现的？** `StatController` 调 `StatResultRepository.findAll()`（继承 JpaRepository），把 `stat_result` 全表查出来，自动序列化成 JSON 数组返回。
- **跨域怎么处理？** `CorsConfig` 放行跨域，方便前端（角色 D）从浏览器直接 fetch。
- **数据从哪来？** 由角色 B 的 Spark/MapReduce 消费 Kafka 后写入 `stat_result`。当前表里是**示例占位数据**，B 的程序跑通后执行 `TRUNCATE TABLE stat_result;` 清空，换成真实统计结果。

---

## 四、和组员的接口约定（联调时给 B 和 D）

**给角色 B（往 MySQL 写数据）：**
- 连接：`jdbc:mysql://192.168.88.101:3306/bigdata_exp4`，用户 `root` 密码 `root`（已开放远程 `root@'%'`）
- 写入表 `stat_result`，字段 `stat_hour, airline_code, success_count`
- Spark JDBC 写入记得加参数：`useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai`

**给角色 D（前端）：**
- 直接 fetch `http://192.168.88.101:8080/api/stat/list`，返回 `[{statHour, airlineCode, successCount}, ...]`

---

## 五、本次部署做过的事（环境记录）

1. 修复 CentOS7 EOL 的 yum 源 → 阿里云 vault
2. 装 MySQL 8.0：`mysql_setup.sh`（设密码、建库表、示例数据、放行）
3. 装 Temurin JDK 17 到 `/export/server/jdk17`
4. Windows 上 `mvn package` 打出 `backend-1.0.0.jar` → 传到 node1 跑
