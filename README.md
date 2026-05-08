# BigData Experiment 4

## 项目说明

本项目为《实验四 综合实验》中的 **大数据采集分析与可视化系统实现**。

项目目标是基于航空预订日志数据，完成一个简单的大数据处理流程：

```text
原始日志数据 → Kafka 数据采集 → Spark/MapReduce 数据处理 → HDFS/MySQL 数据存储 → 前端页面展示
```

主要统计目标：

```text
统计每个时间段内，每个航空公司预订成功的数量。
```

## 小组分工

| 成员 | 负责模块                 | 目录                                 | 主要任务                                                     |
| ---- | ------------------------ | ------------------------------------ | ------------------------------------------------------------ |
| A    | Kafka 数据采集           | `producer/`                          | 编写 Kafka Producer，读取实验数据文件，按行发送到 Kafka topic |
| B    | Spark/MapReduce 数据处理 | `processor/`                         | 读取/消费日志数据，统计每小时每个航空公司 success 数量       |
| C    | 数据存储 + 后端接口      | `sql/`、`backend/`                   | 设计 MySQL/HDFS 存储结构，提供简单查询接口                   |
| D    | 前端展示 + 报告整合      | `frontend/`、`docs/`、`screenshots/` | 实现展示页面，整理实验报告和运行截图                         |

---

## 项目目录结构

```text
bigdata-exp4/
├── producer/          # Kafka 生产者代码
├── processor/         # Spark / MapReduce 数据处理代码
├── backend/           # 后端接口代码
├── frontend/          # 前端展示页面
├── sql/               # 数据库建表 SQL
├── data/              # 实验数据文件
├── docs/              # 实验报告、说明文档
├── screenshots/       # 运行截图
├── scripts/           # 启动脚本
└── README.md          # 项目说明
```

## Git 协作规范

### 克隆项目

第一次参与项目时执行：

```bash
git clone https://github.com/Thxnks/bigdata-exp4.git
cd bigdata-exp4
```

### 分支说明

本项目主要使用以下分支：

```text
main：最终稳定版本
dev：开发整合版本
feature/xxx：每个人自己的功能分支
```

不要直接在 `main` 分支上写代码。

---

### 创建 dev 分支

如果仓库里还没有 `dev` 分支，由仓库负责人执行：

```bash
git checkout -b dev
git push origin dev
```

如果仓库里已经有 `dev` 分支，其他成员执行：

```bash
git fetch origin
git checkout dev
git pull origin dev
```

---

### 每个人创建自己的功能分支

先切换到 `dev` 分支：

```bash
git checkout dev
git pull origin dev
```

然后根据自己负责的模块创建功能分支。

Kafka 数据采集负责人：

```bash
git checkout -b feature/kafka-producer
```

Spark/MapReduce 数据处理负责人：

```bash
git checkout -b feature/spark-process
```

数据存储和后端接口负责人：

```bash
git checkout -b feature/storage-backend
```

前端展示和报告负责人：

```bash
git checkout -b feature/frontend-docs
```

---

### 提交代码

每次完成一小部分功能后，执行：

```bash
git add .
git commit -m "说明本次修改内容"
git push origin 当前分支名
```

示例：

```bash
git add .
git commit -m "add kafka producer"
git push origin feature/kafka-producer
```

---

### 更新最新代码

每天开始写代码前，建议先更新 `dev` 分支：

```bash
git checkout dev
git pull origin dev
```

如果需要把最新的 `dev` 合并到自己的分支：

```bash
git checkout feature/自己的分支名
git merge dev
```

示例：

```bash
git checkout feature/storage-backend
git merge dev
```

---

### 合并代码

每个人完成自己的模块后，把代码 push 到自己的功能分支。

最后由负责人统一合并到 `dev` 分支：

```bash
git checkout dev
git pull origin dev
git merge origin/feature/kafka-producer
git merge origin/feature/spark-process
git merge origin/feature/storage-backend
git merge origin/feature/frontend-docs
git push origin dev
```

测试没问题后，再把 `dev` 合并到 `main`：

```bash
git checkout main
git pull origin main
git merge dev
git push origin main
```

## 统一约定

为了方便最后整合，先统一以下名称。

### Kafka topic 名称

```text
gds-log-topic
```

### 原始数据文件路径

```text
data/kafka采集数据实验.txt
```

### 统计结果字段

```text
stat_hour       时间段
airline_code    航空公司代码
success_count   预订成功数量
```

### MySQL 表名

```text
stat_result
```

---

## 各模块说明

### producer 模块

目录：

```text
producer/
```

作用：

```text
读取 data/kafka采集数据实验.txt 文件，将每一行日志作为一条消息发送到 Kafka topic。
```

需要提交：

```text
1. Kafka Producer 代码
2. topic 创建命令
3. Producer 运行截图
4. Consumer 接收消息截图
```

建议截图放在：

```text
screenshots/producer/
```

---

### processor 模块

目录：

```text
processor/
```

作用：

```text
读取 Kafka 或原始日志文件，对日志进行解析，统计每小时每个航空公司 success 数量。
```

输出结果格式：

```text
stat_hour, airline_code, success_count
```

示例：

```text
2024-05-01 10, CA, 12
2024-05-01 10, MU, 8
2024-05-01 11, CA, 5
```

需要提交：

```text
1. Spark 或 MapReduce 处理代码
2. 统计结果文件
3. 运行截图
```

建议截图放在：

```text
screenshots/processor/
```

---

### backend / sql 模块

目录：

```text
backend/
sql/
```

作用：

```text
设计统计结果表，将 processor 模块生成的统计结果保存到 MySQL 或 HDFS 中，并提供简单查询接口。
```

MySQL 表结构示例：

```sql
CREATE TABLE stat_result (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    stat_hour VARCHAR(50),
    airline_code VARCHAR(20),
    success_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

接口示例：

```text
GET /api/stat/list
```

返回示例：

```json
[
  {
    "statHour": "2024-05-01 10",
    "airlineCode": "CA",
    "successCount": 12
  }
]
```

需要提交：

```text
1. SQL 建表文件
2. 后端接口代码
3. 数据库截图
4. 接口测试截图
```

建议截图放在：

```text
screenshots/backend/
```

---

### frontend / docs 模块

目录：

```text
frontend/
docs/
screenshots/
```

作用：

```text
实现简单前端页面，将统计结果以表格或图表形式展示，并整理实验报告和运行截图。
```

需要提交：

```text
1. 前端展示页面
2. 页面运行截图
3. 实验报告
4. 小组分工说明
5. 每位成员心得
```

建议截图放在：

```text
screenshots/frontend/
```

---

## 最终运行流程

最终项目应当可以按照以下流程运行：

```text
1. 启动 Kafka
2. 创建 topic：gds-log-topic
3. 运行 producer，发送日志数据
4. 运行 processor，统计 success 数量
5. 将统计结果写入 MySQL / HDFS
6. 启动 backend 查询接口
7. 启动 frontend 展示页面
```

---

## 常用 Git 命令

查看当前分支：

```bash
git branch
```

查看当前修改：

```bash
git status
```

提交修改：

```bash
git add .
git commit -m "提交说明"
```

推送代码：

```bash
git push origin 分支名
```

拉取最新代码：

```bash
git pull origin 分支名
```

切换分支：

```bash
git checkout 分支名
```

创建并切换分支：

```bash
git checkout -b 新分支名
```

查看提交记录：

```bash
git log --oneline --graph --all
```

---

## 注意事项

1. **不要直接在 `main` 分支开发**。
2. 每个人只修改自己负责的目录，尽量减少冲突。
3. 每次写代码前先从 `dev` 拉取最新代码。
4. 提交信息尽量写清楚，例如 `add kafka producer`、`add mysql table`。
5. 运行截图统一放到 `screenshots/` 目录。
6. 实验报告统一放到 `docs/` 目录。
7. 原始数据文件统一放到 `data/` 目录。
8. 如果出现代码冲突，不要乱删内容，先在群里说明。
9. 最终提交前，需要保证 `dev` 分支可以完整运行。
10. 最终稳定版本再合并到 `main` 分支。

---

## 当前待办

- [ ] 创建项目基础目录
- [ ] 上传实验数据文件
- [ ] 完成 Kafka Producer
- [ ] 完成 Spark/MapReduce 数据处理
- [ ] 完成 MySQL/HDFS 存储
- [ ] 完成后端查询接口
- [ ] 完成前端展示页面
- [ ] 整理运行截图
- [ ] 完成实验报告