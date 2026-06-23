# Flink 实时电商推荐系统

基于 Apache Flink 的实时电商推荐系统，结合机器学习算法实现个性化商品推荐。

## 项目结构

```
├── initialization/     # 数据库初始化脚本
│   ├── recommend_db_20260527.sql    # 数据库建表脚本
│   ├── data_import_mysql_UF.py      # 用户特征数据导入
│   └── data_import_mysql_UB.py      # 用户行为数据导入
├── flink/              # Flink 实时计算任务
│   ├── src/            # Java/Scala 源码
│   ├── pom.xml         # Maven 配置
│   └── data_simulator_kafka.py      # Kafka 数据模拟器
└── web/                # 前后端服务
    ├── backend/        # 后端服务
    └── frontend/       # 前端界面
```

## 技术栈

- **实时计算**: Apache Flink
- **消息队列**: Apache Kafka
- **数据存储**: MySQL + Redis
- **后端**: Java/Spring Boot
- **前端**: Vue.js
- **数据模拟**: Python

## 快速开始

### 1. 环境准备

- JDK 8+
- Maven 3.6+
- Python 3.6+
- MySQL 5.7+
- Redis
- Flink 1.12+
- Kafka

### 2. 数据库初始化

```bash
# 执行 SQL 脚本
mysql -u root -p < initialization/recommend_db_20260527.sql

# 导入历史数据
cd initialization
python data_import_mysql_UF.py
python data_import_mysql_UB.py
```

### 3. 启动中间件

```bash
# 启动 ZooKeeper
zkServer.sh start

# 启动 Kafka
kafka-server-start.sh -daemon config/server.properties

# 启动 Flink
start-cluster.sh
```

### 4. 编译并提交 Flink 任务

```bash
cd flink
mvn clean package -DskipTests

# 在 Flink Web UI 提交 jar 包
```

### 5. 启动 Web 服务

```bash
# 启动后端
cd web/backend
mvn spring-boot:run

# 启动前端
cd web/frontend
npm install
npm run serve
```

### 6. 启动数据模拟器

```bash
cd flink
python data_simulator_kafka.py
```

## 注意事项

- 确保系统环境变量中已配置 Python 路径
- Redis 默认无密码，如有密码请修改配置文件
- 定期清理 `evaluating` 文件夹中的 log 文件