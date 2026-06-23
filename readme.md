# Flink 实时电商推荐系统

基于 Apache Flink 的实时电商用户购买意向预测与推荐系统，结合 PMML 机器学习模型实现个性化商品推荐，并提供实时监控数据大屏。

## 系统架构

```
数据模拟器 (Python)
    │
    ▼  Kafka / Socket
┌─────────────────────────────────────────────────────────┐
│                   Apache Flink 流处理                     │
│                                                         │
│  Kafka Source ──▶ 数据清洗 ──▶ 数据解析 ──▶ 实时行为落库   │
│       │                                                   │
│       ▼                                                   │
│  滑动窗口聚合(30s/10s) ──▶ 数据倾斜优化(两阶段聚合)        │
│       │                                                   │
│       ▼                                                   │
│  特征合并(冷启动处理) ──▶ PMML模型预测 ──▶ TopK排序        │
│       │                                                   │
│       ▼                                                   │
│  MySQL Sink (推荐结果入库) + Redis Sink (推荐缓存)         │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
              Spring Boot 后端 API
                         │
                         ▼
              Vue 3 实时监控大屏
```

## 技术栈

| 层级 | 技术 | 版本 |
|------|------|------|
| **实时计算** | Apache Flink | 1.15.2 |
| **消息队列** | Apache Kafka | 3.0.0 |
| **数据存储** | MySQL | 8.0 |
| **缓存** | Redis (Jedis) | 3.6.3 |
| **机器学习模型** | PMML (JPMML Evaluator) | 1.6.4 |
| **后端框架** | Spring Boot + MyBatis Plus | 3.1.10 |
| **前端框架** | Vue 3 + Vite + Element Plus | 3.3.4 |
| **可视化** | ECharts + echarts-liquidfill | 5.4.3 |
| **数据模拟** | Python + pandas + kafka-python | 3.6+ |
| **构建工具** | Maven | 3.6+ |

## 项目结构

```
├── initialization/                        # 数据库初始化
│   ├── recommend_db_20260527.sql          # MySQL 建表脚本 (5张表)
│   ├── data_import_mysql_UF.py            # 用户特征数据导入
│   ├── data_import_mysql_UB.py            # 用户行为数据导入
│   └── split_data/                        # 训练集/测试集/用户特征 CSV
│
├── flink/                                 # Flink 实时计算模块
│   ├── src/main/java/recommend/
│   │   ├── StreamRecommendJob.java        # 主入口：流处理推荐任务
│   │   ├── 批处理/BatchFeatureJob.java    # 批处理特征工程
│   │   ├── 流处理/StreamFeatureJob.java   # 流处理特征工程
│   │   ├── clean/
│   │   │   ├── InvalidDataFilter.java     # 无效数据过滤
│   │   │   └── TimeFormatNormalizer.java  # 时间戳格式归一化
│   │   ├── optimizer/
│   │   │   └── DataSkewOptimizer.java     # 两阶段聚合数据倾斜优化
│   │   └── sink/
│   │       ├── MySQLSink.java             # MySQL 输出
│   │       └── RedisSink.java             # Redis 输出
│   ├── src/main/resources/
│   │   └── recommend_model_champion.pmml  # 训练好的 PMML 模型
│   ├── config/
│   │   └── recommend-config.properties    # 系统配置文件
│   ├── data_simulator_kafka.py            # 数据模拟器 (Kafka/Socket)
│   ├── data/                              # 测试数据
│   └── pom.xml                            # Maven 依赖配置
│
└── web/                                   # Web 前后端
    ├── backend/                           # Spring Boot 后端
    │   └── src/main/java/com/recommend/
    │       ├── RecommendApplication.java  # 应用入口
    │       ├── controller/DataController.java  # REST API (15个接口)
    │       ├── service/DataService.java        # 业务逻辑
    │       ├── service/ScheduledTaskService.java  # 定时任务
    │       ├── entity/                    # 数据实体
    │       ├── mapper/                    # MyBatis Plus Mapper
    │       └── config/                    # Redis/CORS 配置
    └── frontend/                          # Vue 3 前端
        └── src/
            ├── App.vue                    # 主监控大屏
            └── components/                # ECharts 图表组件
                ├── BarChart.vue           # 柱状图
                ├── LineChart.vue          # 折线图
                ├── PieChart.vue           # 饼图
                ├── FunnelChart.vue        # 漏斗图
                ├── RadarChart.vue         # 雷达图
                ├── LiquidChart.vue        # 水球图
                └── ManualInputDrawer.vue  # 人工行为录入
```

## 数据库设计

### 5张核心表

| 表名 | 用途 | 关键字段 |
|------|------|---------|
| `user_features` | 用户特征表 | user_id, buy_rate, active_score, high_freq_category |
| `item_features` | 商品特征表 | item_id, category_id, popularity_score (=pv\*0.3+buy\*0.7) |
| `user_behavior` | 用户行为记录表 | user_id, item_id, behavior_type(pv/cart/fav/buy), behavior_time |
| `recommendations` | 推荐结果表 | user_id, item_id, predict_score, final_score(=0.7\*pred+0.3\*pop), rank_no |
| `eval_metrics` | 模型评估指标表 | pr_auc, ctr, cvr, recommend_count |

## 推荐算法

### 核心流程

1. **滑动窗口聚合**（30秒窗口，10秒滑动步长）
   - 统计每个 `(user_id, item_id)` 在窗口内的 `pv_count_5min`（浏览次数）
   - 检测是否有加购/收藏行为 `is_cart_or_fav`

2. **特征合并**（从 Redis/MySQL 获取）
   - 用户特征：`buy_rate`（购买率）、`active_score`（活跃度）、`high_freq_category`（高频类目）
   - 实时特征：`pv_count_5min`、`is_cart_or_fav`
   - 交叉特征：`category_match`（当前商品类目是否匹配用户高频偏好类目）

3. **PMML 模型预测**
   - 使用 JPMML Evaluator 加载 Python 训练的 PMML 模型
   - 输入 5 个特征，输出购买概率 `predict_score`

4. **综合评分**
   - `final_score = 0.7 × predict_score + 0.3 × popularity_score`

5. **TopK 排序**
   - 维护每个用户的历史最优推荐结果
   - 每 5 秒输出 Top 10 推荐

### 冷启动策略

新用户无历史特征时使用默认值：
- `buy_rate` = 0.05
- `active_score` = 1.0
- `high_freq_category` = 当前浏览商品类目

### 数据倾斜优化

采用两阶段聚合（Local-Global Aggregation）解决热门商品导致的计算倾斜：
- 第一阶段：添加随机前缀（0~9）打散到多分区局部聚合
- 第二阶段：去除前缀全局聚合

## REST API

后端提供 15 个 REST 接口（`/api/*`）：

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查 |
| `/api/behavior/distribution` | GET | 用户行为分布统计 |
| `/api/behavior/funnel` | GET | 用户行为转化漏斗 |
| `/api/behavior/recent` | GET | 用户最近行为记录 |
| `/api/behavior/insert` | POST | 插入用户行为（写入 Kafka） |
| `/api/recommend/latest` | GET | 查询指定用户最新推荐 |
| `/api/recommend/prediction-stats` | GET | 推荐预测统计 |
| `/api/recommend/ctr-cvr` | GET | CTR/CVR 查询 |
| `/api/recommend/ctr-cvr-trend` | GET | CTR/CVR 趋势 |
| `/api/recommend/count-trend` | GET | 推荐次数趋势 |
| `/api/metrics/pr-auc` | GET | 最新 PR-AUC |
| `/api/metrics/pr-auc-trend` | GET | PR-AUC 趋势 |
| `/api/item/top10` | GET | TOP10 热门商品 |
| `/api/user/radar` | GET | 用户画像雷达图 |
| `/api/model/retrain` | POST | 触发模型重训练 |
| `/api/model/evaluate` | POST | 触发模型评测 |

## 前端监控大屏

实时监控大屏包含以下可视化模块：

- **顶部**：PR-AUC / CTR / CVR 水球图 + 推荐总数翻牌器
- **左侧**：用户画像雷达图、行为转化漏斗、行为分布柱状图
- **中间**：用户推荐结果排行表（可按用户 ID 查询）
- **右侧**：TOP10 热门商品、推荐预测饼图、CTR & CVR 双轴趋势图
- **底部**：实时推荐数量趋势折线图
- **交互**：支持时间段筛选（今天/最近2/6/24小时）、人工行为录入

大屏每 3 秒自动刷新数据。

## 快速开始

### 环境要求

- JDK 17（后端）/ JDK 8+（Flink）
- Maven 3.6+
- Python 3.6+（含 pandas, kafka-python, sqlalchemy）
- MySQL 8.0+
- Redis
- Flink 1.15+
- Kafka 3.0+
- Node.js 16+（前端）

### 1. 数据库初始化

```bash
# 创建数据库并导入表结构
mysql -u root -p < initialization/recommend_db_20260527.sql

# 导入历史用户特征数据
cd initialization
python data_import_mysql_UF.py

# 导入历史用户行为数据
python data_import_mysql_UB.py
```

### 2. 启动中间件

```bash
# 启动 ZooKeeper
zkServer.sh start

# 启动 Kafka（创建 Topic）
kafka-server-start.sh -daemon config/server.properties
kafka-topics.sh --create --topic user-behavior-topic \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 启动 Flink
start-cluster.sh

# 启动 Redis
redis-server
```

### 3. 编译并提交 Flink 任务

```bash
cd flink
mvn clean package -DskipTests

# 通过 Flink Web UI (http://localhost:8081) 上传 JAR 包
# 主类：recommend.StreamRecommendJob
```

### 4. 启动 Web 服务

```bash
# 启动后端 (端口 8080)
cd web/backend
mvn spring-boot:run

# 启动前端 (端口 5173)
cd web/frontend
npm install
npm run dev
```

### 5. 启动数据模拟器

```bash
cd flink

# Kafka 模式（推荐）
python data_simulator_kafka.py --kafka

# Socket 模式
python data_simulator_kafka.py

# Turbo 高速模式 (~5000条/秒)
python data_simulator_kafka.py --kafka --turbo

# 自定义速率
python data_simulator_kafka.py --kafka  # 默认 200条/秒
```

访问 http://localhost:5173 查看监控大屏。

## 配置说明

### Flink 配置（`flink/config/recommend-config.properties`）

```properties
# MySQL
mysql.url=jdbc:mysql://192.168.88.8:3306/recommend_db
mysql.username=flinkpy
mysql.password=123456

# Redis
redis.host=192.168.88.8
redis.port=6379

# 推荐参数
recommend.topk=10
recommend.predict.weight=0.7
recommend.popularity.weight=0.3

# 滑动窗口
window.size.minutes=5
window.slide.minutes=1

# 冷启动默认值
recommend.default.buy.rate=0.05
recommend.default.active.score=1.0
recommend.default.popularity=0.5
```

### 数据模拟器参数

```bash
# 格式：python data_simulator_kafka.py [文件路径] [主机] [端口] [速率]
python data_simulator_kafka.py data/test.csv localhost 9999 200
python data_simulator_kafka.py --kafka                           # Kafka 模式
python data_simulator_kafka.py --turbo                           # 高速模式
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 文件路径 | split_data/test.csv | 支持 CSV / XLSX |
| 主机 | localhost | Socket 目标主机 |
| 端口 | 9999 | Socket 目标端口 |
| 速率 | 200 条/秒 | 普通模式 1~500 |
| Turbo | 5000 条/秒 | `--turbo` 开启 |

## 注意事项

- 确保 Kafka Topic `user-behavior-topic` 已创建
- Redis 默认无密码，如有密码需修改 `StreamRecommendJob.java` 和后端配置
- PMML 模型文件 `recommend_model_champion.pmml` 需放在 `flink/src/main/resources/` 下
- 后端 Spring Boot 需 JDK 17，Flink 模块需 JDK 8+
- 前端每 3 秒自动拉取后端数据刷新大屏
- 定时任务（每 20 秒）自动更新用户/商品特征并刷新 Redis 缓存
