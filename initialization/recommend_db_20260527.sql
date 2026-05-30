-- MySQL dump 10.13  Distrib 8.0.40, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: recommend_db
-- ------------------------------------------------------
-- Server version	8.0.40

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `eval_metrics`
--

DROP TABLE IF EXISTS `eval_metrics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `eval_metrics` (
                                `id` bigint NOT NULL AUTO_INCREMENT,
                                `metric_start_time` datetime NOT NULL COMMENT '统计开始时间', -- ALTER TABLE eval_metrics CHANGE COLUMN metric_time metric_start_time datetime;
                                `metric_end_time` datetime DEFAULT NULL COMMENT '统计结束时间', -- ALTER TABLE eval_metrics ADD COLUMN metric_end_time datetime AFTER metric_start_time;
                                `pr_auc` decimal(5,4) DEFAULT '0.0000' COMMENT 'PR-AUC',
                                `ctr` decimal(5,4) DEFAULT '0.0000' COMMENT '点击率',
                                `cvr` decimal(5,4) DEFAULT '0.0000' COMMENT '转化率',
                                `recommend_count` int DEFAULT '0' COMMENT '推荐总数',
                                `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
                                PRIMARY KEY (`id`),
                                KEY `idx_metric_start_time` (`metric_start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='评估指标表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `item_features`
--

DROP TABLE IF EXISTS `item_features`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `item_features` (
                                 `item_id` bigint NOT NULL COMMENT '商品ID',
                                 `category_id` bigint NOT NULL COMMENT '商品类目ID',
                                 `pv_count` int DEFAULT '0' COMMENT '总被浏览次数',
                                 `buy_count` int DEFAULT '0' COMMENT '总被购买次数',
                                 `popularity_score` decimal(8,4) DEFAULT '0.0000' COMMENT '热门程度评分 pv*0.3+buy*0.7',
                                 `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                 PRIMARY KEY (`item_id`),
                                 KEY `idx_category_id` (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='商品特征表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `recommendations`
--

DROP TABLE IF EXISTS `recommendations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `recommendations` (
                                   `id` bigint NOT NULL AUTO_INCREMENT,
                                   `user_id` bigint NOT NULL COMMENT '用户ID',
                                   `item_id` bigint NOT NULL COMMENT '推荐商品ID',
                                   `predict_score` decimal(8,4) DEFAULT '0.0000' COMMENT '预测购买概率',
                                   `popularity_score` decimal(8,4) DEFAULT '0.0000' COMMENT '商品热度',
                                   `final_score` decimal(16,12) DEFAULT '0.000000000000' COMMENT '综合排序分 0.7*predict+0.3*popularity', -- ALTER TABLE recommendations MODIFY COLUMN final_score decimal(16,12);
                                   `rank_no` int DEFAULT '0' COMMENT '排名(1-10)',
                                   `recommend_type` varchar(20) DEFAULT 'realtime' COMMENT 'realtime/offline',
                                   `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
                                   PRIMARY KEY (`id`),
                                   UNIQUE KEY `uk_user_item_recommend` (`user_id`,`item_id`,`recommend_type`),
                                   KEY `idx_user_id` (`user_id`),
                                   KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='推荐结果表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_behavior`
--

DROP TABLE IF EXISTS `user_behavior`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_behavior` (
                                 `id` int NOT NULL AUTO_INCREMENT COMMENT '自增主键', -- ALTER TABLE user_behavior MODIFY COLUMN id int NOT NULL AUTO_INCREMENT;
                                 `user_id` bigint NOT NULL COMMENT '用户ID',
                                 `item_id` bigint NOT NULL COMMENT '商品ID',
                                 `category_id` bigint NOT NULL COMMENT '商品类目ID',
                                 `behavior_type` varchar(20) NOT NULL COMMENT 'pv浏览/cart加购/fav收藏/buy购买', -- ALTER TABLE user_behavior MODIFY COLUMN behavior_type varchar(20);
                                 `behavior_time` bigint NOT NULL COMMENT '时间戳(秒)',
                                 `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
                                 PRIMARY KEY (`id`),
                                 UNIQUE KEY `uk_user_item_behavior` (`user_id`,`item_id`,`behavior_type`,`behavior_time`),
                                 KEY `idx_user_id` (`user_id`),
                                 KEY `idx_item_id` (`item_id`),
                                 KEY `idx_behavior_time` (`behavior_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户行为记录表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_features`
--

DROP TABLE IF EXISTS `user_features`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `user_features` (
                                 `user_id` bigint NOT NULL COMMENT '用户ID',
                                 `pv_count` int DEFAULT '0' COMMENT '总浏览次数',
                                 `buy_count` int DEFAULT '0' COMMENT '总购买次数',
                                 `cart_count` int DEFAULT '0' COMMENT '总加购次数',
                                 `fav_count` int DEFAULT '0' COMMENT '总收藏次数',
                                 `buy_rate` decimal(8,4) DEFAULT '0.0000' COMMENT '浏览转购买率',
                                 `active_score` decimal(8,4) DEFAULT '0.0000' COMMENT '活跃度评分',
                                 `high_freq_category` bigint DEFAULT '0' COMMENT '高频类目ID',
                                 `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                 PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户特征表';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed