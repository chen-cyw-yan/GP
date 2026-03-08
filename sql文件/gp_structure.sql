-- MySQL dump 10.13  Distrib 8.0.36, for Win64 (x86_64)
--
-- Host: localhost    Database: gp
-- ------------------------------------------------------
-- Server version	8.0.36

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `stock`
--

DROP TABLE IF EXISTS `stock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `stock` (
  `date` date NOT NULL COMMENT '交易日期',
  `open` double DEFAULT NULL COMMENT '开盘价（元）',
  `high` double DEFAULT NULL COMMENT '最高价（元）',
  `low` double DEFAULT NULL COMMENT '最低价（元）',
  `close` double DEFAULT NULL COMMENT '收盘价（元）',
  `volume` double DEFAULT NULL COMMENT '成交量（手）',
  `amount` double DEFAULT NULL COMMENT '成交额（千元）',
  `outstanding_share` double DEFAULT NULL COMMENT '流通股本（万股）',
  `turnover` double DEFAULT NULL COMMENT '换手率（%）',
  `name` text COMMENT '股票名称',
  `code` varchar(20) NOT NULL COMMENT '股票代码',
  PRIMARY KEY (`code`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock_sector_info`
--

DROP TABLE IF EXISTS `stock_sector_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `stock_sector_info` (
  `sector_name` varchar(100) DEFAULT NULL COMMENT '板块名称',
  `sector_code` varchar(20) NOT NULL COMMENT '板块代码',
  `date` date NOT NULL COMMENT '交易日期',
  `open` double DEFAULT NULL COMMENT '开盘价（元）',
  `high` double DEFAULT NULL COMMENT '最高价（元）',
  `low` double DEFAULT NULL COMMENT '最低价（元）',
  `change_amount` decimal(20,4) DEFAULT NULL COMMENT '涨跌额',
  `change_percent` decimal(10,4) DEFAULT NULL COMMENT '涨跌幅',
  `volume` double DEFAULT NULL COMMENT '成交量（手）',
  `amount` double DEFAULT NULL COMMENT '成交额（千元）',
  `total_market_value` decimal(30,2) DEFAULT NULL COMMENT '总市值',
  `turnover_rate` decimal(10,4) DEFAULT NULL COMMENT '换手率',
  `rise_count` int DEFAULT NULL COMMENT '上涨家数',
  `fall_count` int DEFAULT NULL COMMENT '下跌家数',
  `leading_stock` varchar(100) DEFAULT NULL COMMENT '领涨股票',
  `leading_stock_code` varchar(20) DEFAULT NULL COMMENT '领涨股票代码',
  `leading_stock_change_percent` decimal(10,4) DEFAULT NULL COMMENT '领涨股票-涨跌幅',
  PRIMARY KEY (`date`,`sector_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票板块行情数据表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock_sector_relation`
--

DROP TABLE IF EXISTS `stock_sector_relation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `stock_sector_relation` (
  `stock_code` varchar(20) NOT NULL COMMENT '股票代码 (如: 600519.SH)',
  `stock_name` varchar(100) DEFAULT NULL COMMENT '股票名称 (冗余字段，方便快速查看)',
  `sector_code` varchar(20) NOT NULL COMMENT '板块代码',
  `sector_name` varchar(100) DEFAULT NULL COMMENT '板块名称 (冗余字段)',
  `sector_type` varchar(50) DEFAULT NULL COMMENT '板块类型 (枚举: 行业/概念/地区/指数/风格)',
  `is_primary` tinyint(1) DEFAULT '0' COMMENT '是否为主营板块 (1:是, 0:否)',
  `entry_date` date DEFAULT NULL COMMENT '纳入板块日期',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  UNIQUE KEY `uk_stock_sector` (`stock_code`,`sector_code`),
  KEY `idx_stock_code` (`stock_code`),
  KEY `idx_sector_code` (`sector_code`),
  KEY `idx_sector_type` (`sector_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票与板块关联关系表';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_support_resistance`
--

DROP TABLE IF EXISTS `tick_support_resistance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tick_support_resistance` (
  `trade_date` date NOT NULL COMMENT '交易日期',
  `symbol` varchar(20) NOT NULL COMMENT '证券代码，如 600519、510300',
  `support_price` decimal(10,3) DEFAULT NULL COMMENT '当日计算得到的支撑位价格（密集成交区下沿）',
  `resistance_price` decimal(10,3) DEFAULT NULL COMMENT '当日计算得到的压力位价格（密集成交区上沿）',
  `vwap` decimal(10,3) DEFAULT NULL COMMENT '当日成交量加权平均价（VWAP，资金成本中枢）',
  `dense_lower` decimal(10,3) DEFAULT NULL COMMENT '密集成交区价格下边界',
  `dense_upper` decimal(10,3) DEFAULT NULL COMMENT '密集成交区价格上边界',
  `dense_ratio` decimal(5,2) DEFAULT NULL COMMENT '密集成交区占当日成交金额/成交量比例',
  `calc_method` varchar(50) DEFAULT NULL COMMENT '支撑压力计算方法：amount_profile / volume_profile',
  `price_bin` decimal(6,3) DEFAULT NULL COMMENT '价格分桶粒度，如 0.01(股票) / 0.001(ETF)',
  `total_amount` bigint DEFAULT NULL COMMENT '当日总成交金额',
  `total_volume` bigint DEFAULT NULL COMMENT '当日总成交量',
  PRIMARY KEY (`trade_date`,`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='基于分笔成交数据计算的当日支撑位与压力位结果表';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-02-26 17:17:45



-- gp.stock_call_auction definition

CREATE TABLE `stock_call_auction` (
  `trade_date` date NOT NULL COMMENT '交易日期 (例如: 2023-10-27)',
  `stock_code` varchar(10) NOT NULL COMMENT '股票代码 (例如: sh600000)',
  `stock_name` varchar(50) DEFAULT NULL COMMENT '股票名称 (例如: 浦发银行)',
  `auction_time` time DEFAULT NULL COMMENT '竞价时间 (固定为 09:25:00)',
  `volume` bigint DEFAULT NULL COMMENT '成交量 (19396)',
  `amount` bigint DEFAULT NULL COMMENT '成交金额 (26844064)',
  `nature` varchar(10) DEFAULT NULL COMMENT '性质 (买盘/卖盘)',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '数据入库时间',
  PRIMARY KEY (`trade_date`,`stock_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='集合竞价结束数据表 (无价格版)';



-- gp.stock_abnormal_monitor definition

CREATE TABLE `stock_abnormal_monitor` (
  `stock_code` varchar(20) NOT NULL COMMENT '股票代码，例如 sh601179 或 sz002355',
  `stock_name` varchar(50) DEFAULT NULL COMMENT '股票名称',
  `trade_date` date NOT NULL COMMENT '交易日期，格式 YYYY-MM-DD',
  `close_price` decimal(10,4) DEFAULT NULL COMMENT '当日收盘价',
  `trigger_count` int DEFAULT '0' COMMENT '触发信号次数',
  `is_abnormal_type` varchar(100) DEFAULT NULL COMMENT '是否异动类型描述，如 "3日涨跌幅异常(24.76%)" 或 NULL',
  `next_day_may_trigger` varchar(10) DEFAULT '0' COMMENT '下一日是否可能触发异动标识: 1=是(True), 0=否(False)',
  `min_required_change` decimal(10,6) DEFAULT NULL COMMENT '下一日触发异动所需的最小涨幅比例 (小数形式，如 0.088497)',
  `target_level` varchar(50) DEFAULT NULL COMMENT '目标异动等级，如 "5日异动", "3日异动", "10日异动"',
  `warning_info` varchar(255) DEFAULT NULL COMMENT '预警详细信息，如 "明日若涨 8.85% 将触发5日异动"',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  PRIMARY KEY (`trade_date`,`stock_code`) COMMENT '联合主键：日期 + 股票代码，确保同一天同一只股票唯一',
  KEY `idx_next_day_trigger` (`next_day_may_trigger`) COMMENT '快速筛选下一日可能触发的股票',
  KEY `idx_target_level` (`target_level`) COMMENT '按目标等级查询的索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='股票异动监控与预警数据表 (无ID版)';