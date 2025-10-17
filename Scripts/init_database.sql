-- ============================================================
-- Database Initialization Script
-- Purpose: Set up Datawarehouse with simulated schemas (bronze, silver, gold)
-- Compatible with: MySQL 8+
-- Author: Sagar Meravi
-- ============================================================

-- 1️ Create main metadata/control database
CREATE DATABASE IF NOT EXISTS Datawarehouse
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- 2️ Use the main Datawarehouse for management or logging
USE Datawarehouse;

-- 3️ Create simulated schema layers as separate databases
-- Note: In MySQL, CREATE SCHEMA = CREATE DATABASE
-- These represent typical Data Lake layers:
--  - Bronze: Raw data
--  - Silver: Cleaned/transformed data
--  - Gold: Business-ready data

CREATE DATABASE IF NOT EXISTS bronze_datawarehouse
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS silver_datawarehouse
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS gold_datawarehouse
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- Verification step
SHOW DATABASES LIKE '%datawarehouse%';
