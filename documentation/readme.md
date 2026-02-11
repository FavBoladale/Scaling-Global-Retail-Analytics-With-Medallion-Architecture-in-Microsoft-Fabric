# Scaling Global Retail Analytics with Microsoft Fabric
## Overview
Binaryville is a multinational retailer operating in 27 countries with over 11,000 stores and a rapidly growing e-commerce presence. The organization generates billions of transactional records annually but faces major challenges in consolidating, processing, and analyzing data across regions.

This project demonstrates how a Microsoft Fabric Lakehouse architecture was designed to:
- Consolidate multi-source global retail data
- Process 10B+ transactions annually
- Enable real-time enterprise analytics
- Reduce processing time from 72 hours to under 6 hours
- Support scalable growth and acquisitions

## Business Problem

Binaryville struggled with:
- Fragmented data across 27 countries
- Inconsistent schemas and formats
- Poor historical data quality
- Long processing cycles (72 hours)
- Inability to support real-time decision-making

## Data Sources 
| Source              | Format  | Volume       | Update Frequency |
| ------------------- | ------- | ------------ | ---------------- |
| Customer Data       | CSV     | 500M records | Daily            |
| Product Catalog     | JSON    | 1M SKUs      | Daily            |
| Transaction History | Parquet | 10B annually | Near real-time   |

## Architecture Overview
The solution follows a Medallion (Bronze, Silver, Gold) Lakehouse Architecture implemented in Microsoft Fabric.

### Bronze Layer – Raw Ingestion
The Bronze layer is the raw data storage tier in a data lakehouse architecture. It stores data in its original format (CSV, JSON, Parquet, etc.) as it is ingested from various sources. The purpose of the Bronze layer is to ensure that all raw data is stored securely and in an organized manner, ready for future processing. 

#### Tasks:
- Automated ingestion using Fabric Pipelines
- Raw CSV, JSON, and Parquet data stored in Fabric Lakehouse
- Schema drift handling enabled
- Partitioned by date and region

### Silver Layer – Data Processing & Standardization
The Silver layer is where raw data from the Bronze layer is cleaned, standardized, and prepared for analytical processing. I addressed specific data cleaning and transformation requirements, such as validating customer information, categorizing customer segments, and ensuring data quality by removing junk records.

#### Tasks:
- Data cleaning using Fabric Dataflows Gen2
- Currency normalization
- Time zone standardization
- Regional product code harmonization
- Delta Lake implementation for:
- ACID transactions
- Schema evolution
- Time travel

### Gold Layer – Business-Ready Models
The Gold layer is designed for business-ready data, where key metrics and summaries are derived from the cleansed data stored in the Silver layer. Here, I  aggregated sales data into a daily summary table, which will provide insights into the total sales per day for Binaryville as well as generated sales summaries based on product categories, which will help Binaryville understand how different product categories are performing in terms of total sales.

#### Tasks
- Unified customer 360 view
- Standardized product dimension
- Sales fact tables
- Inventory performance models
- Aggregated regional financial models

### Batch & Incremental Processing
- Spark notebooks designed for incremental loads
- Partition-based processing
- Optimized Delta tables (Z-Ordering, compaction)
- Reduced runtime from 72 hours → under 6 hours

### Analytics & Reporting
I created a semantic model for the Gold layer of the data lakehouse which can be used to create Power BI dashboard report to visualizes the business metrics derived from the Gold layer of the data lakehouse. The goal is to present key metrics like daily sales, category sales, and total revenue in a visually compelling and interactive format for business decision-makers.

Power BI connected directly to Fabric Lakehouse for:
- Real-time financial dashboards
- Global sales performance tracking
- Inventory forecasting models
- Customer personalization insights
- Executive KPI dashboards

## Key Achievements
The solution delivered a 90% reduction in data processing time, significantly accelerating insights across the organization. It improved inventory forecasting accuracy by 25% and enhanced customer personalization efforts, driving a 15% increase in repeat purchases. Additionally, it enabled real-time consolidated financial reporting and established a scalable architecture capable of supporting future acquisitions and growth.



