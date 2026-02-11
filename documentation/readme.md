# Scaling Global Retail Analytics with Microsoft Fabric

<img width="1536" height="1024" alt="Medallion Architecture Diagram" src="https://github.com/user-attachments/assets/11f6e032-1e31-4005-ae7b-393f754bcee5" />

## Overview
This repository demonstrates an enterprise-scale data platform built for Binaryville, a multinational retailer operating in 27 countries with over 11,000 stores and a strong e-commerce presence.

The solution implements a Medallion Architecture (Bronze, Silver, Gold) in Microsoft Fabric, with Azure Data Lake Storage (ADLS) serving as the centralized ingestion layer. It processes billions of records across customer, product, and transaction data to enable real-time enterprise analytics.


## Business Problem

Binaryville struggled with fragmented data spread across 27 countries, resulting in inconsistent schemas and varying data formats across regions. Historical records, particularly from legacy systems and recent acquisitions, suffered from poor data quality. These challenges led to prolonged processing cycles of up to 72 hours and limited the organization’s ability to generate timely, reliable insights. As a result, the business was unable to effectively support real-time, company-wide decision-making.

## Data Sources 
| Source              | Format  | Volume       | Update Frequency |
| ------------------- | ------- | ------------ | ---------------- |
| Customer Data       | CSV     | 500M records | Daily            |
| Product Catalog     | JSON    | 1M SKUs      | Daily            |
| Transaction History | Parquet | 10B annually | Near real-time   |

These sources originate from CRM, Inventory Management, POS, and e-commerce systems across 27 countries.

## Architecture Overview
The solution is built on a scalable Lakehouse architecture using Microsoft Fabric, designed to consolidate and transform massive volumes of global retail data into trusted, analytics-ready assets. It follows a structured data flow:

Customer / Product / Orders Data → ADLS → Fabric Data Pipeline → Bronze → Silver → Gold → Power BI

At a high level, the architecture separates data ingestion, storage, transformation, and consumption into clearly defined layers to ensure scalability, maintainability, and performance.

The process begins with raw data ingestion from multiple enterprise systems across 27 countries. Customer data (CSV), product data (JSON), and transactional order data (Parquet) are first landed in Azure Data Lake Storage (ADLS), which acts as the centralized and scalable storage foundation. ADLS enables cost-efficient storage of billions of records, supports multiple file formats, and retains five years of historical data for analytical and compliance purposes.

From ADLS, Microsoft Fabric Data Pipelines orchestrate automated and incremental data movement into the Fabric Lakehouse. This orchestration layer ensures reliable scheduling, monitoring, error handling, and schema drift management. It enables both historical backfill processing and daily incremental loads without disrupting ongoing business operations.

Once inside Microsoft Fabric, the solution implements a Medallion Architecture (Bronze, Silver, Gold) to progressively refine data quality and usability:

### Bronze Layer – Raw Ingestion
The Bronze layer stores raw, source-aligned data in Delta tables with minimal transformation. It preserves data lineage and supports schema evolution while enabling scalable distributed processing.The Bronze layer is the raw data storage tier in a data lakehouse architecture. It stores data in its original format (CSV, JSON, Parquet, etc.) as it is ingested from various sources. The purpose of the Bronze layer is to ensure that all raw data is stored securely and in an organized manner, ready for future processing. 



### Silver Layer – Data Processing & Standardization
The Silver layer is responsible for transforming raw Bronze data into clean, standardized, and analytics-ready datasets. At this stage, data is validated, deduplicated, and enriched to ensure quality and consistency across regions. Key transformations include validating customer information, categorizing customer segments, removing junk records, normalizing currencies, aligning time zones, and harmonizing regional product codes. These processes are implemented using Fabric Dataflows Gen2 and Spark-based transformations, while Delta Lake capabilities such as ACID transactions, schema evolution, and time travel ensure data reliability, governance, and optimized performance.


### Gold Layer – Business-Ready Models
The Gold layer delivers business-ready, curated datasets structured for analytics. This layer includes unified customer views, standardized product dimensions, sales fact tables, financial aggregates, and inventory performance models. Data models here are optimized for reporting and enterprise KPIs. 

Finally, Power BI connects directly to the Gold layer, enabling real-time dashboards and executive reporting across sales, finance, marketing, and operations. This architecture supports near real-time insights while maintaining strong governance, scalability, and performance optimization.

Overall, the solution balances large-scale batch processing with incremental updates, reduces processing time dramatically, and establishes a future-ready data platform capable of supporting acquisitions, global expansion, and advanced analytics initiatives.


### Batch & Incremental Processing
To efficiently handle five years of historical data alongside continuous daily updates, the solution implements a robust batch and incremental processing strategy using Spark notebooks within Microsoft Fabric. Historical data loads were processed in distributed batches, while daily incremental loads were designed to capture only newly added or updated records, significantly reducing unnecessary computation. Partition-based processing was applied using region and date to optimize query performance and enable partition pruning. Delta Lake optimizations, including Z-Ordering and table compaction, were implemented to improve read performance and storage efficiency. Together, these strategies reduced total processing time from 72 hours to under 6 hours while maintaining data consistency and reliability at scale.


## Key Achievements
The implementation delivered measurable enterprise impact by reducing data processing time by 90%, dramatically accelerating the availability of insights across the organization. Enhanced data standardization and modeling improved inventory forecasting accuracy by 25%, enabling better demand planning and stock optimization. Improved customer data consolidation powered personalization strategies that increased repeat purchases by 15%. Additionally, the architecture enabled real-time consolidated financial reporting across all regions and established a scalable, future-ready platform capable of supporting continued global expansion and acquisitions.



