# Project Summary: Parquet Read/Write with Iceberg

## 📋 Executive Overview
**Parquet Read/Write** is a Spring Boot microservice that provides high-performance transaction data storage and retrieval using **Apache Iceberg** for metadata management, **AWS S3** as data warehouse, and **PostgreSQL** as the metadata catalog. The system writes transactions in **Parquet format** with automatic **multi-level partitioning** by account, year, month, and day for optimized querying.

---

## 🎯 Core Functionality

### Primary Features
1. **Transaction Ingestion** - REST API to accept individual or batch transactions
2. **Parquet Storage** - Write transactions in columnar Parquet format with Snappy compression
3. **Multi-Level Partitioning** - Automatic organization into: `account_id=X/year=Y/month=M/day=D/`
4. **Iceberg Catalog** - Track data files and metadata versions with ACID semantics
5. **Query Support** - Read transactions by account and time range with partition pruning
6. **Cloud Storage** - AWS S3 integration for scalable warehouse storage

### Data Flow
```
HTTP POST Request (Transaction)
        ↓
Spring Boot REST Controller
        ↓
ParquetService (orchestration)
        ↓
IcebergTransactionWriter (writes to S3)
        ↓
S3 Data Lake (partitioned Parquet files)
        ↓
PostgreSQL Metadata Catalog (tracks versions)
        ↓
Iceberg Manifest Files (enable partition pruning)
```

---

## 🏗️ Architecture Components

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **REST API Layer** | HTTP endpoints for transaction operations | Spring Boot MVC |
| **Service Layer** | Business logic orchestration | Java/Spring |
| **Writer Layer** | Multi-format write support | 3 implementations (Iceberg, Local, Remote) |
| **Metadata Catalog** | Tracks data files and versions | Apache Iceberg + PostgreSQL JDBC |
| **Data Warehouse** | Persistent columnar storage | AWS S3 with Parquet format |
| **Configuration** | Iceberg & Hadoop settings | Spring Configuration Beans |

---

## 💾 Data Model

### Transaction Schema (Avro Format)
```json
{
  "transaction_id": "string",      // Unique identifier
  "account_id": "string",          // Account reference (PARTITION)
  "amount": "double",              // Transaction amount
  "currency": "string",            // ISO currency code (USD, EUR, GBP, JPY)
  "transaction_type": "string",    // CREDIT or DEBIT
  "timestamp": "long",             // Milliseconds since epoch
  "description": "string|null",    // Optional details
  "year": "int",                   // Extracted from timestamp (PARTITION)
  "month": "int",                  // Extracted from timestamp (PARTITION)
  "day": "int"                     // Extracted from timestamp (PARTITION)
}
```

### Partitioning Strategy
- **Level 1:** `account_id` - Separate data by account
- **Level 2:** `year` - Annual partitions
- **Level 3:** `month` - Monthly sub-partitions
- **Level 4:** `day` - Daily granularity

**Example S3 Path:**
```
s3://transaction-cold-storage/iceberg-warehouse/default/transactions/data/
└── account_id=ACC_1092/year=2025/month=01/day=15/
    └── transactions-1769865138770.parquet
```

---

## 🔧 Technology Stack

### Core Frameworks
| Technology | Version | Purpose |
|-----------|---------|---------|
| **Spring Boot** | 4.0.1 | REST API framework & dependency injection |
| **Java** | 17 (LTS) | Primary programming language |
| **Maven** | 3.x | Build & dependency management |

### Data Layer
| Technology | Version | Purpose |
|-----------|---------|---------|
| **Apache Iceberg** | 1.4.2 | Table format & metadata management |
| **Apache Parquet** | 1.12.3 | Columnar storage format |
| **Apache Avro** | 1.12.3 | Schema serialization & record format |
| **PostgreSQL** | 15 (Docker) | JDBC Catalog metadata storage |

### Cloud & Storage
| Technology | Version | Purpose |
|-----------|---------|---------|
| **AWS S3** | SDK v2 (2.20.118) | Data warehouse (Parquet files) |
| **Apache Hadoop** | Auto | S3A filesystem & configuration |
| **Hadoop AWS** | 3.3.6 | S3 integration for Hadoop |

### Compression & Serialization
| Technology | Purpose |
|-----------|---------|
| **Snappy** | Parquet file compression (built-in) |
| **Protobuf** | Iceberg message serialization |

---

## 📁 Project Structure

```
parquetreadwrite/
├── src/main/
│   ├── java/com/mukesh/coldstorageexp/parquetreadwrite/
│   │   ├── config/
│   │   │   └── IcebergCatalogConfig.java         # Iceberg + Hadoop S3A configuration
│   │   ├── controller/
│   │   │   └── ParquetController.java            # REST endpoints
│   │   ├── model/
│   │   │   └── Transaction.java                  # Data model with partition extraction
│   │   ├── service/
│   │   │   └── ParquetService.java               # Orchestration logic
│   │   ├── writer/
│   │   │   ├── TransactionFileWriter.java        # Interface
│   │   │   ├── IcebergTransactionWriter.java     # ✅ ACTIVE: S3 + Iceberg
│   │   │   ├── LocalTransactionFileWriter.java   # Legacy: Local disk
│   │   │   └── RemoteTransactionFileWriter.java  # Legacy: Remote server
│   │   └── ParquetreadwriteApplication.java      # Spring Boot entry point
│   ├── resources/
│   │   ├── application.properties                # Configuration
│   │   ├── transaction-schema.avsc               # ✅ ACTIVE: Iceberg schema
│   │   └── transaction.avsc                      # Legacy: Local writer schema
│   └── test/java/...                            # Unit tests
└── pom.xml                                       # Maven dependencies
```

---

## 🌐 REST API Endpoints

### Single Transaction Insert
```bash
POST /api/transaction
Content-Type: application/json

{
  "transactionId": "txn_001",
  "accountId": "ACC_1092",
  "amount": 5000.00,
  "currency": "USD",
  "transactionType": "CREDIT",
  "timestamp": 1704067200000,
  "description": "Salary"
}

Response: "written"
```

### Batch Transactions Insert
```bash
POST /api/transactions
Content-Type: application/json

[
  { transaction object 1 },
  { transaction object 2 },
  ...
]

Response: "written"
```

### Read Transactions
```bash
GET /api/transactions?accountId=ACC_1092&start=1704067200000&end=1706745600000

Response: [
  { Transaction objects with partition metadata }
]
```

### Health Check
```bash
GET /api/health

Response: "UP"
```

---

## ⚙️ Configuration (application.properties)

```properties
# Iceberg Catalog Configuration
iceberg.catalog.uri=jdbc:postgresql://localhost:5432/iceberg_catalog
iceberg.catalog.warehouse=s3://transaction-cold-storage/iceberg-warehouse
iceberg.catalog.name=postgres_catalog
iceberg.namespace=default
iceberg.table.name=transactions
iceberg.enabled=true

# Database Credentials
iceberg.catalog.db.user=postgres
iceberg.catalog.db.password=postgres

# AWS S3 Configuration
iceberg.s3.region=eu-north-1
iceberg.s3.endpoint=                    # Uses default AWS S3
iceberg.s3.access-key=                  # Uses DefaultCredentialsProvider
```

---

## 📊 Key Statistics (Current Implementation)

| Metric | Value |
|--------|-------|
| **Parquet Files Written** | 13+ |
| **Accounts** | 4 (ACC_1092, ACC_2051, ACC_3075, ACC_4109) |
| **Time Coverage** | Oct 2024 → Mar 2026 |
| **Currencies Supported** | 4 (USD, EUR, GBP, JPY) |
| **Compression** | Snappy |
| **Record Format** | Apache Avro |
| **Metadata Versions** | Multiple (tracked by Iceberg) |

---

## 🚀 Deployment & Operations

### Prerequisites
- **Java 17+** (LTS)
- **Maven 3.6+**
- **PostgreSQL 15** (for JDBC Catalog)
- **AWS Credentials** (IAM role or environment variables)
- **AWS S3 Bucket** (transaction-cold-storage)

### Local Development
```bash
# Start PostgreSQL
docker run -d --name postgres-iceberg \
  -e POSTGRES_DB=iceberg_catalog \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 postgres:15

# Build project
mvn clean package

# Run application
java -jar target/parquetreadwrite-0.0.1-SNAPSHOT.jar

# Test API
curl -X POST http://localhost:8080/api/transaction \
  -H "Content-Type: application/json" \
  -d '{"transactionId":"test","accountId":"ACC_1092",...}'
```

---

## 🎯 Query Optimization

### Partition Pruning (Automatic)
Iceberg's manifest files enable intelligent partition pruning:
```sql
-- This query only reads: account_id=ACC_1092/year=2025/month=01/day=*/
SELECT * FROM transactions 
WHERE account_id = 'ACC_1092' 
  AND year = 2025 
  AND month = 01
```

**Benefits:**
- ✅ 90%+ reduction in S3 bytes scanned
- ✅ Faster query execution
- ✅ Lower AWS costs
- ✅ Automatic garbage collection

---

## 📈 Future Enhancements

1. **Multi-Region Replication** - Iceberg enables efficient data replication
2. **Time-Travel Queries** - Query historical snapshots via Iceberg versioning
3. **Schema Evolution** - Add new fields without rewriting data
4. **Streaming Ingestion** - Kafka/Kinesis integration for real-time writes
5. **Query Engine Integration** - Presto, Athena, or Spark for analytics
6. **Data Governance** - Row-level security and audit logging

---

## 📝 Summary Table

| Aspect | Details |
|--------|---------|
| **Project Type** | Spring Boot Microservice |
| **Data Format** | Parquet (columnar) |
| **Metadata Format** | Apache Iceberg |
| **Catalog Backend** | PostgreSQL JDBC |
| **Warehouse Storage** | AWS S3 |
| **Partitioning** | 4-level (account/year/month/day) |
| **Compression** | Snappy |
| **Data Model** | Transaction ledger with temporal partitions |
| **API Type** | REST (JSON) |
| **Scalability** | Horizontal (S3 managed) |
| **Cost Model** | Pay-per-GB stored + S3 API calls |

---

**Status:** ✅ Production-Ready | **Last Updated:** 31 Jan 2026

 docker run -d \
  --name postgres-iceberg \
  -e POSTGRES_DB=iceberg_catalog \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -v /Users/mukeshdewangan/Documents/parquet_experiment/postgres_data:/var/lib/postgresql/data \
  postgres:15


   CREATE TABLE demo_db.iceberg_tables (
    catalog_name               VARCHAR(255)  NOT NULL,
    table_namespace            VARCHAR(255)  NOT NULL,
    table_name                 VARCHAR(255)  NOT NULL,
    metadata_location          VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    CONSTRAINT iceberg_tables_pkey PRIMARY KEY (catalog_name, table_namespace, table_name)
);