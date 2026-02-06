# Orders ETL Service - Technical Documentation

**Repository:** `orders-etl-service`
**Runtime:** Node.js (JavaScript, ES6+)
**Branch:** `main`

---

## Project Structure

```
orders-etl-service/
├── index.js                          # Main entry point and CLI handler
├── package.json                      # Dependencies and npm scripts
├── .env.example                      # Environment variable template
├── .gitignore                        # Git ignore rules
├── README.md                         # Basic project readme
├── logs/                             # Local log output directory
│
└── src/
    ├── config/
    │   └── index.js                  # Centralized configuration loader
    │
    ├── core/
    │   ├── index.js                  # Core module exports
    │   ├── BasePipeline.js           # Abstract base class for all pipelines
    │   ├── DatabaseConnection.js     # PostgreSQL connection pool manager
    │   └── S3Handler.js              # AWS S3 operations handler
    │
    ├── etl/
    │   └── Orchestrator.js           # Central coordinator for pipeline execution
    │
    ├── extractors/                   # External API data extractors
    │   ├── index.js                  # Extractor registry
    │   ├── salesforce/               # Salesforce extractor
    │   │   ├── index.js              # Main Salesforce extractor class
    │   │   ├── SalesforceClient.js   # Salesforce API client (JWT auth)
    │   │   └── queries/              # SOQL query configurations
    │   │       ├── dental-groups.js  # Dental groups query + mapping
    │   │       └── dental-practices.js # Dental practices query + mapping
    │   └── magictouch/               # MagicTouch extractor
    │       ├── index.js              # Main MagicTouch extractor class
    │       └── MagicTouchClient.js   # MagicTouch API client
    │
    ├── pipelines/
    │   ├── index.js                  # Auto-discovery pipeline registry
    │   ├── _template/                # Template for creating new pipelines
    │   │   └── index.js
    │   ├── orders/                   # Orders pipeline (staging + merge)
    │   │   └── index.js
    │   ├── product-catalog/          # Product catalog reference data
    │   │   └── index.js
    │   ├── dental-practices/         # Dental practices reference data
    │   │   └── index.js
    │   ├── dental-groups/            # Dental groups reference data
    │   │   └── index.js
    │   ├── lab-product-mapping/      # Lab-to-product mapping
    │   │   └── index.js
    │   ├── lab-practice-mapping/     # Lab-to-practice mapping
    │   │   └── index.js
    │   ├── product-lab-markup/       # Product pricing/markup by lab
    │   │   └── index.js
    │   └── product-lab-rev-share/    # Revenue share schedules
    │       └── index.js
    │
    └── utils/
        ├── logger.js                 # Winston logging configuration
        └── hash.js                   # MD5 hash utility for deduplication
```

---

## 1. ETL Overview

### Purpose of the Pipeline

- Extracts CSV files from AWS S3 bucket folders
- Transforms and validates rows according to pipeline-specific rules
- Loads validated data into PostgreSQL database tables
- Produces audit logs (success/error) back to S3 for traceability

### Business Use Case

- Serves the dental industry vertical
- Processes dental lab order transactions from upstream lab management systems
- Maintains reference data for products, practices, groups, and pricing
- Primary entity: **orders** (dental lab case orders with patient, product, and shipping details)
- Supporting entities: product catalogs, dental practices/groups, lab mappings, pricing schedules

---

## 2. Data Sources

### Source Systems

- **AWS S3** — single bucket with multiple prefixes (folders), one per pipeline
- CSV files are deposited by an external upstream system (not part of this repo)
- Only files ending in `.csv` are processed
- Sub-folders `processed/` and `logs/` are excluded from processing

### S3 Folder Structure (per pipeline)

```
s3://<bucket>/<SOURCEPATH>/
├── file1.csv                    ← Picked up for processing
├── file2.csv
├── processed/                   ← Valid-row CSVs land here after processing
│   └── 2025-01-15T10-30-00-000Z_file1.csv
└── logs/                        ← Audit log CSVs land here after processing
    └── file1_log_2025-01-15T10-30-00-000Z.csv
```

### Data Extraction Method

- `S3Handler.listFiles()` paginates through `ListObjectsV2` for each source prefix
- Filters out folder markers, `processed/`, `logs/`, and non-CSV files
- Each file is retrieved via `GetObjectCommand` as a stream
- Stream is piped into `csv-parser` library for parsing

### Pipeline-to-S3-Prefix Mapping

- **orders** → env var `SOURCEPATH` (default: `dev_orders`)
- **product-catalog** → env var `PRODUCT_CATALOG_SOURCEPATH` (default: `dev_product_catalog`)
- **dental-practices** → env var `DENTAL_PRACTICES_SOURCEPATH` (default: `dev_dental_practices`)
- **dental-groups** → env var `DENTAL_GROUPS_SOURCEPATH` (default: `dev_dental_groups`)
- **lab-product-mapping** → env var `LAB_PRODUCT_MAPPING_SOURCEPATH` (default: `dev_lab_product_mapping`)
- **lab-practice-mapping** → env var `LAB_PRACTICE_MAPPING_SOURCEPATH` (default: `dev_lab_practice_mapping`)
- **product-lab-markup** → env var `PRODUCT_LAB_MARKUP_SOURCEPATH` (default: `dev_product_lab_markup`)
- **product-lab-rev-share** → env var `PRODUCT_LAB_REV_SHARE_SOURCEPATH` (default: `dev_product_lab_rev_share`)

---

## 2.5 External API Extractors

In addition to processing CSV files from S3, the service includes **extractors** that pull data directly from external APIs and upload CSVs to S3 for subsequent pipeline processing.

### Extractor Architecture

```
External API → Extractor → CSV → S3 (source/) → Pipeline → PostgreSQL
```

Extractors fetch data from external systems, convert to CSV format matching pipeline expectations, and upload to the appropriate S3 source folder. The standard ETL pipelines then process these CSVs.

### Salesforce Extractor (`src/extractors/salesforce/`)

**Purpose:** Extract dental groups and practices from Salesforce CRM

**Authentication:** JWT Bearer flow using RSA private key

**Available Extractors:**
- `dental-groups` — Fetches Account records with Corporate_ID__c
- `dental-practices` — Fetches Account records that belong to a parent group

**Data Flow:**
1. Authenticate to Salesforce using JWT
2. Execute SOQL query
3. Map Salesforce records to CSV format (matching pipeline expectations)
4. Upload CSV to S3 source folder

**Salesforce Configuration (Environment Variables):**
- `SF_LOGIN_URL` — Salesforce login URL (e.g., `https://login.salesforce.com`)
- `SF_CLIENT_ID` — Connected App consumer key
- `SF_USERNAME` — Salesforce username
- `SF_PRIVATE_KEY` — RSA private key content (with `\n` for newlines)
- `SF_PRIVATE_KEY_PATH` — Alternative: path to private key file
- `SF_API_VERSION` — API version (default: `59.0`)

**Query Configurations:**
- See `src/extractors/salesforce/queries/dental-groups.js`
- See `src/extractors/salesforce/queries/dental-practices.js`

### MagicTouch Extractor (`src/extractors/magictouch/`)

**Purpose:** Extract dental lab orders from MagicTouch lab management system

**Authentication:** Token-based (username/password)

**Available Extractors:**
- `orders` — Fetches cases and flattens by product line

**Data Flow:**
1. Authenticate to MagicTouch API
2. Fetch cases (filtered by customer type "Incisive")
3. Fetch customers for phone number lookup
4. Flatten cases by product (one row per product)
5. Upload CSV to S3 orders source folder

**Export Modes:**
- `INC` (Incremental) — Fetch cases modified in last 7 days
- `FULL` — Fetch all cases

**MagicTouch Configuration (Environment Variables):**
- `MAGICTOUCH_BASE_URL` — API base URL
- `MAGICTOUCH_USER_ID` — API user ID
- `MAGICTOUCH_PASSWORD` — API password
- `EXPORT_MODE` — `INC` or `FULL` (default: `INC`)

**Output CSV Columns:**
- Maps to orders pipeline expected columns: `labid`, `submissiondate`, `shippingdate`, `casedate`, `caseid`, `productid`, `productdescription`, `quantity`, `productprice`, `patientname`, `customerid`, `customername`, `address`, `phonenumber`, `casestatus`, etc.
- `labid` is hardcoded to `2` for MagicTouch orders

### Extractor Usage

Extractors are not exposed via CLI. Use programmatically:
```javascript
const { SalesforceExtractor, MagicTouchExtractor } = require('./src/extractors');
await sfExtractor.extract('dental-groups');  // Salesforce
await mtExtractor.extract();                  // MagicTouch
```

---

## 3. Transformations

### Common Processing Flow (BasePipeline)

All pipelines inherit this sequence from `src/core/BasePipeline.js`:

- **Parse** — Stream CSV through `csv-parser` into array of row objects
- **Normalize** — Lowercase all column headers, strip non-alphanumeric characters
  - Example: `CaseId` → `caseid`, `Lab Product ID` → `labproductid`
- **Map** — Pipeline-specific `mapRow()` transforms normalized keys to DB column names
- **Validate** — Check that all `requiredFields` are present and non-empty
- **Insert** — Each row inserted with PostgreSQL `SAVEPOINT` for individual error recovery
- **Post-process** — Optional hook (only orders pipeline uses this to call stored procedure)

### Pipeline Summary

- **orders** → `orders_stage` — Required: submissiondate, casedate, caseid, productid, quantity, customerid — Truncate + merge
- **product-catalog** → `incisive_product_catalog` — Required: incisive_id, incisive_name, category
- **dental-practices** → `dental_practices` — Required: practice_id, dental_group_id
- **dental-groups** → `dental_groups` — Required: dental_group_id, name
- **lab-product-mapping** → `lab_product_mapping` — Required: lab_id, lab_product_id, incisive_product_id
- **lab-practice-mapping** → `lab_practice_mapping` — Required: lab_id, practice_id, lab_practice_id
- **product-lab-markup** → `product_lab_markup` — Required: lab_id, lab_product_id
- **product-lab-rev-share** → `product_lab_rev_share` — Required: lab_id, lab_product_id, fee_schedule_name

**Note:** All pipelines except `orders` use `ON CONFLICT DO NOTHING`. The `orders` pipeline truncates staging, inserts all rows, then calls `merge_orders_stage()` stored procedure.

### Validation and Cleansing Rules

- **Required field validation** — Each pipeline declares mandatory columns; rows missing any are rejected
- **Type coercion** — parseInt/parseFloat applied where specified; NaN values pass through (no explicit guard)
- **Duplicate handling** — Reference data pipelines use `ON CONFLICT DO NOTHING`; orders pipeline uses `row_hash` + merge procedure
- **Empty string handling** — Treated as missing for required field validation

---

## 4. Data Destinations

### Target Database

- **Engine:** PostgreSQL
- **Schema:** `etl` (with fallback to `public` via `search_path=etl,public`)
- **Connection:** `pg.Pool` with configurable pool settings

### Database Connection Settings

- Max connections: 10 (configurable via `DB_POOL_MAX`)
- Idle timeout: 30 seconds (configurable via `DB_IDLE_TIMEOUT`)
- Connect timeout: 2 seconds (configurable via `DB_CONNECT_TIMEOUT`)
- SSL: Optional (configurable via `DB_SSL`)

### Load Strategies

- **orders:** Truncate `orders_stage` → insert all → call `merge_orders_stage()`
- **All others:** Insert with `ON CONFLICT DO NOTHING` (upsert behavior)

### Stored Procedures

- **`merge_orders_stage()`** — Called after orders pipeline inserts into staging
  - Merges staging data into final orders table
  - Procedure definition is in PostgreSQL, NOT in this repository
  - Must be inspected directly in the database

### Schema Notes

- DDL/migration scripts are NOT included in this repository
- Table schemas must be obtained from the database directly
- The `orders_stage` table has 31 columns including ETL-added fields (`source_file_key`, `row_hash`)

---

## 5. Scheduling & Execution

### How Jobs Run

- **No built-in scheduler** — This is a single-run CLI process
- Executes, processes all available CSV files, then exits
- External scheduling required (cron, CloudWatch Events, manual trigger)

### CLI Commands

```bash
node index.js              # Process all pipelines
node index.js <name>       # Process specific pipeline (e.g., orders, dental-groups)
node index.js list         # List available pipelines
```

### Execution Characteristics

- Pipelines and files processed sequentially
- Each file in single PostgreSQL transaction
- Exit code: 0 (success) or 1 (fatal error)
- **No built-in automation** — external scheduling required (cron, CloudWatch, etc.)

---

## 6. Configuration

### Environment Variables

**AWS / S3 Configuration**
- `AWS_REGION` — AWS region (default: `us-east-1`)
- `S3_BUCKET` — S3 bucket name (required)
- `AWS_ACCESS_KEY_ID` — AWS access key (optional if using IAM role)
- `AWS_SECRET_ACCESS_KEY` — AWS secret key (optional if using IAM role)

**Database Configuration**
- `DB_HOST` — PostgreSQL host (default: `localhost`)
- `DB_PORT` — PostgreSQL port (default: `5432`)
- `DB_USER` — Database username (required)
- `DB_PASSWORD` — Database password (required)
- `DB_NAME` — Database name (default: `postgres`)
- `DB_SSL` — Enable SSL connection (default: `false`)
- `DB_POOL_MAX` — Max pool connections (default: `10`)
- `DB_IDLE_TIMEOUT` — Idle timeout in ms (default: `30000`)
- `DB_CONNECT_TIMEOUT` — Connect timeout in ms (default: `2000`)

**Pipeline S3 Paths**
- `SOURCEPATH` — Orders pipeline S3 prefix
- `PRODUCT_CATALOG_SOURCEPATH` — Product catalog S3 prefix
- `DENTAL_PRACTICES_SOURCEPATH` — Dental practices S3 prefix
- `DENTAL_GROUPS_SOURCEPATH` — Dental groups S3 prefix
- `LAB_PRODUCT_MAPPING_SOURCEPATH` — Lab product mapping S3 prefix
- `LAB_PRACTICE_MAPPING_SOURCEPATH` — Lab practice mapping S3 prefix
- `PRODUCT_LAB_MARKUP_SOURCEPATH` — Product lab markup S3 prefix
- `PRODUCT_LAB_REV_SHARE_SOURCEPATH` — Product lab rev share S3 prefix

**Logging Configuration**
- `LOG_LEVEL` — Winston log level (default: `info`)
- `LOG_TO_CONSOLE` — Enable console output (default: `true`)
- `BATCH_SIZE` — Rows per processing batch (default: `100`)

**Salesforce Extractor Configuration**
- `SF_LOGIN_URL` — Salesforce login URL (e.g., `https://login.salesforce.com`)
- `SF_CLIENT_ID` — Connected App consumer key
- `SF_USERNAME` — Salesforce username for JWT auth
- `SF_PRIVATE_KEY` — RSA private key content (use `\n` for newlines in env var)
- `SF_PRIVATE_KEY_PATH` — Alternative: file path to private key
- `SF_API_VERSION` — Salesforce API version (default: `59.0`)

**MagicTouch Extractor Configuration**
- `MAGICTOUCH_BASE_URL` — MagicTouch API base URL
- `MAGICTOUCH_USER_ID` — API user ID
- `MAGICTOUCH_PASSWORD` — API password
- `EXPORT_MODE` — `INC` (last 7 days) or `FULL` (all records)

### Credentials Handling

- **Database credentials:** Plaintext in `.env` file (file is in `.gitignore`)
- **AWS credentials:** Either plaintext in `.env` OR via IAM role/instance profile
  - S3Handler checks for explicit credentials first
  - Falls back to AWS default credential chain if not provided
- **Salesforce credentials:** JWT authentication using RSA private key
  - Private key can be provided as content (`SF_PRIVATE_KEY`) or file path (`SF_PRIVATE_KEY_PATH`)
  - Requires Connected App configuration in Salesforce
- **MagicTouch credentials:** Username/password in `.env` file
- **No secrets manager integration** — Must be handled at infrastructure level

---

## 7. Deployment

### Current State

- No Dockerfiles in repository
- No CI/CD pipeline configuration
- No infrastructure-as-code (Terraform, CloudFormation)
- No deployment scripts
- Deployment appears to be manual

### Setup Steps

1. Clone repo, run `npm install`
2. Copy `.env.example` to `.env`, configure values
3. Create PostgreSQL `etl` schema and target tables (DDL not in repo)
4. Deploy `merge_orders_stage()` stored procedure
5. Ensure S3 bucket exists with proper IAM permissions
6. Run: `npm start` (production) or `npm run dev` (with nodemon)

### DEV vs PROD

- Controlled by `.env` (different S3 prefixes, DB hosts, credentials)
- No `NODE_ENV` checks in code

---

## 8. Failure Handling & Monitoring

### Retry Logic

- **No automatic retry logic exists**
- If a file fails, it remains in the S3 source folder
- Original file deletion is **commented out** (`Orchestrator.js:267`)
- De facto retry: next run will re-process the same files

### Transaction Behavior

- Each CSV file processed within a single `BEGIN`/`COMMIT` transaction
- Each row insert uses `SAVEPOINT` for individual error recovery
- Failed row: savepoint rolls back, error recorded, transaction continues
- Failed `postProcess` (e.g., merge procedure): entire transaction rolls back
- Unhandled exception during processing: entire transaction rolls back

### Error Reporting to S3

- `processed/{timestamp}_{filename}.csv` — Valid rows only
- `logs/{baseName}_log_{timestamp}.csv` — All rows with `etl_status`, `etl_reason`, `missingFields` columns

### Local Logging

- Winston JSON logs: `logs/combined.log`, `logs/error.log`
- Pipeline-specific: `logs/<pipeline>/combined.log`, `logs/<pipeline>/error.log`
- Console output enabled when `LOG_TO_CONSOLE=true`

### Alerts

- **No alerting system** — Must be built externally (monitor logs, S3 folders, exit codes)

---

## 9. Operational Playbook

### How to Re-Run Jobs

**Re-run all pipelines:**
```bash
node index.js
# or
node index.js all
```

**Re-run a specific pipeline:**
```bash
node index.js orders
node index.js dental-groups
node index.js product-catalog
```

**Re-process a specific file:**
1. Ensure the file exists in the S3 source folder (`s3://<bucket>/<prefix>/`)
2. Remove other files if you want to isolate the run
3. Run: `node index.js <pipeline-name>`

**Note:** Source file deletion is disabled — files remain after processing. Manual cleanup needed to prevent re-processing.

### How to Fix Failed Pipelines

**Validation errors:** Check S3 `logs/*_log_*.csv`, filter `etl_status = 'error'`, review `etl_reason`, fix source data, re-run.

**Database errors:** Check `logs/<pipeline>/error.log`, review constraint violations in `etl_reason`, fix data/schema, re-run.

**Stored procedure failure (orders):** Check `logs/orders/error.log`, inspect `merge_orders_stage()` in PostgreSQL, fix and re-run.

**Connectivity issues:** Verify `.env` settings (DB_*, S3_BUCKET, AWS_*), test connections manually.

### How to Add a New Pipeline

1. Copy `src/pipelines/_template/` to `src/pipelines/<new-name>/`
2. Implement required methods: `name`, `tableName`, `requiredFields`, `envKey`, `mapRow()`, `buildInsertQuery()`
3. Add env var to `.env` (e.g., `NEW_PIPELINE_SOURCEPATH=dev_new_pipeline`)
4. Create target table in PostgreSQL
5. Restart — auto-registers via filesystem discovery

### Key Operational Caveats

- **Source file deletion disabled** — Files remain after processing (`Orchestrator.js:267`); manual cleanup needed
- **No test suite** — Validate changes manually in dev environment
- **No database migrations** — Schema changes applied manually
- **`merge_orders_stage()` external** — Stored procedure not in repo; inspect database directly

---

## Appendix: Dependencies

**Production Dependencies**
- `@aws-sdk/client-s3` (^3.450.0) — AWS S3 API client
- `axios` (^1.6.0) — HTTP client for Salesforce API
- `csv-parser` (^3.0.0) — Streaming CSV parser
- `dotenv` (^16.3.1) — Environment variable loader
- `jsonwebtoken` (^9.0.0) — JWT signing for Salesforce auth
- `pg` (^8.11.3) — PostgreSQL client and connection pool
- `winston` (^3.11.0) — Structured logging

**Development Dependencies**
- `nodemon` (^3.0.2) — Auto-restart on file changes
