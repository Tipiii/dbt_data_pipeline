# dbt Capstone — Data Vault 2.0 trên BigQuery

Project xây dựng pipeline mô hình dữ liệu theo chuẩn **Data Vault 2.0** bằng `dbt` trên `Google BigQuery` cho bộ dữ liệu Instacart.

## Kiến trúc

```
Raw (BigQuery) → Staging (View) → Raw Vault (Hub / Link / Satellite) → Marts (Dim / Fact)
```

| Layer | Schema BigQuery | Materialization | Mô tả |
|---|---|---|---|
| **Staging** | `stg_instacart` | view | Chuẩn hóa dữ liệu raw, tính hash key và load timestamp |
| **Raw Vault** | `raw_vault` | table | Hub, Link, Satellite theo Data Vault 2.0 |
| **Marts** | `marts` | table | Dimension và Fact table phục vụ phân tích |

---

## Công nghệ sử dụng

- `dbt Core`
- `dbt-bigquery`
- `Google BigQuery`
- SQL + Jinja2 macro
- `Kestra` (orchestration)

---

## Cấu trúc thư mục

```text
.
├── macros/
│   ├── hash.sql                    # hash_columns(), hashdiff_columns()
│   ├── hub.sql                     # build_hub()
│   ├── link.sql                    # build_link()
│   ├── sat.sql                     # build_sat()
│   ├── stage.sql                   # stage()
│   └── raw_vault_helpers.sql       # utility helpers
├── models/
│   ├── sources.yml
│   ├── staging/
│   │   └── instacart/
│   │       ├── instacart_stg.yml
│   │       ├── stg_aisles.sql
│   │       ├── stg_departments.sql
│   │       ├── stg_orders.sql
│   │       ├── stg_order_products_prior.sql
│   │       ├── stg_order_products_train.sql
│   │       └── stg_products.sql
│   ├── raw_vault/
│   │   ├── raw_vault.yml
│   │   ├── hub/
│   │   │   ├── hub_aisle.sql
│   │   │   ├── hub_department.sql
│   │   │   ├── hub_order.sql
│   │   │   ├── hub_product.sql
│   │   │   └── hub_user.sql
│   │   ├── link/
│   │   │   ├── link_order_product.sql
│   │   │   ├── link_order_user.sql
│   │   │   ├── link_product_aisle.sql
│   │   │   └── link_product_department.sql
│   │   └── sat/
│   │       ├── sat_aisle_details.sql
│   │       ├── sat_department_details.sql
│   │       ├── sat_order_details.sql
│   │       ├── sat_order_product_details.sql
│   │       └── sat_product_details.sql
│   └── marts/
│       ├── marts.yml
│       ├── dim_aisles.sql
│       ├── dim_departments.sql
│       ├── dim_orders.sql
│       ├── dim_products.sql
│       ├── dim_users.sql
│       └── fct_order_products.sql
├── kestra/
│   ├── dbt_bigquery_staging.yml
│   ├── dbt_bigquery_raw_vault.yml
│   └── dbt_bigquery_mart.yml
├── seeds/
├── snapshots/
├── tests/
└── dbt_project.yml
```

---

## Nguồn dữ liệu

Source `raw_instacart` trong `models/sources.yml`, dataset `dbt_dev_toni` trên BigQuery:

| Bảng | Mô tả |
|---|---|
| `aisles` | Danh mục quầy hàng |
| `departments` | Danh mục ngành hàng |
| `orders` | Đơn hàng của khách hàng |
| `order_products_prior` | Chi tiết sản phẩm trong đơn prior |
| `order_products_train` | Chi tiết sản phẩm trong đơn train |
| `products` | Thông tin sản phẩm |

---

## Macros

Project sử dụng macro Jinja2 để tái sử dụng logic Data Vault:

| Macro | Mô tả |
|---|---|
| `stage()` | Tạo staging view với hash key và load timestamp |
| `build_hub()` | Tạo Hub table với deduplication và source tracking |
| `build_link()` | Tạo Link table với các hub key |
| `build_sat()` | Tạo Satellite table với hashdiff và effective dating |
| `hash_columns()` | MD5 hash một hoặc nhiều cột |
| `hashdiff_columns()` | Tạo hashdiff từ danh sách cột |

---

## Kiểm thử dữ liệu

Toàn bộ các layer đều có schema test trong file YAML:

**Staging** (`instacart_stg.yml`): `not_null`, `unique` trên các khóa chính (`aisle_id`, `department_id`, `order_id`, `product_id`)

**Raw Vault** (`raw_vault.yml`): `not_null`, `unique` trên hash key (`hk_*`) và business key; kiểm tra `relationships` giữa Link và Hub

**Marts** (`marts.yml`): `not_null`, `unique`, `relationships`, `accepted_values` trên toàn bộ dimension và fact table

---

## Cách chạy

### 1. Cài đặt

```bash
pip install dbt-core dbt-bigquery
```

### 2. Kiểm tra kết nối

```bash
dbt debug
```

### 3. Chạy theo layer

```bash
# Staging
dbt run --select staging
dbt test --select staging

# Raw Vault
dbt run --select raw_vault
dbt test --select raw_vault

# Marts
dbt run --select marts
dbt test --select marts
```

### 4. Sinh tài liệu

```bash
dbt docs generate
dbt docs serve
```

---

## Orchestration (Kestra)

Có 3 flow Kestra tương ứng 3 layer, mỗi flow clone repo từ GitHub, chạy Docker container `python:3.11-slim`, cài dbt rồi thực thi `dbt run` và `dbt test`:

| File | Layer |
|---|---|
| `kestra/dbt_bigquery_staging.yml` | Staging |
| `kestra/dbt_bigquery_raw_vault.yml` | Raw Vault |
| `kestra/dbt_bigquery_mart.yml` | Marts |

Credentials BigQuery được inject qua Kestra secret `GCP_SERVICE_ACCOUNT_JSON`.

---

## Tình trạng hiện tại

- Staging: hoàn chỉnh (6 model, có test)
- Raw Vault: hoàn chỉnh (5 hub, 4 link, 5 satellite, có test)
- Marts: hoàn chỉnh (5 dim, 1 fact, có test)
- Business Vault: chưa có model
