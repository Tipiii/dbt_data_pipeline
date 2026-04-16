![OCB Logo](https://upload.wikimedia.org/wikipedia/vi/e/e5/Logo-Ngan_hang_Phuong_Dong.png)

![Profile Photo](https://media.licdn.com/dms/image/v2/C4D03AQGSUdBr9040Ww/profile-displayphoto-shrink_800_800/profile-displayphoto-shrink_800_800/0/1516577445013?e=1778112000&v=beta&t=46ezdKquRRNuunPiI9Kc3JCPdgYix-GcwiaG0WhJT0g)

![Raffles Logo](https://media.licdn.com/dms/image/v2/C560BAQHXMNx-QKI7Nw/company-logo_200_200/company-logo_200_200/0/1630657899648?e=2147483647&v=beta&t=OiX0gjlO85FQ_8F8n9MQUjNIKe-o6BjhcDFvxbDohHQ)

# OCB Data Vault 2.0 — dbt on Databricks

> **Project**: `datavaultmodel2` · **Org**: OCB (Ocean Commercial Bank)  
> **Stack**: dbt Core · Databricks (Unity Catalog) · Delta Lake · GitLab CI/CD

---

## 1. Project Overview

Data Warehouse chuẩn **Data Vault 2.0** cho OCB, tích hợp 6 hệ thống nguồn:

| Source | Domain |
|---|---|
| T24 | Core Banking (tài khoản, khoản vay, tiền gửi) |
| WAY4 | Cards (thẻ, giao dịch, billing) |
| Omni | Digital Banking (người dùng, thanh toán) |
| BPM | Workflow nghiệp vụ, hồ sơ, bảo hiểm |
| CRM | Lịch sử liên hệ khách hàng |
| Call Center | Log cuộc gọi |

---

## 2. Architecture 
```

| Layer | Materialization |
|---|---|
| Staging | `view` |
| Hub | `incremental (merge)` |
| Link | `incremental (merge)` |
| Satellite | `incremental (merge)` |
| PIT / Bridge | `incremental (merge)` |
```
---

## 3. Prerequisites

### Công cụ cần cài

```bash
# Python >= 3.10
python --version

# dbt với adapter Databricks
pip install dbt-databricks

# Databricks CLI (để deploy bundles)
pip install databricks-cli
# hoặc
brew install databricks  # macOS

# Kiểm tra version
dbt --version
databricks --version
```
---

## 4. Setup Guide

- [Databricks configurations](https://docs.getdbt.com/reference/resource-configs/databricks-configs?version=1.12)
- [Connect Databricks to dbt Core](https://docs.getdbt.com/docs/local/connect-data-platform/databricks-setup?version=1.12)

### Profile Setup

File `profiles.yml` đã có sẵn trong repo. Nó đọc thông tin kết nối từ environment variables (xem mục 3).

```yaml
# profiles.yml (đã có trong repo)
datavault-model:
  target: dev
  outputs:
    dev:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_SQL_WAREHOUSE_HTTP_PATH') }}"
      client_id: "{{ env_var('DATABRICKS_CLIENT_ID') }}"
      client_secret: "{{ env_var('DATABRICKS_CLIENT_SECRET') }}"
      catalog: "{{ env_var('DATABRICKS_DESTINATION_CATALOG') }}"
      schema: "{{ env_var('DATABRICKS_DESTINATION_SCHEMA') }}"
      threads: 20
```

### dbt Run
```bash
# Chạy theo source
dbt run --select tag:t24 --vars '{"target_date": "20250415", "run_mode": "daily"}'

# Chạy theo layer
dbt run --select raw_vault.*
dbt run --select business_vault.*
```


---

## 5. Backfill Guide

Mỗi model dùng `target_date` (format: `YYYYMMDD`) để lọc theo ngày. Incremental merge đảm bảo idempotent — chạy lại cùng ngày vẫn an toàn.

```bash
# Backfill
dbt run --vars '{"target_date": "20250101", "run_mode": "backfill"}' --target dev
```

---

## 6. Troubleshooting

- [Troubleshoot the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/troubleshooting)

---

## 7. Documentation

Tài liệu tham khảo:

- [dbt Core (phiên bản 1.12)](https://docs.getdbt.com/docs/introduction?version=1.12)
- [Data Vault 2.0](https://datafinder.ru/files/downloads/01/Building-a-Scalable-Data-Warehouse-with-Data-Vault-2.0.pdf)
- [GitHub: dbt Databricks adapter](https://github.com/databricks/dbt-databricks)

