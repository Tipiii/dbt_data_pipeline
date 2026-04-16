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

# Kiểm tra version
dbt --version
databricks --version
```
---

## 4. Setup Guide

- [Databricks configurations](https://docs.getdbt.com/reference/resource-configs/databricks-configs?version=1.12)
- [Connect Databricks to dbt Core](https://docs.getdbt.com/docs/local/connect-data-platform/databricks-setup?version=1.12)

### Profile Setup

File `profiles.yml` đã có sẵn trong repo. Nó đọc thông tin kết nối từ environment variables.


### dbt Run
```bash
# Chạy theo source
dbt run --select tag:t24 --vars '{"target_date": "20250415", "run_mode": "daily"}'

# Chạy theo layer
dbt run --select raw_vault.*
dbt run --select business_vault.*
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

