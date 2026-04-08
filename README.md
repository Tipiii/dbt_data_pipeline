# dbt Capstone with BigQuery

Project này xây dựng pipeline mô hình dữ liệu bằng `dbt` trên `BigQuery` cho bộ dữ liệu Instacart. Hiện tại repo tập trung vào lớp `staging`, chuẩn hóa dữ liệu từ các bảng raw và tạo nên các `view` để phục vụ các bước mô hình hóa tiếp theo.

## Mục tiêu project

- Kết nối dữ liệu raw Instacart trên BigQuery vào dbt
- Xây dựng lớp `staging` để chuẩn hóa tên bảng và quản lý transformation
- Khai báo `source`, `schema test` và mô tả model bằng file YAML
- Chuẩn bị cấu trúc cho các lớp `raw_vault` và `business_vault` trong giai đoạn tiếp theo

## Công nghệ sử dụng

- `dbt Core`
- `dbt-bigquery`
- `Google BigQuery`
- SQL + Jinja macro

## Cấu trúc thư mục

```text
.
|-- analyses/
|-- macros/
|   `-- stage.sql
|-- models/
|   |-- sources.yml
|   |-- staging/
|   |   `-- instacart/
|   |       |-- instacart_stg.yml
|   |       |-- stg_aisles.sql
|   |       |-- stg_departments.sql
|   |       |-- stg_orders.sql
|   |       |-- stg_order_products_prior.sql
|   |       `-- stg_products.sql
|   |-- raw_vault/
|   |   |-- hub/
|   |   |-- link/
|   |   |-- sat/
|   |   `-- raw_vault.yml
|   `-- business_vault/
|-- seeds/
|-- snapshots/
|-- tests/
`-- dbt_project.yml
```

## Nguồn dữ liệu

Project đang khai báo source `raw_instacart` trong `models/sources.yml`, gồm các bảng:

- `aisles`
- `departments`
- `order_products_prior`
- `order_products_train`
- `orders`
- `products`

Trong phạm vi hiện tại, các model staging đã được tạo cho:

- `stg_aisles`
- `stg_departments`
- `stg_orders`
- `stg_order_products_prior`
- `stg_products`

## Cách project đang hoạt động

Mỗi model trong thư mục `models/staging/instacart/` sẽ:

1. Khai báo bảng nguồn cần lấy dữ liệu.
2. Gọi macro `stage` trong `macros/stage.sql`.
3. Build thành `view` trong schema staging theo cấu hình tại `dbt_project.yml`.

Schema staging hiện đang được cấu hình là `stg_instacart`.

## Kiểm thử dữ liệu

Project đã khai báo schema test trong `models/staging/instacart/instacart_stg.yml` cho một số khóa chính, chủ yếu:

- `not null`
- `unique`

Những test này đang được áp dụng cho các cột định danh như:

- `aisle_id`
- `department_id`
- `order_id`
- `product_id`

## Cách chạy project

### 1. Cài đặt môi trường

Bạn cần có:

- Python
- `dbt-core`
- `dbt-bigquery`
- file `profiles.yml` với profile tên `dbt_bigquery`
- quyền truy cập BigQuery và credentials hợp lệ

Ví dụ cài đặt:

```bash
pip install dbt-core dbt-bigquery
```

### 2. Kiểm tra kết nối

```bash
dbt debug
```

### 3. Chạy model staging

```bash
dbt run --select staging
```

### 4. Chạy test

```bash
dbt test --select staging
```

### 5. Sinh tài liệu

```bash
dbt docs generate
dbt docs serve
```

## Tình trạng hiện tại

- Đã có `source` cho bộ dữ liệu Instacart
- Đã có các model `staging` cơ bản trên BigQuery
- Đã có YAML mô tả model và schema test
- Đã có khung thư mục cho `raw_vault` và `business_vault`
- Chưa có model fact/dimension hoặc business layer hoàn chỉnh trong repo

## Hướng phát triển tiếp theo

- Bổ sung model cho `order_products_train` nếu cần sử dụng tập train
- Hoàn thiện `raw_vault` gồm hub, link, satellite
- Xây dựng `business_vault` hoặc mart phân tích
- Bổ sung thêm test quan hệ như `relationships`
- Tạo tài liệu lineage đầy đủ bằng `dbt docs`

## Lệnh Git có sẵn trong repo

File `commands.md` hiện đang lưu các lệnh khởi tạo Git cơ bản:

```bash
git init
git add .
git commit -m "init dbt project"
```

## Ghi chú

README này được viết dựa trên trạng thái hiện tại của repo. Nếu bạn tiếp tục xây dựng thêm `raw_vault`, `business_vault` hoặc data mart, phần mô tả kiến trúc và luồng dữ liệu nên được cập nhật tiếp để đồng bộ với project.
