# dbt Capstone with BigQuery

Project nay xay dung pipeline mo hinh du lieu bang `dbt` tren `BigQuery` cho bo du lieu Instacart. Hien tai repo tap trung vao lop `staging`, chuan hoa du lieu tu cac bang raw va tao nen cac `view` de phuc vu cac buoc mo hinh hoa tiep theo.

## Muc tieu project

- Ket noi du lieu raw Instacart tren BigQuery vao dbt
- Xay dung lop `staging` de chuan hoa ten bang va quan ly transformation
- Khai bao `source`, `schema test` va mo ta model bang file YAML
- Chuan bi cau truc cho cac lop `raw_vault` va `business_vault` trong giai doan tiep theo

## Cong nghe su dung

- `dbt Core`
- `dbt-bigquery`
- `Google BigQuery`
- SQL + Jinja macro

## Cau truc thu muc

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

## Nguon du lieu

Project dang khai bao source `raw_instacart` trong `models/sources.yml`, gom cac bang:

- `aisles`
- `departments`
- `order_products_prior`
- `order_products_train`
- `orders`
- `products`

Trong pham vi hien tai, cac model staging da duoc tao cho:

- `stg_aisles`
- `stg_departments`
- `stg_orders`
- `stg_order_products_prior`
- `stg_products`

## Cach project dang hoat dong

Moi model trong thu muc `models/staging/instacart/` se:

1. Khai bao bang nguon can lay du lieu.
2. Goi macro `stage` trong `macros/stage.sql`.
3. Build thanh `view` trong schema staging theo cau hinh tai `dbt_project.yml`.

Schema staging hien dang duoc cau hinh la `stg_instacart`.

## Kiem thu du lieu

Project da khai bao schema test trong `models/staging/instacart/instacart_stg.yml` cho mot so khoa chinh, chu yeu:

- `not null`
- `unique`

Nhung test nay dang duoc ap dung cho cac cot dinh danh nhu:

- `aisle_id`
- `department_id`
- `order_id`
- `product_id`

## Cach chay project

### 1. Cai dat moi truong

Ban can co:

- Python
- `dbt-core`
- `dbt-bigquery`
- file `profiles.yml` voi profile ten `dbt_bigquery`
- quyen truy cap BigQuery va credentials hop le

Vi du cai dat:

```bash
pip install dbt-core dbt-bigquery
```

### 2. Kiem tra ket noi

```bash
dbt debug
```

### 3. Chay model staging

```bash
dbt run --select staging
```

### 4. Chay test

```bash
dbt test --select staging
```

### 5. Sinh tai lieu

```bash
dbt docs generate
dbt docs serve
```

## Tinh trang hien tai

- Da co `source` cho bo du lieu Instacart
- Da co cac model `staging` co ban tren BigQuery
- Da co YAML mo ta model va schema test
- Da co khung thu muc cho `raw_vault` va `business_vault`
- Chua co model fact/dimension hoac business layer hoan chinh trong repo

## Huong phat trien tiep theo

- Bo sung model cho `order_products_train` neu can su dung tap train
- Hoan thien `raw_vault` gom hub, link, satellite
- Xay dung `business_vault` hoac mart phan tich
- Bo sung them test quan he nhu `relationships`
- Tao tai lieu lineage day du bang `dbt docs`

## Lenh Git co san trong repo

File `commands.md` hien dang luu cac lenh khoi tao Git co ban:

```bash
git init
git add .
git commit -m "init dbt project"
```

## Ghi chu

README nay duoc viet dua tren trang thai hien tai cua repo. Neu ban tiep tuc xay dung them `raw_vault`, `business_vault` hoac data mart, phan mo ta kien truc va luong du lieu nen duoc cap nhat tiep de dong bo voi project.
