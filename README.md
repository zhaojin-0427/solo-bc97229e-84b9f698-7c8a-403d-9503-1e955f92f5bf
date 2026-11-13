# 鸟类监测数据湖（Flask + PyArrow + SQLite）

把各监测站逐年新增鸟种的调查 CSV 入湖：每批一个独立 Parquet 文件，目录库（SQLite）
按列名合并出统一的逻辑结构，研究员可按站点预览 / 下载统一结构的 CSV。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

可选：灌入示例数据（含一次「uint64 与 text 冲突 → 提升为 text」的演示）

```bash
docker compose exec web python scripts/seed_demo.py
```

运行端到端自检（解析校验 / 结构合并 / 类型提升 / 并发 / 重启持久化 / Web 流程）：

```bash
docker compose exec web python scripts/selftest.py
```

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATA_DIR=./data
flask --app app.web run        # 或 gunicorn -b 0.0.0.0:8000 app.web:app
```

## 数据格式与入湖流程

1. **上传**：每批填写站点 + 选 CSV。CSV 首列为观测日（`YYYY-MM-DD`），其余每列一种鸟；
   列名去空白后非空且不得重复；空单元格一律按 NULL 处理。
2. **逐列声明类型**：每个鸟种列声明为 `uint64`（非负 64 位整数，0 ~ 2^64-1）或 `text`。
   首列观测日归一化为固定列名 `obs_date`（date 类型），鸟种列不得使用该保留名。
3. **发布**：
   - 解析校验（坏整数、坏日期、字段数不齐 → **整批不发布**）；
   - 写 `parquet/<批次UUID>.parquet`：先写临时文件 + fsync，再原子 rename，**既有文件永不重写**；
   - 最后才开启**短暂的**目录库写事务：合并逻辑结构 + 登记批次。
   解析和写文件期间**不持有**目录库写事务；登记失败会删掉未发布的文件，**目录永不引用残件**。

## 逻辑结构合并规则

- 按列名合并；批次缺列在读出时补 NULL；新鸟种自动加列；
- 同鸟种类型冲突（一边 `uint64` 一边 `text`）→ 提升为 `text`；
- 提升后**不得退回**整数（`text` 是吸收态）；旧整数读出时按**十进制**转成文本；
- 并发上传新增不同鸟种、或对同列并发提升：发布事务经 `BEGIN IMMEDIATE` 串行化，
  结构变化与各批记录都完整保留（自检第 3 项覆盖）。

## 一致性读取

预览 / 下载在同**一个 SQLite 读事务**内取「逻辑结构 + 文件清单」快照（WAL 模式），
再按该快照读取不可变的 Parquet 文件——一次取数始终使用同一状态的结构与清单。
页面上的 `schema v<N>` 每次结构变化都会递增。

## 持久化与重启

`DATA_DIR`（Docker 卷 `lake_data`）内含 `catalog.db`（WAL）与全部 Parquet 文件。
重启后直接可用；启动时清理暂存区与崩溃遗留的临时文件，未被目录引用的孤儿
Parquet 过了宽限期才删除（不影响在途发布）。

## 目录结构

```
app/
  catalog.py    # SQLite 目录库：逻辑结构 + 批次清单，读写事务
  csvparse.py   # CSV 解析与校验（纯标准库）
  store.py      # PyArrow：批次表构建、原子写 Parquet、统一结构读取、CSV 导出
  service.py    # 入湖编排：暂存 -> 校验 -> 写文件 -> 短事务登记；启动清理
  web.py        # Flask 路由：上传/声明/发布/预览/下载
  templates/    # 页面
scripts/
  selftest.py   # 端到端自检
  seed_demo.py  # 示例数据灌入
samples/        # 示例 CSV
```

## 主要 HTTP 入口

| 路由 | 说明 |
| --- | --- |
| `GET /` | 总览：逻辑结构、站点、批次 |
| `GET/POST /upload` | 上传 CSV（第一步） |
| `POST /publish` | 声明类型后发布（第二步） |
| `GET /preview?station=&limit=` | 按站点预览统一结构 |
| `GET /download.csv?station=` | 下载统一结构 CSV（可按站点过滤） |
| `GET /healthz` | 健康检查 |
