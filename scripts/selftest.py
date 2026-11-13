#!/usr/bin/env python3
"""端到端自检：CSV 校验、发布、结构合并、类型提升、并发、重启持久化、Web 入口、
重复调查索引/补建/核查标记。

用法（在仓库根目录，容器内或装好依赖的环境）：
    python scripts/selftest.py
"""
import io
import os
import re
import sqlite3
import sys
import tempfile
import threading
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import backfill, catalog, csvparse, service, store  # noqa: E402
from app.config import Config  # noqa: E402


def check(name, cond, extra=""):
    if cond:
        print(f"  ✓ {name}")
    else:
        raise AssertionError(f"{name}  {extra}")


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def make_cfg(tmp):
    cfg = Config(tmp)
    service.init_storage(cfg)
    return cfg


def make_cfg_manual_backfill(tmp):
    """关闭自动补建线程的存储（用于精确控制补建时机）。"""
    cfg = Config(tmp)
    cfg.dup_backfill_enabled = False
    service.init_storage(cfg)
    return cfg


def dup_status(cfg):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.get_dup_status(conn)
    finally:
        conn.close()


def snap_of(cfg, station=None):
    conn = catalog.connect(cfg.db_path)
    try:
        return catalog.snapshot(conn, station)
    finally:
        conn.close()


# ---------- 1. CSV 解析与校验 ----------

def test_parser(tmp):
    p = os.path.join(tmp, "a.csv")
    write(p, "日期,麻雀,白鹭\n2024-01-01,3,x y\n2024-01-02,,5\n")
    header = csvparse.read_header(p)
    check("表头解析", header == ["日期", "麻雀", "白鹭"])
    dates, cols = csvparse.parse_rows(p, ["uint64", "text"])
    check("日期解析", str(dates[0]) == "2024-01-01")
    check("空单元格为NULL(uint64)", cols[0] == [3, None])
    check("文本列保留原文", cols[1] == ["x y", "5"])

    def expect_csv_error(name, content, types=("uint64",)):
        write(p, content)
        try:
            csvparse.parse_rows(p, list(types))
        except csvparse.CsvError:
            print(f"  ✓ {name}")
        else:
            raise AssertionError(name + " 未被拒绝")

    expect_csv_error("负数拒绝", "d,麻雀\n2024-01-01,-3\n")
    expect_csv_error("非数字拒绝", "d,麻雀\n2024-01-01,12x\n")
    expect_csv_error("超出uint64拒绝", "d,麻雀\n2024-01-01,18446744073709551616\n")
    expect_csv_error("坏日期拒绝", "d,麻雀\n2024-13-01,1\n")
    expect_csv_error("字段数不一致拒绝", "d,麻雀\n2024-01-01,1,2\n")
    expect_csv_error("无数据行拒绝", "d,麻雀\n")

    write(p, "d,麻雀\n2024-01-01,18446744073709551615\n")
    _, cols = csvparse.parse_rows(p, ["uint64"])
    check("uint64上限可用", cols[0] == [2 ** 64 - 1])

    write(p, "d,麻雀,麻雀\n2024-01-01,1,2\n")
    try:
        csvparse.read_header(p)
        raise AssertionError("重复列名未被拒绝")
    except csvparse.CsvError:
        print("  ✓ 重复列名拒绝")


# ---------- 2. 发布、结构合并、类型提升 ----------

def test_publish_merge(tmp):
    cfg = make_cfg(tmp)
    f1 = os.path.join(tmp, "s1.csv")
    f2 = os.path.join(tmp, "s2.csv")
    f3 = os.path.join(tmp, "s3.csv")
    write(f1, "日期,麻雀,白鹭\n2023-05-01,12,3\n2023-05-02,9,\n")
    write(f2, "日期,麻雀,苍鹭\n2024-04-20,7,1\n")
    write(f3, "日期,白鹭,黑水鸡\n2023-05-01,约12,5\n")
    r1 = service.publish(cfg, "青湾", "s1.csv", f1, ["uint64", "uint64"])
    r2 = service.publish(cfg, "青湾", "s2.csv", f2, ["uint64", "uint64"])
    r3 = service.publish(cfg, "芦苇荡", "s3.csv", f3, ["text", "uint64"])
    check("新列加入", any(c["change"] == "added" and c["name"] == "苍鹭" for c in r2["changes"]))
    check("同名异型提升text", any(c["change"] == "promoted" and c["name"] == "白鹭" for c in r3["changes"]))
    check("每批独立Parquet", len({r["batch_id"] for r in (r1, r2, r3)}) == 3
          and len(os.listdir(cfg.parquet_dir)) == 3)

    snap_all = snap_of(cfg)
    snap_qw = snap_of(cfg, "青湾")
    types = {c["name"]: c["type"] for c in snap_all["columns"]}
    check("逻辑结构按列名合并", types == {"麻雀": "uint64", "白鹭": "text", "苍鹭": "uint64", "黑水鸡": "uint64"}, str(types))
    check("批次全部登记", len(snap_all["batches"]) == 3)
    check("按站点过滤", len(snap_qw["batches"]) == 2)

    schema, table = store.load_unified(cfg.parquet_dir, snap_all["columns"], snap_all["batches"])
    check("统一行数", table.num_rows == 4)
    check("统一列序", schema.names == ["obs_date", "麻雀", "白鹭", "苍鹭", "黑水鸡"])
    rows = table.to_pylist()
    check("缺列补NULL", rows[1]["苍鹭"] is None and rows[1]["白鹭"] is None)
    check("旧整数按十进制转文本", rows[0]["白鹭"] == "3" and isinstance(rows[0]["白鹭"], str))
    check("文本原样保留", rows[3]["白鹭"] == "约12")
    check("整数列保持uint64", rows[0]["麻雀"] == 12 and isinstance(rows[0]["麻雀"], int))

    # 提升后不得退回整数：再发一批把「白鹭」声明为 uint64
    f4 = os.path.join(tmp, "s4.csv")
    write(f4, "日期,白鹭\n2024-05-01,6\n")
    service.publish(cfg, "青湾", "s4.csv", f4, ["uint64"])
    snap2 = snap_of(cfg)
    types2 = {c["name"]: c["type"] for c in snap2["columns"]}
    check("提升后不退回整数", types2["白鹭"] == "text")
    _, t2 = store.load_unified(cfg.parquet_dir, snap2["columns"], snap2["batches"])
    check("新批整数也按文本读出", t2.to_pylist()[-1]["白鹭"] == "6")

    # 统一 CSV 导出（用 csv 解析，兼容 pyarrow 对非 ASCII 表头的引号风格）
    import csv as _csv
    csv_text = store.table_to_csv_bytes(t2).decode("utf-8")
    header = next(_csv.reader(io.StringIO(csv_text)))
    check("统一CSV含全部列", header == ["obs_date", "麻雀", "白鹭", "苍鹭", "黑水鸡"], str(header))


# ---------- 3. 并发发布 ----------

def test_concurrent(tmp):
    cfg = make_cfg(tmp)
    errors = []

    def worker(i, decl):
        try:
            f = os.path.join(tmp, f"c{i}.csv")
            col = "共有种" if i < 2 else f"鸟种{i}"
            write(f, f"日期,{col}\n2024-06-0{i % 9 + 1},5\n")
            service.publish(cfg, f"站{i}", f"c{i}.csv", f, [decl])
        except Exception:
            errors.append(traceback.format_exc())

    # 线程 0/1 对同一列给出冲突类型(uint64/text)，其余线程各添新列
    decls = ["uint64", "text"] + ["uint64"] * 8
    threads = [threading.Thread(target=worker, args=(i, decls[i])) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check("并发无异常", not errors, "\n".join(errors))
    snap = snap_of(cfg)
    types = {c["name"]: c["type"] for c in snap["columns"]}
    check("并发批次全保留", len(snap["batches"]) == 10, str(len(snap["batches"])))
    check("并发新增列全保留", all(f"鸟种{i}" in types for i in range(2, 10)), str(types))
    check("并发下同列提升为text", types.get("共有种") == "text", str(types.get("共有种")))
    _, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
    check("并发统一行数", table.num_rows == 10)
    vals = sorted(r["共有种"] for r in table.to_pylist() if r["共有种"] is not None)
    check("冲突列两批数据均保留为文本", vals == ["5", "5"], str(vals))


# ---------- 4. 坏批不发布 ----------

def test_bad_batch(tmp):
    cfg = make_cfg(tmp)
    good = os.path.join(tmp, "g.csv")
    write(good, "日期,麻雀\n2024-01-01,3\n")
    service.publish(cfg, "站A", "g.csv", good, ["uint64"])
    before_files = set(os.listdir(cfg.parquet_dir))

    bad = os.path.join(tmp, "b.csv")
    write(bad, "日期,麻雀,白鹭\n2024-01-02,abc,1\n")
    try:
        service.publish(cfg, "站A", "b.csv", bad, ["uint64", "uint64"])
        raise AssertionError("坏整数应整批拒绝")
    except csvparse.CsvError:
        print("  ✓ 坏整数整批拒绝")
    check("坏批未登记", len(snap_of(cfg)["batches"]) == 1)
    check("坏批无残件", set(os.listdir(cfg.parquet_dir)) == before_files)

    # 写盘失败（模拟）=> 整批不发布
    orig = store.write_parquet_atomic
    def boom(_table, _path):
        raise OSError("disk full (simulated)")
    store.write_parquet_atomic = boom
    try:
        try:
            service.publish(cfg, "站A", "g2.csv", good, ["uint64"])
            raise AssertionError("写盘失败应抛错")
        except OSError:
            print("  ✓ 写盘失败整批不发布")
    finally:
        store.write_parquet_atomic = orig
    check("写盘失败未登记", len(snap_of(cfg)["batches"]) == 1)
    check("写盘失败无残件", set(os.listdir(cfg.parquet_dir)) == before_files)


# ---------- 5. 重启持久化 ----------

def test_restart(tmp):
    cfg = make_cfg(tmp)
    f = os.path.join(tmp, "r.csv")
    write(f, "日期,麻雀\n2024-01-01,3\n")
    service.publish(cfg, "站A", "r.csv", f, ["uint64"])
    cfg2 = Config(cfg.data_dir)          # 模拟重启：全新对象指向同一数据目录
    service.init_storage(cfg2)
    snap = snap_of(cfg2)
    check("重启后批次在", len(snap["batches"]) == 1)
    _, table = store.load_unified(cfg2.parquet_dir, snap["columns"], snap["batches"])
    check("重启后数据可读", table.num_rows == 1 and table.to_pylist()[0]["麻雀"] == 3)
    check("目录不引用残件", all(
        os.path.isfile(os.path.join(cfg2.parquet_dir, b["parquet_file"]))
        for b in snap["batches"]))


# ---------- 6. Web 入口 ----------

def test_web(tmp):
    os.environ["DATA_DIR"] = os.path.join(tmp, "webdata")
    from app.web import create_app
    app = create_app()
    app.config["TESTING"] = True
    c = app.test_client()

    r = c.get("/")
    check("首页200", r.status_code == 200)

    data = {"station": "青湾",
            "file": (io.BytesIO("日期,麻雀,白鹭\n2023-05-01,12,3\n".encode("utf-8")), "a.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    check("上传后进入声明页", r.status_code == 200 and "逐列声明类型".encode() in r.data)
    m = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data)
    check("声明页含暂存令牌", m is not None)
    token = m.group(1).decode()

    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "a.csv",
                                 "type_0": "uint64", "type_1": "text"})
    check("发布后跳转预览", r.status_code == 302 and "/preview" in r.headers["Location"])

    r = c.get("/preview?station=青湾")
    check("预览含数据", r.status_code == 200 and "麻雀".encode() in r.data and b"12" in r.data)

    r = c.get("/download.csv?station=青湾")
    body = r.data.decode("utf-8")
    check("下载统一CSV", r.status_code == 200 and r.headers["Content-Type"].startswith("text/csv"))
    check("CSV含表头与数据", "obs_date" in body and "麻雀" in body and "12" in body)

    # 坏整数批次：网页流程下整批不发布
    data = {"station": "青湾",
            "file": (io.BytesIO("日期,麻雀\n2023-05-02,oops\n".encode("utf-8")), "bad.csv")}
    r = c.post("/upload", data=data, content_type="multipart/form-data")
    token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
    r = c.post("/publish", data={"token": token, "station": "青湾", "filename": "bad.csv",
                                 "type_0": "uint64"}, follow_redirects=True)
    check("坏批提示未发布", "批次未发布".encode() in r.data)
    check("坏批未入统一结构", b"oops" not in c.get("/download.csv").data)


# ---------- 7. 重复调查索引：分组、行号、完全相同重复行 ----------

def test_dup_groups(tmp):
    cfg = make_cfg_manual_backfill(tmp)
    f1 = os.path.join(tmp, "a.csv")
    f2 = os.path.join(tmp, "b.csv")
    f3 = os.path.join(tmp, "c.csv")
    write(f1, "日期,麻雀\n2024-03-01,1\n2024-03-02,2\n")
    write(f2, "日期,麻雀\n2024-03-01,3\n2024-03-01,4\n")       # 与 f1 同站同日；本批同日两行
    write(f3, "日期,麻雀\n2024-03-02,5\n")                      # 另一组
    r1 = service.publish(cfg, "青湾", "a.csv", f1, ["uint64"])
    r2 = service.publish(cfg, "青湾", "b.csv", f2, ["uint64"])
    r3 = service.publish(cfg, "青湾", "c.csv", f3, ["uint64"])

    # 发布事务内同步登记索引：补建未跑也已有全部观测
    conn = catalog.connect(cfg.db_path)
    try:
        n_obs = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
    finally:
        conn.close()
    check("发布即整批索引", n_obs == 5, str(n_obs))

    backfill.run_backfill_once(cfg)
    check("空历史补建直接就绪", dup_status(cfg)["ready"])

    conn = catalog.connect(cfg.db_path)
    try:
        total = catalog.count_dup_groups(conn)
        groups = catalog.list_dup_groups(conn, None, 0, 20)
    finally:
        conn.close()
    check("重复组数量（>=2 条观测）", total == 2, str(total))
    by_day = {g["obs_date"]: g for g in groups}
    check("03-01 组三条观测", by_day["2024-03-01"]["n_obs"] == 3)
    check("03-02 组两条观测", by_day["2024-03-02"]["n_obs"] == 2)

    conn = catalog.connect(cfg.db_path)
    try:
        rows = catalog.list_group_rows(conn, "青湾", "2024-03-01")
    finally:
        conn.close()
    check("组内行含来源批次/文件名/行号",
          {(r["batch_id"], r["source_filename"], r["row_no"]) for r in rows} ==
          {(r1["batch_id"], "a.csv", 1),
           (r2["batch_id"], "b.csv", 1),
           (r2["batch_id"], "b.csv", 2)}, str(rows))
    check("批内数据行号从1起", min(r["row_no"] for r in rows) == 1)

    # 完全相同的重复行也分别保留
    f4 = os.path.join(tmp, "d.csv")
    write(f4, "日期,麻雀\n2024-03-01,1\n2024-03-01,1\n")
    r4 = service.publish(cfg, "青湾", "d.csv", f4, ["uint64"])
    conn = catalog.connect(cfg.db_path)
    try:
        rows = catalog.list_group_rows(conn, "青湾", "2024-03-01")
        d_rows = [r for r in rows if r["batch_id"] == r4["batch_id"]]
    finally:
        conn.close()
    check("完全相同重复行分别保留", len(d_rows) == 2
          and {r["row_no"] for r in d_rows} == {1, 2}, str(d_rows))


# ---------- 8. 核查标记：乐观并发 + 发布撤销 + 旧页不能标变化组 ----------

def test_dup_marks(tmp):
    cfg = make_cfg_manual_backfill(tmp)
    f1 = os.path.join(tmp, "a.csv")
    f2 = os.path.join(tmp, "b.csv")
    write(f1, "日期,麻雀\n2024-05-01,1\n")
    write(f2, "日期,麻雀\n2024-05-01,2\n")
    service.publish(cfg, "站X", "a.csv", f1, ["uint64"])
    service.publish(cfg, "站X", "b.csv", f2, ["uint64"])
    backfill.run_backfill_once(cfg)

    conn = catalog.connect(cfg.db_path)
    try:
        n = catalog.mark_group_checked(conn, "站X", "2024-05-01",
                                       "2026-01-01T00:00:00+00:00")
    finally:
        conn.close()
    check("标记返回组内观测数", n == 2, str(n))

    conn = catalog.connect(cfg.db_path)
    try:
        g = catalog.list_dup_groups(conn, None, 0, 20)[0]
    finally:
        conn.close()
    check("组显示已核查", g["checked"] and not g["stale"] and g["n_at_check"] == 2)

    # 组内新增观测 => 发布事务撤销标记
    f3 = os.path.join(tmp, "c.csv")
    write(f3, "日期,麻雀\n2024-05-01,3\n")
    service.publish(cfg, "站X", "c.csv", f3, ["uint64"])
    conn = catalog.connect(cfg.db_path)
    try:
        g = catalog.list_dup_groups(conn, None, 0, 20)[0]
        still = conn.execute(
            "SELECT 1 FROM dup_checks WHERE station=? AND obs_date=?",
            ("站X", "2024-05-01")).fetchone()
    finally:
        conn.close()
    check("新增观测撤销标记", still is None and not g["checked"])
    check("撤销后组内三条", g["n_obs"] == 3)

    # 旧页面（渲染时组内 2 条）提交标记：expected_n=2 与当前 3 不符 => 拒绝
    conn = catalog.connect(cfg.db_path)
    try:
        rejected = catalog.mark_group_checked(
            conn, "站X", "2024-05-01", "2026-01-02T00:00:00+00:00",
            expected_n=2)
        inserted = conn.execute(
            "SELECT 1 FROM dup_checks WHERE station=? AND obs_date=?",
            ("站X", "2024-05-01")).fetchone()
    finally:
        conn.close()
    check("旧页面标记被拒绝", rejected is None and inserted is None)

    # 刷新页面后按当前观测数（3）标记 => 成功
    conn = catalog.connect(cfg.db_path)
    try:
        n = catalog.mark_group_checked(
            conn, "站X", "2024-05-01", "2026-01-03T00:00:00+00:00",
            expected_n=3)
    finally:
        conn.close()
    check("刷新后标记成功", n == 3)

    # 不足两条的组不能标记
    conn = catalog.connect(cfg.db_path)
    try:
        none = catalog.mark_group_checked(
            conn, "站X", "2099-01-01", "2026-01-03T00:00:00+00:00")
    finally:
        conn.close()
    check("单条/空组不可标记", none is None)


# ---------- 9. 补建：扫描不占写事务、期间发布可用、就绪含全部批次 ----------

def test_backfill_lifecycle(tmp):
    cfg = make_cfg_manual_backfill(tmp)
    files = []
    for i in range(6):
        p = os.path.join(tmp, f"h{i}.csv")
        write(p, f"日期,麻雀\n2024-01-0{i+1},{i}\n")
        service.publish(cfg, "站H", f"h{i}.csv", p, ["uint64"])
        files.append(p)
    check("补建前状态为 backfilling", dup_status(cfg)["status"] == "backfilling")

    # 手工逐批补建，验证游标推进与幂等（发布时已同步索引，补建应零新增）
    conn = catalog.connect(cfg.db_path)
    try:
        n_before = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
        first = catalog.batches_after_cursor(conn, 0, limit=1)[0]
        backfill.index_one_batch(cfg, conn, first)
        cur1 = catalog.get_backfill_cursor(conn)
        # 再补同一批：INSERT OR IGNORE 不产生重复
        backfill.index_one_batch(cfg, conn, first)
        n_after = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
    finally:
        conn.close()
    check("游标按批推进", cur1 == first["rowid"], str(cur1))
    check("补建幂等无重复（发布已索引的行不重复登记）", n_after == n_before == 6,
          f"{n_before}->{n_after}")

    # 补建进行中：上传/预览/下载照常（目录库短事务不被扫描占用）
    p = os.path.join(tmp, "hmid.csv")
    write(p, "日期,麻雀\n2024-02-01,9\n")
    rmid = service.publish(cfg, "站H", "hmid.csv", p, ["uint64"])
    conn = catalog.connect(cfg.db_path)
    try:
        snap = catalog.snapshot(conn)
        mid_indexed = conn.execute(
            "SELECT COUNT(*) AS n FROM obs_index WHERE batch_id=?",
            (rmid["batch_id"],)).fetchone()["n"]
    finally:
        conn.close()
    check("补建期间发布可用", len(snap["batches"]) == 7)
    check("补建期间批次自行登记索引", mid_indexed == 1)

    # 补建期间页面提示未完成
    check("就绪前提示未完成", not dup_status(cfg)["ready"])

    # 跑完补建：包含期间发布的全部批次（游标扫过全部 7 批）
    final = backfill.run_backfill_once(cfg)
    check("补建就绪", final["ready"])
    conn = catalog.connect(cfg.db_path)
    try:
        n_obs = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
        n_batches = conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
        indexed_batches = conn.execute(
            "SELECT COUNT(DISTINCT batch_id) AS n FROM obs_index").fetchone()["n"]
        check("就绪包含全部观测", n_obs == 7, str(n_obs))
        check("就绪覆盖全部批次", indexed_batches == n_batches == 7)

        # 扫描不占写事务的旁证：补建完成后仍可立即拿到一致读快照
        snap2 = catalog.snapshot(conn)
    finally:
        conn.close()
    check("补建后一致快照可用", len(snap2["batches"]) == 7)

    # 再启动一次补建：已就绪直接返回
    again = backfill.run_backfill_once(cfg)
    check("已就绪不重复补建", again["ready"])


# ---------- 9b. 补建瞬时故障重试与接续 ----------

def test_backfill_retry(tmp):
    import logging
    logging.getLogger("birdlake.backfill").setLevel(logging.CRITICAL)  # 静默重试日志
    backfill.RETRY_BASE_DELAY = 0  # 测试中不等待
    cfg = make_cfg_manual_backfill(tmp)
    for i in range(2):
        p = os.path.join(tmp, f"r{i}.csv")
        write(p, "日期,麻雀\n2024-01-01,1\n")
        service.publish(cfg, "站R", f"r{i}.csv", p, ["uint64"])

    # 前两次扫描失败、第三次成功 => 重试后整批补建完成
    calls = {"n": 0}
    orig = backfill._read_batch_dates

    def flaky(c, b):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise OSError("simulated transient failure")
        return orig(c, b)

    backfill._read_batch_dates = flaky
    try:
        final = backfill.run_backfill_once(cfg)
    finally:
        backfill._read_batch_dates = orig
    check("瞬时故障重试后就绪", final["ready"])
    check("确实发生了重试", calls["n"] >= 3, str(calls["n"]))
    conn = catalog.connect(cfg.db_path)
    try:
        n = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
    finally:
        conn.close()
    check("重试后索引完整", n == 2, str(n))

    # 持续失败：重试耗尽后抛出，游标停在上一批，重启可接续
    saved_reader = backfill._read_batch_dates
    cfg2_dir = os.path.join(tmp, "persist")
    os.makedirs(cfg2_dir, exist_ok=True)
    cfg2 = Config(cfg2_dir)
    cfg2.dup_backfill_enabled = False
    service.init_storage(cfg2)
    for i in range(2):
        p = os.path.join(cfg2_dir, f"p{i}.csv")
        write(p, "日期,麻雀\n2024-01-01,1\n")
        service.publish(cfg2, "站P", f"p{i}.csv", p, ["uint64"])
    backfill._read_batch_dates = lambda c, b: (_ for _ in ()).throw(OSError("permanent"))
    try:
        try:
            backfill.run_backfill_once(cfg2)
            raise AssertionError("持续失败应抛出")
        except OSError:
            print("  ✓ 重试耗尽后抛出，等待重启接续")
    finally:
        backfill._read_batch_dates = saved_reader
    check("持续失败后保持未就绪", not dup_status(cfg2)["ready"])
    check("持续失败后游标停在0（第一批未完成）", dup_status(cfg2)["cursor"] == 0)
    final2 = backfill.run_backfill_once(cfg2)
    check("重启接续后就绪", final2["ready"])


# ---------- 10. 重启接续补建、索引与标记保留 ----------

def test_backfill_restart(tmp):
    cfg = make_cfg_manual_backfill(tmp)
    for i in range(4):
        p = os.path.join(tmp, f"k{i}.csv")
        write(p, f"日期,麻雀\n2024-07-0{i+1},{i}\n")
        service.publish(cfg, "站K", f"k{i}.csv", p, ["uint64"])
    # 只补建前两批（模拟中断）
    conn = catalog.connect(cfg.db_path)
    try:
        for _ in range(2):
            batch = catalog.batches_after_cursor(
                conn, catalog.get_backfill_cursor(conn), limit=1)[0]
            backfill.index_one_batch(cfg, conn, batch)
        cursor_before = catalog.get_backfill_cursor(conn)
    finally:
        conn.close()
    check("中断时仍 backfilling", not dup_status(cfg)["ready"])

    # 制造一个核查标记（需要重复组：再发同站同日批次）
    p = os.path.join(tmp, "kdup.csv")
    write(p, "日期,麻雀\n2024-07-01,8\n")
    service.publish(cfg, "站K", "kdup.csv", p, ["uint64"])
    conn = catalog.connect(cfg.db_path)
    try:
        catalog.mark_group_checked(
            conn, "站K", "2024-07-01", "2026-03-01T00:00:00+00:00")
    finally:
        conn.close()

    # 模拟重启：全新 Config/init_storage，自动补建线程接续
    cfg2 = Config(cfg.data_dir)
    service.init_storage(cfg2)
    check("重启后接续补建并就绪", backfill.wait_until_ready(cfg2, timeout=30))

    conn = catalog.connect(cfg2.db_path)
    try:
        n_obs = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
        mark = conn.execute(
            "SELECT n_at_check FROM dup_checks WHERE station=? AND obs_date=?",
            ("站K", "2024-07-01")).fetchone()
        cursor_after = catalog.get_backfill_cursor(conn)
    finally:
        conn.close()
    check("重启后索引完整（含补建前已索引部分）", n_obs == 5, str(n_obs))
    check("已完成索引保留（游标未倒退）", cursor_after >= cursor_before)
    check("核查标记重启后保留", mark is not None and mark["n_at_check"] == 2)


# ---------- 11. 发布原子性：失败不留半批（含索引/标记回滚） ----------

def test_publish_atomicity(tmp):
    cfg = make_cfg_manual_backfill(tmp)
    p = os.path.join(tmp, "a.csv")
    write(p, "日期,麻雀\n2024-08-01,1\n2024-08-01,2\n")
    service.publish(cfg, "站A", "a.csv", p, ["uint64"])
    before_files = set(os.listdir(cfg.parquet_dir))

    # 让写事务在索引阶段失败：模拟 catalog.index_batch 抛错
    orig = catalog.index_batch
    def boom(*_a, **_kw):
        raise sqlite3.OperationalError("simulated index failure")
    catalog.index_batch = boom
    try:
        try:
            service.publish(cfg, "站A", "b.csv", p, ["uint64"])
            raise AssertionError("索引失败应导致整批回滚")
        except sqlite3.OperationalError:
            print("  ✓ 索引失败整批回滚")
    finally:
        catalog.index_batch = orig

    conn = catalog.connect(cfg.db_path)
    try:
        n_batches = conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
        n_obs = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
    finally:
        conn.close()
    check("回滚后批次不增加", n_batches == 1)
    check("回滚后索引不增加", n_obs == 2, str(n_obs))
    check("回滚后无残件", set(os.listdir(cfg.parquet_dir)) == before_files)


# ---------- 12. 重复核查 Web 入口（含补建提示与乐观并发） ----------

def test_dup_web(tmp):
    os.environ["DATA_DIR"] = os.path.join(tmp, "webdata")
    from app.web import create_app
    app = create_app()
    app.config["TESTING"] = True
    c = app.test_client()
    cfg = app.config["_cfg"]

    def publish_via_web(station, fname, content, types):
        data = {"station": station,
                "file": (io.BytesIO(content.encode("utf-8")), fname)}
        r = c.post("/upload", data=data, content_type="multipart/form-data")
        token = re.search(rb'name="token" value="([0-9a-f]{32})"', r.data).group(1).decode()
        form = {"token": token, "station": station, "filename": fname}
        for i, t in enumerate(types):
            form[f"type_{i}"] = t
        r = c.post("/publish", data=form, follow_redirects=False)
        check(f"发布 {fname}", r.status_code == 302)

    publish_via_web("青湾", "a.csv", "日期,麻雀\n2024-09-01,1\n", ["uint64"])
    publish_via_web("青湾", "b.csv", "日期,麻雀\n2024-09-01,2\n2024-09-02,3\n", ["uint64"])

    # 等补建就绪（create_app 已启动后台线程）
    check("Web 补建就绪", backfill.wait_until_ready(cfg, timeout=30))

    r = c.get("/duplicates")
    check("核查页200", r.status_code == 200)
    body = r.data.decode("utf-8")
    check("列出重复组", "2024-09-01" in body and "a.csv" in body and "b.csv" in body)
    check("显示批内行号", ">1<" in body)

    # 标记（页面渲染时该组 2 条观测）
    r = c.post("/duplicates/mark",
               data={"station": "青湾", "obs_date": "2024-09-01", "expected_n": "2"},
               follow_redirects=True)
    check("标记成功提示", "已标记核查".encode() in r.data)
    r = c.get("/duplicates")
    check("页面显示已核查", "已核查 2026-".encode() in r.data)

    # 新增同站同日观测 => 标记撤销；旧页面（expected_n=2）再标记被拒
    publish_via_web("青湾", "c.csv", "日期,麻雀\n2024-09-01,4\n", ["uint64"])
    r = c.get("/duplicates")
    check("发布后标记撤销", "已核查 2026-".encode() not in r.data
          and "标记已过期".encode() not in r.data)
    r = c.post("/duplicates/mark",
               data={"station": "青湾", "obs_date": "2024-09-01", "expected_n": "2"},
               follow_redirects=True)
    check("旧页面标记被拒", "已发生变化".encode() in r.data)
    r = c.post("/duplicates/mark",
               data={"station": "青湾", "obs_date": "2024-09-01", "expected_n": "3"},
               follow_redirects=True)
    check("刷新后标记成功", "已标记核查".encode() in r.data)

    # 缺 expected_n 被拒
    r = c.post("/duplicates/mark", data={"station": "青湾", "obs_date": "2024-09-01"})
    check("缺版本令牌400", r.status_code == 400)

    # 非法日期被拒
    r = c.post("/duplicates/mark",
               data={"station": "青湾", "obs_date": "not-a-date", "expected_n": "3"})
    check("非法日期400", r.status_code == 400)

    # 分页：造 > 一页的重复组（每天一对同日观测 => 25 个不同重复组）
    for i in range(25):
        day = f"{i+1:02d}"
        publish_via_web("分页站", f"p{i}a.csv",
                        f"日期,麻雀\n2024-10-{day},{i}\n", ["uint64"])
        publish_via_web("分页站", f"p{i}b.csv",
                        f"日期,麻雀\n2024-10-{day},{i+100}\n", ["uint64"])
    backfill.wait_until_ready(cfg, timeout=30)
    r = c.get("/duplicates?station=分页站&page=1")
    check("分页第1页", r.status_code == 200 and "第 1/2 页".encode() in r.data)
    r2 = c.get("/duplicates?station=分页站&page=2")
    check("分页第2页", r2.status_code == 200 and "第 2/2 页".encode() in r2.data)
    check("重复组总数为25", "共 25 组".encode() in r.data)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        steps = [
            ("CSV 解析与校验", test_parser),
            ("发布 / 结构合并 / 类型提升", test_publish_merge),
            ("并发发布（新增列 + 同列类型冲突）", test_concurrent),
            ("坏整数 / 写盘失败整批不发布", test_bad_batch),
            ("重启后已发布数据可用", test_restart),
            ("Web 入口（Flask test client）", test_web),
            ("重复调查索引：分组 / 行号 / 完全相同重复行", test_dup_groups),
            ("核查标记：乐观并发 / 发布撤销 / 旧页拒绝", test_dup_marks),
            ("补建生命周期：不占写事务 / 期间发布 / 就绪完整", test_backfill_lifecycle),
            ("补建瞬时故障重试与重启接续", test_backfill_retry),
            ("重启接续补建、索引与标记保留", test_backfill_restart),
            ("发布原子性：索引失败不留半批", test_publish_atomicity),
            ("重复核查 Web 入口与分页", test_dup_web),
        ]
        for i, (title, fn) in enumerate(steps, 1):
            print(f"[{i}/{len(steps)}] {title}")
            fn(os.path.join(tmp, f"case{i}"))
    print("\n全部自检通过 ✅")


if __name__ == "__main__":
    main()
