#!/usr/bin/env python3
"""端到端自检：CSV 校验、发布、结构合并、类型提升、并发、重启持久化、Web 入口。

用法（在仓库根目录，容器内或装好依赖的环境）：
    python scripts/selftest.py
"""
import io
import os
import re
import sys
import tempfile
import threading
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import catalog, csvparse, service, store  # noqa: E402
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

    # 统一 CSV 导出
    csv_bytes = store.table_to_csv_bytes(t2).decode("utf-8")
    check("统一CSV含全部列", csv_bytes.splitlines()[0] == "obs_date,麻雀,白鹭,苍鹭,黑水鸡", csv_bytes.splitlines()[0])


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


def main():
    with tempfile.TemporaryDirectory() as tmp:
        steps = [
            ("CSV 解析与校验", test_parser),
            ("发布 / 结构合并 / 类型提升", test_publish_merge),
            ("并发发布（新增列 + 同列类型冲突）", test_concurrent),
            ("坏整数 / 写盘失败整批不发布", test_bad_batch),
            ("重启后已发布数据可用", test_restart),
            ("Web 入口（Flask test client）", test_web),
        ]
        for i, (title, fn) in enumerate(steps, 1):
            print(f"[{i}/{len(steps)}] {title}")
            fn(os.path.join(tmp, f"case{i}"))
    print("\n全部自检通过 ✅")


if __name__ == "__main__":
    main()
