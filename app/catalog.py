"""目录库（SQLite, WAL 模式）：逻辑结构 + 批次清单。

事务纪律：
- 写事务（write_txn）只在「Parquet 文件已完整落盘」之后短暂持有，
  用于合并逻辑结构 + 登记批次，解析/写文件期间绝不持有库写事务。
- 读事务（read_txn）给调用方一个一致快照：同一次取数看到同一状态的
  逻辑结构与文件清单。
"""
import sqlite3
from contextlib import contextmanager

COLUMN_TYPES = ("uint64", "text")

DDL = """
CREATE TABLE IF NOT EXISTS columns (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,   -- 稳定列序：先出现的列排前
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL CHECK (type IN ('uint64','text'))
);
CREATE TABLE IF NOT EXISTS batches (
  id              TEXT PRIMARY KEY,
  station         TEXT NOT NULL,
  source_filename TEXT NOT NULL,
  parquet_file    TEXT NOT NULL UNIQUE,
  n_rows          INTEGER NOT NULL,
  created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_station ON batches(station);
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- 观测索引：每条数据行一行（含完全相同的重复行，靠 row_no 各自保留）
CREATE TABLE IF NOT EXISTS obs_index (
  batch_id TEXT NOT NULL,
  row_no   INTEGER NOT NULL,           -- 批内数据行号，从 1 起
  station  TEXT NOT NULL,
  obs_date TEXT NOT NULL,              -- ISO 日期 YYYY-MM-DD
  PRIMARY KEY (batch_id, row_no)
);
CREATE INDEX IF NOT EXISTS idx_obs_group ON obs_index(station, obs_date);
-- 重复组核查标记：n_at_check 是标记时组内观测数，兼作乐观并发版本号
CREATE TABLE IF NOT EXISTS dup_checks (
  station    TEXT NOT NULL,
  obs_date   TEXT NOT NULL,
  n_at_check INTEGER NOT NULL,
  checked_at TEXT NOT NULL,
  PRIMARY KEY (station, obs_date)
);
"""

DUP_STATUS_KEY = "dup_index_status"       # backfilling | ready
DUP_CURSOR_KEY = "dup_backfill_cursor"    # 已补建到的 batches.rowid
DUP_STATUS_BACKFILLING = "backfilling"
DUP_STATUS_READY = "ready"


def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path):
    conn = connect(db_path)
    try:
        conn.executescript(DDL)
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '0')")
        # 首次启动进入补建态；已就绪的库保持 ready（INSERT OR IGNORE 不覆盖）
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)",
            (DUP_STATUS_KEY, DUP_STATUS_BACKFILLING),
        )
    finally:
        conn.close()


@contextmanager
def write_txn(conn):
    """短写事务：BEGIN IMMEDIATE 串行化并发发布者。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def read_txn(conn):
    """一致读快照（WAL 下读写不互斥）。"""
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")


def merge_schema(conn, decls):
    """在同一写事务内合并逻辑结构。

    decls: [(name, 'uint64'|'text'), ...]
    规则：新列加入；同名同型不变；同名异型提升为 text（text 吸收，永不退回 uint64）。
    """
    changes = []
    for name, new_type in decls:
        if new_type not in COLUMN_TYPES:
            raise ValueError(f"非法列类型: {new_type!r}")
        row = conn.execute("SELECT type FROM columns WHERE name = ?", (name,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO columns(name, type) VALUES (?, ?)", (name, new_type))
            changes.append({"change": "added", "name": name, "type": new_type})
        elif row["type"] == new_type:
            continue
        elif row["type"] == "text":
            continue  # 已提升为 text，不退回整数
        else:
            conn.execute("UPDATE columns SET type = 'text' WHERE name = ? AND type = 'uint64'", (name,))
            changes.append({"change": "promoted", "name": name, "type": "text"})
    if changes:
        conn.execute(
            "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
            "WHERE key = 'schema_version'"
        )
    return changes


def insert_batch(conn, batch):
    conn.execute(
        "INSERT INTO batches(id, station, source_filename, parquet_file, n_rows, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (batch["id"], batch["station"], batch["source_filename"],
         batch["parquet_file"], batch["n_rows"], batch["created_at"]),
    )


def snapshot(conn, station=None):
    """一个读事务内同时取逻辑结构与批次清单（同一状态）。"""
    with read_txn(conn):
        cols = [dict(r) for r in conn.execute("SELECT name, type FROM columns ORDER BY id")]
        q = ("SELECT id, station, source_filename, parquet_file, n_rows, created_at"
             " FROM batches")
        if station:
            rows = conn.execute(q + " WHERE station = ? ORDER BY rowid", (station,)).fetchall()
        else:
            rows = conn.execute(q + " ORDER BY rowid").fetchall()
        ver = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    return {
        "schema_version": int(ver["value"]) if ver else 0,
        "columns": cols,
        "batches": [dict(r) for r in rows],
    }


# ---------- 重复调查索引与核查标记 ----------

def get_dup_status(conn):
    with read_txn(conn):
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (DUP_STATUS_KEY,)).fetchone()
        status = row["value"] if row else DUP_STATUS_BACKFILLING
        cur = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (DUP_CURSOR_KEY,)).fetchone()
        n_indexed = conn.execute("SELECT COUNT(*) AS n FROM obs_index").fetchone()["n"]
        n_batches = conn.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"]
    return {
        "status": status,
        "ready": status == DUP_STATUS_READY,
        "cursor": int(cur["value"]) if cur else 0,
        "n_indexed_rows": n_indexed,
        "n_batches": n_batches,
    }


def get_backfill_cursor(conn):
    with read_txn(conn):
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (DUP_CURSOR_KEY,)).fetchone()
    return int(row["value"]) if row else 0


def batches_after_cursor(conn, cursor, limit=1):
    """按登记顺序取 rowid > cursor 的批次（补建扫描用；只读）。"""
    with read_txn(conn):
        rows = conn.execute(
            "SELECT rowid, id, station, parquet_file, n_rows FROM batches"
            " WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (cursor, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def index_batch(conn, batch_id, station, entries):
    """整批观测登记进索引（幂等：重复补建同一批不产生重复行）。

    entries: [(row_no, obs_date_iso), ...]；必须在写事务内调用。
    """
    conn.executemany(
        "INSERT OR IGNORE INTO obs_index(batch_id, row_no, station, obs_date)"
        " VALUES (?, ?, ?, ?)",
        [(batch_id, row_no, station, obs_date) for row_no, obs_date in entries],
    )


def advance_backfill_cursor(conn, cursor):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (DUP_CURSOR_KEY, str(cursor)),
    )


def mark_dup_ready(conn):
    """排空检查后翻转就绪态（同一写事务内提交，保证就绪即完整）。"""
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (DUP_STATUS_KEY, DUP_STATUS_READY),
    )


def revoke_marks_for(conn, station, obs_dates):
    """组内新增观测时撤销核查标记（发布写事务内调用）。"""
    if not obs_dates:
        return 0
    conn.executemany(
        "DELETE FROM dup_checks WHERE station = ? AND obs_date = ?",
        [(station, d) for d in obs_dates],
    )
    return conn.total_changes


def count_dup_groups(conn, station=None):
    q = ("SELECT COUNT(*) AS n FROM ("
         " SELECT station, obs_date FROM obs_index"
         " GROUP BY station, obs_date HAVING COUNT(*) >= 2)")
    with read_txn(conn):
        if station:
            return conn.execute(q + " WHERE station = ?", (station,)).fetchone()["n"]
        return conn.execute(q).fetchone()["n"]


def list_dup_groups(conn, station=None, offset=0, limit=20):
    """分页列出重复组，并带核查状态与组内观测数（同一读快照）。"""
    where = "WHERE g.station = ?" if station else ""
    params = [station] if station else []
    sql = (
        "SELECT g.station, g.obs_date, g.n AS n_obs,"
        "       c.checked_at, c.n_at_check"
        " FROM (SELECT station, obs_date, COUNT(*) AS n FROM obs_index"
        "       GROUP BY station, obs_date HAVING COUNT(*) >= 2) g"
        f" LEFT JOIN dup_checks c ON c.station = g.station AND c.obs_date = g.obs_date"
        f" {where} ORDER BY g.station, g.obs_date LIMIT ? OFFSET ?"
    )
    with read_txn(conn):
        rows = conn.execute(sql, params + [limit, offset]).fetchall()
        groups = []
        for r in rows:
            g = dict(r)
            g["checked"] = g["checked_at"] is not None
            g["stale"] = g["checked"] and g["n_obs"] != g["n_at_check"]
            groups.append(g)
    return groups


def list_group_rows(conn, station, obs_date):
    """组内各行：来源批次、源文件名、批内数据行号（从 1 起）。

    完全相同的重复行分别保留（obs_index 主键含 row_no）。
    """
    with read_txn(conn):
        rows = conn.execute(
            "SELECT o.batch_id, o.row_no, b.source_filename, b.parquet_file"
            " FROM obs_index o JOIN batches b ON b.id = o.batch_id"
            " WHERE o.station = ? AND o.obs_date = ?"
            " ORDER BY b.rowid, o.row_no",
            (station, obs_date),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_group_checked(conn, station, obs_date, checked_at, expected_n=None):
    """把组标为已核查（乐观并发）。

    expected_n 是页面渲染时组内观测数，作为版本令牌：
    - 组当前观测数与 expected_n 不符 => 页面已过期，拒绝写入（None），
      旧页面无法标记已经变化的组；
    - expected_n 为 None（自检/内部调用）=> 按当前状态直接标记。
    成功返回当前观测数；组已不足两条返回 None。
    """
    with write_txn(conn):
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM obs_index"
            " WHERE station = ? AND obs_date = ?",
            (station, obs_date),
        ).fetchone()["n"]
        if n < 2:
            return None
        if expected_n is not None and expected_n != n:
            return None  # 组在页面打开后发生变化，必须刷新重新核查
        conn.execute(
            "INSERT INTO dup_checks(station, obs_date, n_at_check, checked_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(station, obs_date) DO UPDATE SET"
            "   n_at_check = excluded.n_at_check, checked_at = excluded.checked_at",
            (station, obs_date, n, checked_at),
        )
        return n


def unmark_group(conn, station, obs_date):
    with write_txn(conn):
        cur = conn.execute(
            "SELECT n_at_check FROM dup_checks WHERE station = ? AND obs_date = ?",
            (station, obs_date),
        ).fetchone()
        if cur is None:
            return False
        conn.execute(
            "DELETE FROM dup_checks WHERE station = ? AND obs_date = ?",
            (station, obs_date),
        )
        return True
