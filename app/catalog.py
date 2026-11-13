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
"""


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
