"""历史重复调查索引的首次后台补建。

纪律（对应需求）：
- 扫描在目录库写事务之外进行：Parquet 读取（可能较慢）不持有任何库事务，
  上传 / 预览 / 下载照常可用；
- 每个已登记批次只做一次「短写事务」：整批索引幂等写入 + 推进补建游标；
- 游标与已完成索引、核查标记都在库里，重启后从未完成处接续，已完成成果保留；
- 补建期间成功发布的批次在发布事务内自行登记索引；补建排空时在同一写事务内
  复检「游标之后无批次」才翻转 ready——就绪时必然包含期间所有成功发布的批次。
"""
import logging
import threading
import time

import pyarrow.parquet as pq

from . import catalog

_backfill_lock = threading.Lock()   # 同一进程内至多一个补建线程
_stop_events = {}
_log = logging.getLogger("birdlake.backfill")

# 单批扫描的瞬时故障重试：指数退避；超限后退出线程，等下次重启从游标接续
MAX_BATCH_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.5


def _read_batch_dates(cfg, batch):
    """扫描 Parquet 的观测日列（不持有库事务）。返回 [(row_no, iso_date), ...]。"""
    path = _safe_parquet_path(cfg, batch["parquet_file"])
    table = pq.read_table(path, columns=["obs_date"])
    dates = table.column("obs_date").to_pylist()
    return [(i + 1, d.isoformat()) for i, d in enumerate(dates)]


def _safe_parquet_path(cfg, name):
    # parquet 文件名是批次 UUID，防止任何路径穿越可能
    if "/" in name or "\\" in name or not name.endswith(".parquet"):
        raise ValueError(f"非法 Parquet 文件名: {name!r}")
    import os
    return os.path.join(cfg.parquet_dir, name)


def index_one_batch(cfg, conn, batch_row):
    """扫描一个批次并在短写事务内整批登记索引、推进游标。

    扫描在库事务之外，瞬时 I/O 故障按指数退避重试；持续失败则抛出，由调用方
    决定退出（游标已停在上一批，重启可接续）。
    """
    batch_id = batch_row["id"]
    station = batch_row["station"]
    entries = None
    for attempt in range(1, MAX_BATCH_ATTEMPTS + 1):
        try:
            entries = _read_batch_dates(cfg, batch_row)  # 库事务之外扫描
            break
        except Exception:
            if attempt == MAX_BATCH_ATTEMPTS:
                raise
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            _log.warning(
                "补建扫描批次 %s 第 %d 次失败，%.1fs 后重试",
                batch_id[:8], attempt, delay, exc_info=True)
            time.sleep(delay)
    with catalog.write_txn(conn):
        catalog.index_batch(conn, batch_id, station, entries)
        catalog.advance_backfill_cursor(conn, batch_row["rowid"])


def _finalize_if_drained(conn):
    """写事务内复检游标之后已无批次才置 ready；返回是否就绪。"""
    with catalog.write_txn(conn):
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (catalog.DUP_CURSOR_KEY,)
        ).fetchone()
        cursor = int(row["value"]) if row else 0
        remaining = conn.execute(
            "SELECT COUNT(*) AS n FROM batches WHERE rowid > ?", (cursor,)
        ).fetchone()["n"]
        if remaining:
            return False
        catalog.mark_dup_ready(conn)
        return True


def run_backfill_once(cfg, stop_event=None):
    """执行补建直到排空并置 ready。返回最终状态字典。"""
    conn = catalog.connect(cfg.db_path)
    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                return catalog.get_dup_status(conn)
            status = catalog.get_dup_status(conn)
            if status["ready"]:
                return status
            cursor = status["cursor"]
            pending = catalog.batches_after_cursor(conn, cursor, limit=1)
            if not pending:
                if _finalize_if_drained(conn):
                    return catalog.get_dup_status(conn)
                continue  # 排空检查与翻转之间有新批次发布，继续补建
            index_one_batch(cfg, conn, pending[0])
    finally:
        conn.close()

def _worker(cfg, stop_event):
    try:
        run_backfill_once(cfg, stop_event)
    except Exception:
        # 后台线程不允许吞掉进程；下次重启会从游标接续。这里落日志后退出。
        _log.exception("重复索引补建中断，将在下次启动时接续")


def start_background_backfill(cfg):
    """启动守护补建线程（每进程至多一个；已就绪则不启动）。"""
    with _backfill_lock:
        key = cfg.db_path
        ev = _stop_events.get(key)
        if ev is not None and not ev.is_set():
            return ev
        conn = catalog.connect(cfg.db_path)
        try:
            status = catalog.get_dup_status(conn)
        finally:
            conn.close()
        if status["ready"]:
            return None
        ev = threading.Event()
        _stop_events[key] = ev
        t = threading.Thread(target=_worker, args=(cfg, ev),
                             name="dup-backfill", daemon=True)
        t.start()
        return ev


def wait_until_ready(cfg, timeout=30.0):
    """阻塞等待补建就绪（主要给自检脚本用）。"""
    deadline = time.monotonic() + timeout
    conn = catalog.connect(cfg.db_path)
    try:
        while time.monotonic() < deadline:
            if catalog.get_dup_status(conn)["ready"]:
                return True
            time.sleep(0.02)
        return catalog.get_dup_status(conn)["ready"]
    finally:
        conn.close()
