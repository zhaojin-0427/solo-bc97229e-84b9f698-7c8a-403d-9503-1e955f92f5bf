"""Parquet 物理存储与统一结构读取（PyArrow）。

- 每批一个独立 Parquet 文件，文件名 = 批次 UUID，既有文件永不重写；
- 写入先落临时文件 + fsync，再原子 rename，目录（SQLite）只引用完整文件；
- 统一读取：按目录快照中的逻辑结构投影每个批次文件——缺列补 NULL，
  逻辑类型为 text 而文件列为 uint64 时按十进制转成文本。
"""
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

DATE_COLUMN = "obs_date"  # 首列观测日归一化为固定列名


def logical_arrow_type(t):
    return pa.uint64() if t == "uint64" else pa.string()


def build_batch_table(dates, columns):
    """columns: [(name, 'uint64'|'text', values), ...]"""
    names = [DATE_COLUMN] + [c[0] for c in columns]
    arrays = [pa.array(dates, type=pa.date32())]
    for _name, typ, vals in columns:
        arrays.append(pa.array(vals, type=logical_arrow_type(typ)))
    return pa.Table.from_arrays(arrays, names=names)


def _fsync_dir(dirpath):
    fd = os.open(dirpath, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_parquet_atomic(table, final_path):
    """写临时文件 -> fsync -> 原子 rename -> 目录 fsync。失败清理临时文件。"""
    dirpath = os.path.dirname(final_path)
    base = os.path.basename(final_path)
    tmp_path = os.path.join(dirpath, f".{base}.tmp")
    try:
        with open(tmp_path, "wb") as f:
            pq.write_table(table, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, final_path)  # 同目录原子改名；文件名为批次UUID，不会覆盖既有文件
        _fsync_dir(dirpath)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def unified_schema(columns):
    """目录快照中的逻辑结构 -> Arrow schema（obs_date 在前）。"""
    fields = [pa.field(DATE_COLUMN, pa.date32())]
    for c in columns:
        fields.append(pa.field(c["name"], logical_arrow_type(c["type"])))
    return pa.schema(fields)


def load_unified(parquet_dir, columns, batches):
    """按同一份（逻辑结构, 文件清单）快照读取统一结构的表。"""
    schema = unified_schema(columns)
    tables = []
    for b in batches:
        path = os.path.join(parquet_dir, b["parquet_file"])
        t = pq.read_table(path)
        file_colnames = set(t.schema.names)
        arrs = []
        for f in schema:
            if f.name in file_colnames:
                a = t.column(f.name)
                if not a.type.equals(f.type):
                    a = pc.cast(a, f.type)  # uint64 -> string 为十进制文本
                arrs.append(a)
            else:
                arrs.append(pa.nulls(t.num_rows, type=f.type))  # 缺列补 NULL
        tables.append(pa.Table.from_arrays(arrays=arrs, schema=schema))
    if not tables:
        empty = [pa.array([], type=f.type) for f in schema]
        return schema, pa.Table.from_arrays(empty, schema=schema)
    return schema, pa.concat_tables(tables, promote_options="none")


def table_to_csv_bytes(table):
    sink = pa.BufferOutputStream()
    pcsv.write_csv(table, sink)
    return sink.getvalue().to_pybytes()
