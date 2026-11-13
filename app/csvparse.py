"""调查 CSV 解析与校验（纯标准库）。

格式约定：
- 首列为观测日（YYYY-MM-DD），其余每列一种鸟；
- 列名去空白后非空且不得重复；
- 空单元格为 NULL；
- uint64 列：去空白后必须全为数字且 <= 2^64-1，否则整批拒绝。
"""
import csv
import datetime
import re

UINT64_MAX = (1 << 64) - 1
_INT_RE = re.compile(r"[0-9]+")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class CsvError(ValueError):
    """CSV 内容不合法（调用方据此整批拒绝）。"""


def _is_blank_row(row):
    return not row or all((c or "") == "" for c in row)


def _check_names(names):
    if len(names) < 2:
        raise CsvError("CSV 至少需要两列：首列观测日 + 至少一个鸟种列")
    for n in names:
        if not n:
            raise CsvError("存在空白列名")
    seen, dups = set(), []
    for n in names:
        if n in seen and n not in dups:
            dups.append(n)
        seen.add(n)
    if dups:
        raise CsvError("列名重复: " + ", ".join(dups))


def read_header(path):
    """返回去空白后的表头列表；第一个非空行视为表头。"""
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if _is_blank_row(row):
                continue
            names = [(h or "").strip() for h in row]
            _check_names(names)
            return names
    raise CsvError("CSV 文件为空")


def _parse_date(raw, lineno):
    s = (raw or "").strip()
    if not _DATE_RE.fullmatch(s):
        raise CsvError(f"第 {lineno} 行观测日 {raw!r} 不是 YYYY-MM-DD 格式")
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        raise CsvError(f"第 {lineno} 行观测日 {raw!r} 不是有效日期")


def _parse_cell(raw, typ, lineno, colname):
    if typ == "text":
        return raw if raw != "" else None
    s = (raw or "").strip()
    if s == "":
        return None
    if not _INT_RE.fullmatch(s):
        raise CsvError(f"第 {lineno} 行列「{colname}」的值 {raw!r} 不是非负整数")
    v = int(s)
    if v > UINT64_MAX:
        raise CsvError(f"第 {lineno} 行列「{colname}」的值 {raw!r} 超出 uint64 上限")
    return v


def parse_rows(path, col_types):
    """按声明类型解析数据行。col_types 对应第 2..N 列（鸟种列）。

    返回 (dates, columns_values)；任一单元格非法即抛 CsvError（整批不发布）。
    """
    names = read_header(path)
    ncols = len(names)
    if len(col_types) != ncols - 1:
        raise CsvError(f"声明的列类型数（{len(col_types)}）与鸟种列数（{ncols - 1}）不一致")
    dates = []
    out = [[] for _ in col_types]
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        lineno = 0
        header_seen = False
        for row in reader:
            lineno += 1
            if _is_blank_row(row):
                continue
            if not header_seen:
                header_seen = True  # 第一个非空行是表头，read_header 已校验
                continue
            if len(row) != ncols:
                raise CsvError(f"第 {lineno} 行字段数 {len(row)} 与表头 {ncols} 不一致")
            dates.append(_parse_date(row[0], lineno))
            for i, typ in enumerate(col_types):
                out[i].append(_parse_cell(row[i + 1], typ, lineno, names[i + 1]))
    if not dates:
        raise CsvError("CSV 没有数据行")
    return dates, out
