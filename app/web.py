"""Flask Web 入口：上传批次（两步：上传 -> 逐列声明类型）、按站点预览、下载统一结构 CSV、重复调查核查。"""
import datetime
import io
import os

from flask import (Flask, abort, flash, redirect, render_template, request,
                   send_file, url_for)

from . import backfill, catalog, csvparse, service, store
from .config import Config

MAX_UPLOAD_BYTES = 64 * 1024 * 1024
DUP_PAGE_SIZE = 20


def create_app(config=None):
    cfg = config or Config()
    service.init_storage(cfg)

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
    app.secret_key = os.environ.get("SECRET_KEY", "bird-lake-dev-secret")
    app.config["_cfg"] = cfg  # 供自检脚本等待补建就绪

    def snapshot(station=None):
        conn = catalog.connect(cfg.db_path)
        try:
            return catalog.snapshot(conn, station)
        finally:
            conn.close()

    @app.get("/")
    def index():
        snap = snapshot()
        stations = {}
        for b in snap["batches"]:
            s = stations.setdefault(
                b["station"], {"station": b["station"], "n_batches": 0, "n_rows": 0})
            s["n_batches"] += 1
            s["n_rows"] += b["n_rows"]
        recent = list(reversed(snap["batches"]))[:50]
        conn = catalog.connect(cfg.db_path)
        try:
            dup_status = catalog.get_dup_status(conn)
        finally:
            conn.close()
        return render_template("index.html", snap=snap,
                               stations=sorted(stations.values(), key=lambda x: x["station"]),
                               recent=recent, dup_status=dup_status)

    @app.get("/upload")
    def upload_form():
        return render_template("upload.html")

    @app.post("/upload")
    def upload():
        station = (request.form.get("station") or "").strip()
        f = request.files.get("file")
        if not station:
            flash("请填写站点", "err")
            return redirect(url_for("upload_form"))
        if f is None or not f.filename:
            flash("请选择 CSV 文件", "err")
            return redirect(url_for("upload_form"))
        token = service.stage_upload(cfg, f.stream)
        try:
            header = service.staged_header(cfg, token)
        except csvparse.CsvError as e:
            service.discard_staged(cfg, token)
            flash(f"CSV 表头无效：{e}", "err")
            return redirect(url_for("upload_form"))
        if store.DATE_COLUMN in header[1:]:
            service.discard_staged(cfg, token)
            flash(f"鸟种列不能使用保留列名 {store.DATE_COLUMN}", "err")
            return redirect(url_for("upload_form"))
        return render_template("declare.html", token=token, station=station,
                               filename=f.filename, date_col=header[0],
                               columns=header[1:])

    @app.post("/publish")
    def publish():
        token = request.form.get("token") or ""
        station = (request.form.get("station") or "").strip()
        filename = request.form.get("filename") or ""
        try:
            path = service.staged_path(cfg, token)
        except ValueError as e:
            flash(str(e), "err")
            return redirect(url_for("upload_form"))
        try:
            header = service.staged_header(cfg, token)
            types = [request.form.get(f"type_{i}", "") for i in range(len(header) - 1)]
            result = service.publish(cfg, station, filename, path, types)
        except (csvparse.CsvError, ValueError) as e:
            flash(f"批次未发布：{e}", "err")
            return redirect(url_for("upload_form"))
        except OSError as e:
            flash(f"批次未发布：写入存储失败（{e}）", "err")
            return redirect(url_for("upload_form"))
        finally:
            service.discard_staged(cfg, token)
        parts = [f"批次 {result['batch_id'][:8]} 已发布：站点「{result['station']}」，{result['n_rows']} 行"]
        for c in result["changes"]:
            if c["change"] == "added":
                parts.append(f"新增列 {c['name']}({c['type']})")
            elif c["change"] == "promoted":
                parts.append(f"列 {c['name']} 类型提升为 text")
        flash("；".join(parts), "ok")
        return redirect(url_for("preview", station=result["station"]))

    @app.get("/preview")
    def preview():
        station = request.args.get("station") or None
        try:
            limit = max(1, min(int(request.args.get("limit", "200")), 5000))
        except ValueError:
            limit = 200
        # 同一次取数：一个读快照内的逻辑结构 + 文件清单
        snap = snapshot(station)
        schema, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
        rows = table.slice(0, limit).to_pylist()
        all_stations = sorted({b["station"] for b in snapshot()["batches"]})
        return render_template("preview.html", station=station, stations=all_stations,
                               columns=schema.names, rows=rows, total=table.num_rows,
                               limit=limit, version=snap["schema_version"])

    @app.get("/download.csv")
    def download_csv():
        station = request.args.get("station") or None
        snap = snapshot(station)
        _schema, table = store.load_unified(cfg.parquet_dir, snap["columns"], snap["batches"])
        data = store.table_to_csv_bytes(table)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"birds_{station or 'all'}_{ts}.csv"
        return send_file(io.BytesIO(data), mimetype="text/csv",
                         as_attachment=True, download_name=name)

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    # ---------- 重复调查核查 ----------

    def _all_stations():
        return sorted({b["station"] for b in snapshot()["batches"]})

    @app.get("/duplicates")
    def duplicates():
        station = (request.args.get("station") or "").strip() or None
        try:
            page = max(1, int(request.args.get("page", "1")))
        except ValueError:
            page = 1
        conn = catalog.connect(cfg.db_path)
        try:
            status = catalog.get_dup_status(conn)
            total = catalog.count_dup_groups(conn, station) if status["ready"] else 0
            pages = max(1, (total + DUP_PAGE_SIZE - 1) // DUP_PAGE_SIZE)
            page = min(page, pages)
            offset = (page - 1) * DUP_PAGE_SIZE
            groups = (catalog.list_dup_groups(conn, station, offset, DUP_PAGE_SIZE)
                      if status["ready"] else [])
            # 组内行在同一读快照内逐组取（每组行数小）
            for g in groups:
                g["rows"] = catalog.list_group_rows(conn, g["station"], g["obs_date"])
        finally:
            conn.close()
        return render_template(
            "duplicates.html", status=status, groups=groups, station=station,
            stations=_all_stations(), page=page, pages=pages, total=total,
            page_size=DUP_PAGE_SIZE)

    @app.post("/duplicates/mark")
    def duplicates_mark():
        station = (request.form.get("station") or "").strip()
        obs_date = (request.form.get("obs_date") or "").strip()
        try:
            page = max(1, int(request.form.get("page", "1")))
        except ValueError:
            page = 1
        try:
            expected_n = int(request.form.get("expected_n", ""))
        except (TypeError, ValueError):
            abort(400)
        if not station or not _valid_iso_date(obs_date) or expected_n < 1:
            abort(400)
        conn = catalog.connect(cfg.db_path)
        try:
            status = catalog.get_dup_status(conn)
            if not status["ready"]:
                flash("历史重复索引仍在补建中，暂不能标记核查，请稍后再试。", "err")
                return redirect(url_for("duplicates"))
            n = catalog.mark_group_checked(
                conn, station, obs_date,
                datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
                expected_n=expected_n)
        finally:
            conn.close()
        if n is None:
            flash("该组在页面打开后已发生变化（有新观测加入或标记已被撤销），"
                  "请刷新页面重新核查，未写入标记。", "err")
        else:
            flash(f"已标记核查：{station} / {obs_date}（组内 {n} 条观测）", "ok")
        return redirect(url_for("duplicates",
                                station=request.form.get("station_filter") or "",
                                page=page))

    @app.post("/duplicates/unmark")
    def duplicates_unmark():
        station = (request.form.get("station") or "").strip()
        obs_date = (request.form.get("obs_date") or "").strip()
        if not station or not _valid_iso_date(obs_date):
            abort(400)
        conn = catalog.connect(cfg.db_path)
        try:
            ok = catalog.unmark_group(conn, station, obs_date)
        finally:
            conn.close()
        flash("已撤销核查标记" if ok else "该组本就没有核查标记",
              "ok" if ok else "err")
        return redirect(url_for(
            "duplicates", station=request.form.get("station_filter") or ""))

    @app.errorhandler(413)
    def too_large(_e):
        flash("文件过大（上限 64MB）", "err")
        return redirect(url_for("upload_form"))

    return app


def _valid_iso_date(s):
    try:
        datetime.date.fromisoformat(s)
    except ValueError:
        return False
    return True


app = create_app()
