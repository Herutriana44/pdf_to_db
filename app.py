"""
Flask web application for PDF to Database extraction.
"""
import json
import logging
import os
import queue
import threading
import time
from pathlib import Path
from werkzeug.utils import secure_filename

from flask import (
    Flask,
    Response,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    send_file,
)
from config import UPLOAD_FOLDER, PAGES_FOLDER, OUTPUT_FOLDER, ALLOWED_EXTENSIONS, MAX_CONTENT_LENGTH
from pdf_extractor import extract_tables_from_pdf, extract_metadata_pymupdf, deduplicate_chars_columns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
app.config["SECRET_KEY"] = os.urandom(24)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def get_extracted_metadata() -> list:
    """Scan output folder and build index from CSV files."""
    import csv as csv_module
    results = []
    if not os.path.isdir(OUTPUT_FOLDER):
        return results

    for f in sorted(os.listdir(OUTPUT_FOLDER)):
        if not f.endswith(".csv"):
            continue
        path = os.path.join(OUTPUT_FOLDER, f)
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fp:
                reader = csv_module.reader(fp)
                rows = list(reader)
            results.append({
                "csv_filename": f,
                "row_count": max(0, len(rows) - 1),
            })
        except Exception:
            results.append({"csv_filename": f, "row_count": 0})
    return results


@app.route("/")
def index():
    """Home page - links to processing and result data."""
    return render_template("index.html")


def _run_extraction(filepath: str, pages_dir: str, log_queue: queue.Queue) -> tuple:
    """Run extraction in thread, put logs in queue. Returns (success, result_or_error)."""
    try:
        extracted = extract_tables_from_pdf(
            filepath,
            OUTPUT_FOLDER,
            pages_dir,
            log_callback=lambda msg: log_queue.put(("log", msg)),
        )
        log_queue.put(("done", {"count": len(extracted)}))
        return (True, extracted)
    except Exception as e:
        log_queue.put(("error", str(e)))
        return (False, str(e))


@app.route("/processing", methods=["GET", "POST"])
def processing():
    """Documents processing page - upload and extract PDF."""
    if request.method == "POST":
        # Check if streaming (fetch with Accept: text/event-stream)
        want_stream = "text/event-stream" in request.headers.get("Accept", "")

        if "pdf_file" not in request.files:
            if want_stream:
                return jsonify({"error": "No file selected."}), 400
            flash("No file selected.", "error")
            return redirect(request.url)

        file = request.files["pdf_file"]
        if file.filename == "":
            if want_stream:
                return jsonify({"error": "No file selected."}), 400
            flash("No file selected.", "error")
            return redirect(request.url)

        if not allowed_file(file.filename):
            if want_stream:
                return jsonify({"error": "Only PDF files are allowed."}), 400
            flash("Only PDF files are allowed.", "error")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        if want_stream:
            # Stream logs via SSE
            log_queue = queue.Queue()
            pages_dir = os.path.join(PAGES_FOLDER, f"{Path(filename).stem}_{int(time.time())}")

            def generate():
                def run():
                    try:
                        _run_extraction(filepath, pages_dir, log_queue)
                    finally:
                        if os.path.exists(filepath):
                            try:
                                os.remove(filepath)
                            except OSError:
                                pass
                    log_queue.put(None)  # Sentinel

                t = threading.Thread(target=run)
                t.start()

                while True:
                    try:
                        item = log_queue.get(timeout=0.5)
                    except queue.Empty:
                        yield f"data: {json.dumps({'type': 'ping'})}\n\n"
                        continue
                    if item is None:
                        break
                    typ, val = item
                    if typ == "log":
                        payload = {"type": "log", "msg": val}
                    elif isinstance(val, dict):
                        payload = {"type": typ, **val}
                    else:
                        payload = {"type": typ, "error": val}
                    yield f"data: {json.dumps(payload)}\n\n"

            return Response(
                generate(),
                mimetype="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        # Standard form submit
        try:
            pages_dir = os.path.join(PAGES_FOLDER, f"{Path(filename).stem}_{int(time.time())}")
            extracted = extract_tables_from_pdf(filepath, OUTPUT_FOLDER, pages_dir)
            app.logger.info("Extraction complete: %d table(s)", len(extracted))
            flash(f"Extraction complete. {len(extracted)} table(s) extracted.", "success")
            return redirect(url_for("results"))
        except Exception as e:
            app.logger.exception("Extraction failed")
            flash(f"Extraction failed: {str(e)}", "error")
            return redirect(request.url)
        finally:
            if os.path.exists(filepath):
                os.remove(filepath)

    return render_template("processing.html")


@app.route("/results")
def results():
    """Result data page - search and view extracted data."""
    data = get_extracted_metadata()
    query = request.args.get("q", "").strip().lower()
    if query:
        data = [
            d for d in data
            if query in (d.get("csv_filename") or "").lower()
        ]
    return render_template("results.html", data=data, search_query=query)


@app.route("/results/rename", methods=["POST"])
def rename_file():
    """Rename extracted CSV file (outside processing page)."""
    data = request.get_json() or request.form
    old_name = (data.get("old_name") or data.get("csv_filename") or "").strip()
    new_name = (data.get("new_name") or "").strip()

    if not old_name or not new_name:
        return jsonify({"success": False, "error": "Missing old_name or new_name"}), 400

    if not new_name.endswith(".csv"):
        new_name += ".csv"

    new_name = secure_filename(new_name)
    if not new_name:
        return jsonify({"success": False, "error": "Invalid new filename"}), 400

    old_path = os.path.join(OUTPUT_FOLDER, old_name)
    new_path = os.path.join(OUTPUT_FOLDER, new_name)

    if not os.path.isfile(old_path):
        return jsonify({"success": False, "error": "File not found"}), 404

    if os.path.exists(new_path) and os.path.normpath(old_path) != os.path.normpath(new_path):
        return jsonify({"success": False, "error": "Target filename already exists"}), 400

    try:
        os.rename(old_path, new_path)
        return jsonify({"success": True, "new_name": new_name})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/results/download/<path:filename>")
def download_file(filename):
    """Download a CSV file."""
    path = os.path.join(OUTPUT_FOLDER, secure_filename(filename))
    if not os.path.isfile(path):
        return "File not found", 404
    return send_file(path, as_attachment=True, download_name=filename)


@app.route("/results/view/<path:filename>/deduplicate", methods=["POST"])
def deduplicate_columns(filename):
    """Apply _deduplicate_chars to selected column(s) (second post-processing)."""
    path = os.path.join(OUTPUT_FOLDER, secure_filename(filename))
    if not os.path.isfile(path):
        flash("File not found.", "error")
        return redirect(url_for("view_file", filename=filename))

    column_val = request.form.get("column")
    if not column_val:
        flash("Pilih kolom yang ingin dideduplikasi.", "error")
        return redirect(url_for("view_file", filename=filename))

    try:
        import csv as csv_module
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv_module.reader(f)
            header = next(reader, [])
        num_cols = len(header)

        if column_val == "all":
            indices = list(range(num_cols))
        else:
            idx = int(column_val)
            if idx < 0 or idx >= num_cols:
                raise ValueError("Index kolom tidak valid")
            indices = [idx]

        deduplicate_chars_columns(path, indices)
        flash("Deduplikasi karakter berhasil diterapkan.", "success")
    except (ValueError, IndexError) as e:
        flash(f"Kolom tidak valid: {e}", "error")
    except Exception as e:
        app.logger.exception("Deduplicate failed")
        flash(f"Gagal: {str(e)}", "error")

    return redirect(url_for("view_file", filename=filename))


@app.route("/results/view/<path:filename>")
def view_file(filename):
    """View CSV content with search filter and pagination."""
    path = os.path.join(OUTPUT_FOLDER, secure_filename(filename))
    if not os.path.isfile(path):
        return "File not found", 404

    search_query = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(10, int(request.args.get("per_page", 50))))

    data_rows = []
    header = []
    total_rows = 0
    try:
        import csv as csv_module
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv_module.reader(f)
            all_rows = list(reader)

        if not all_rows:
            pass
        else:
            header = all_rows[0]
            if search_query:
                q_lower = search_query.lower()
                data_rows = [
                    row for row in all_rows[1:]
                    if any(q_lower in str(c or "").lower() for c in row)
                ]
            else:
                data_rows = all_rows[1:]

        total_rows = len(data_rows)
    except Exception as e:
        return str(e), 500

    total_pages = (total_rows + per_page - 1) // per_page if total_rows else 1
    page = min(page, total_pages)
    start = (page - 1) * per_page
    end = start + per_page
    page_rows = data_rows[start:end]

    return render_template(
        "view_csv.html",
        filename=filename,
        header=header,
        rows=page_rows,
        search_query=search_query,
        total_rows=total_rows,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
