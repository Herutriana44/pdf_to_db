"""
PDF to Database Extractor
Extracts tables from PDF documents with multi-page table detection.
Uses pymupdf for metadata and pdfplumber for table extraction.
"""
import csv
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Any

import fitz  # pymupdf
import pdfplumber

logger = logging.getLogger(__name__)

# Kolom metadata tambahan (setelah page + bbox)
META_TENTANG_COLS = ("tentang", "tipe_regional", "kelas_rs", "tipe_rs", "rawat_jalan")


def _normalize_row(row: List[Any]) -> Tuple[str, ...]:
    """Normalize row for header comparison (strip, lower, collapse whitespace)."""
    return tuple(
        str(c or "").strip().lower().replace("\n", " ").replace("\r", "")
        for c in row
    )


def _headers_match(header_a: List[Any], header_b: List[Any], threshold: float = 0.8) -> bool:
    """
    Check if two table headers are similar enough to consider them the same table.
    Uses threshold for similarity (default 80% match).
    """
    if not header_a or not header_b:
        return False
    norm_a = _normalize_row(header_a)
    norm_b = _normalize_row(header_b)
    if len(norm_a) != len(norm_b):
        return False
    matches = sum(1 for a, b in zip(norm_a, norm_b) if a and b and a == b)
    return matches / max(len(norm_a), 1) >= threshold


def _words_to_multiline_text(words: List[dict]) -> str:
    """Gabungkan kata-kata menjadi teks multi-baris (urut baris lalu kiri-ke-kanan)."""
    if not words:
        return ""
    words = sorted(words, key=lambda w: (w.get("top", 0), w.get("x0", 0)))
    lines: List[List[dict]] = []
    current: List[dict] = [words[0]]
    for w in words[1:]:
        if abs(w.get("top", 0) - current[-1].get("top", 0)) < 4:
            current.append(w)
        else:
            lines.append(sorted(current, key=lambda x: x.get("x0", 0)))
            current = [w]
    lines.append(sorted(current, key=lambda x: x.get("x0", 0)))
    return "\n".join(" ".join(x.get("text", "") for x in ln).strip() for ln in lines if ln).strip()


def _extract_text_above_table(
    page: Any,
    table_bbox: Tuple[float, float, float, float],
    words: List[dict],
) -> str:
    """
    Ambil teks di atas tepi atas tabel (biasanya judul/region/kelas).
    Koordinat pdfplumber: y ke bawah; teks di atas tabel punya bottom <= top tabel.
    """
    if not words:
        return ""
    x0, top, x1, bottom = table_bbox if len(table_bbox) >= 4 else (0, 0, 0, 0)
    if x0 == x1 == top == bottom == 0:
        return _extract_text_top_region(page, words)

    margin = 8.0
    above: List[dict] = []
    for w in words:
        try:
            w_bottom = float(w.get("bottom", 0))
        except (TypeError, ValueError):
            continue
        if w_bottom <= float(top) + 1.5:
            wx0, wx1 = float(w.get("x0", 0)), float(w.get("x1", 0))
            if wx1 >= x0 - margin and wx0 <= x1 + margin:
                above.append(w)

    if not above:
        return ""

    return _words_to_multiline_text(above)


def _extract_text_top_region(page: Any, words: List[dict]) -> str:
    """Fallback jika bbox tabel tidak valid: ambil teks di ~35% tinggi halaman atas."""
    if not words:
        return ""
    h = float(getattr(page, "height", 0) or 0)
    if h <= 0:
        return _words_to_multiline_text(words[: min(80, len(words))])
    cutoff = h * 0.35
    top_words = [w for w in words if float(w.get("bottom", 0)) <= cutoff]
    return _words_to_multiline_text(top_words) if top_words else ""


def parse_tentang_metadata(tentang: str) -> dict:
    """
    Parse teks konteks (tentang) menjadi field terstruktur.
    Contoh: REGIONAL 5, KELAS A, SWASTA, RAWAT JALAN.
    """
    raw = (tentang or "").replace("\r", "\n")
    single_line = re.sub(r"\s+", " ", raw.replace("\n", " ")).strip()
    t = single_line.upper()

    tipe_regional = ""
    m = re.search(r"REGIONAL\s*(\d+)", t, re.I)
    if m:
        tipe_regional = f"REGIONAL {m.group(1)}"

    kelas_rs = ""
    m = re.search(r"KELAS\s+([A-Z])\b", t, re.I)
    if m:
        kelas_rs = m.group(1).upper()

    tipe_rs = ""
    if re.search(r"\bSWASTA\b", t, re.I):
        tipe_rs = "SWASTA"
    elif re.search(r"\bPEMERINTAH\b", t, re.I):
        tipe_rs = "PEMERINTAH"
    elif re.search(r"\bBLU\b", t, re.I):
        tipe_rs = "BLU"

    rawat_jalan = ""
    if re.search(r"RAWAT\s+JALAN", t, re.I):
        rawat_jalan = "RAWAT JALAN"
    elif re.search(r"RAWAT\s+INAP", t, re.I):
        rawat_jalan = "RAWAT INAP"
    elif re.search(r"\bIGD\b", t, re.I):
        rawat_jalan = "IGD"

    return {
        "tentang": single_line,
        "tipe_regional": tipe_regional,
        "kelas_rs": kelas_rs,
        "tipe_rs": tipe_rs,
        "rawat_jalan": rawat_jalan,
    }


def _slug_for_filename(tentang: str, fallback_stem: str) -> str:
    """Nama file aman dari teks tentang (tanpa newline)."""
    s = (tentang or "").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[^\w\s\-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s\-]+", "_", s).strip("_")
    if not s or len(s) < 2:
        s = re.sub(r"[^\w\-]", "_", fallback_stem, flags=re.UNICODE) or "table"
    if len(s) > 100:
        s = s[:100].rstrip("_")
    return s


def _deduplicate_chars(s: str) -> str:
    """
    Fix double-character extraction artifact from PDF.
    Example: 'DDEESSKKRRIIPPSSII KKOODDEE IINNAA--CCBBGG' -> 'DESKRIPSI KODE INA-CBG'
    Detects alternating duplicate pattern (s[0::2] == s[1::2]) and collapses to single chars.
    Handles multiple levels of duplication (e.g. 4x -> 2x -> 1x).

    Proses per kata (split by whitespace) karena spasi antar kata sering tidak diduplikasi
    oleh ekstraktor PDF, sehingga string gabungan punya panjang ganjil atau pola putus.
    Contoh gagal: 'PPRROOSSEEDDUURR BBEESSAARR' (spasi tunggal) -> proses per kata berhasil.
    """
    if not s:
        return s

    def _dedup_token(t: str) -> str:
        while len(t) >= 2 and len(t) % 2 == 0 and t[0::2] == t[1::2]:
            t = t[0::2]
        return t

    # Pisah by whitespace, dedup tiap token, gabung lagi (preserve separator)
    parts = re.split(r"(\s+)", s)
    return "".join(_dedup_token(p) if not re.match(r"^\s+$", p) else p for p in parts)


def _normalize_cell(cell: Any) -> str:
    """Normalize cell value (whitespace). Dedup applied in postprocess_csv_file."""
    if cell is None:
        return ""
    s = str(cell).strip().replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    return re.sub(r"\s+", " ", s)


def _postprocess_csv_file(csv_path: str, log_callback: Optional[Callable[[str], None]] = None) -> None:
    """
    Postprocess CSV file: read row by row, apply deduplication to each cell, write back.
    Fixes double-character extraction artifacts (e.g. DDEESSKKRRIIPPSSII -> DESKRIPSI).
    """
    def log(msg: str) -> None:
        if log_callback:
            log_callback(msg)
        logger.info(msg)

    output_dir = os.path.dirname(csv_path)
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv", prefix="pdf2db_", dir=output_dir)
    try:
        with os.fdopen(temp_fd, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            with open(csv_path, "r", encoding="utf-8", errors="ignore") as in_f:
                reader = csv.reader(in_f)
                row_count = 0
                for row in reader:
                    processed = []
                    for cell in row:
                        s = str(cell).strip().replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
                        s = re.sub(r"\s+", " ", s)
                        s = _deduplicate_chars(s)
                        processed.append(s)
                    writer.writerow(processed)
                    row_count += 1
        os.replace(temp_path, csv_path)
        log(f"  [Postprocess] {os.path.basename(csv_path)}: {row_count} baris diproses")
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise e


def deduplicate_chars_columns(
    csv_path: str,
    column_indices: List[int],
    log_callback: Optional[Callable[[str], None]] = None,
) -> None:
    """
    Apply _deduplicate_chars only to specified columns (second post-processing).
    column_indices: 0-based column indices. Empty list = no-op.
    """
    if not column_indices:
        return

    def log(msg: str) -> None:
        if log_callback:
            log_callback(msg)
        logger.info(msg)

    output_dir = os.path.dirname(csv_path)
    temp_fd, temp_path = tempfile.mkstemp(suffix=".csv", prefix="pdf2db_", dir=output_dir)
    try:
        with os.fdopen(temp_fd, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.writer(out_f)
            with open(csv_path, "r", encoding="utf-8", errors="ignore") as in_f:
                reader = csv.reader(in_f)
                row_count = 0
                for row in reader:
                    processed = []
                    for i, cell in enumerate(row):
                        s = str(cell).strip().replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
                        s = re.sub(r"\s+", " ", s)
                        if i in column_indices:
                            s = _deduplicate_chars(s)
                        processed.append(s)
                    writer.writerow(processed)
                    row_count += 1
        os.replace(temp_path, csv_path)
        log(f"  [Deduplicate] {os.path.basename(csv_path)}: kolom {column_indices}, {row_count} baris")
    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise e


def split_pdf_to_pages(
    pdf_path: str,
    pages_dir: str,
    log_callback: Optional[Callable[[str], None]] = None,
) -> List[str]:
    """
    Split PDF into individual page files (1 page per file).
    Returns list of paths to page PDF files in order.
    """
    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            log_callback(msg)

    os.makedirs(pages_dir, exist_ok=True)
    base_name = Path(pdf_path).stem
    page_paths: List[str] = []

    log(f"[Split] Membuka PDF: {os.path.basename(pdf_path)}")
    doc = fitz.open(pdf_path)
    total = len(doc)
    log(f"[Split] Total halaman: {total}")

    for i in range(total):
        page_path = os.path.join(pages_dir, f"{base_name}_page_{i + 1:05d}.pdf")
        single = fitz.open()
        single.insert_pdf(doc, from_page=i, to_page=i)
        single.save(page_path)
        single.close()
        page_paths.append(page_path)
        if (i + 1) % 50 == 0 or i == total - 1:
            log(f"[Split] Halaman {i + 1}/{total} selesai")
    doc.close()

    log(f"[Split] Selesai. {len(page_paths)} file halaman dibuat.")
    return page_paths


def extract_metadata_pymupdf(pdf_path: str) -> dict:
    """Extract PDF metadata using pymupdf."""
    meta = {}
    try:
        doc = fitz.open(pdf_path)
        meta["page_count"] = len(doc)
        if doc.metadata:
            meta["title"] = doc.metadata.get("title", "")
            meta["author"] = doc.metadata.get("author", "")
            meta["subject"] = doc.metadata.get("subject", "")
            meta["creator"] = doc.metadata.get("creator", "")
            meta["producer"] = doc.metadata.get("producer", "")
        doc.close()
    except Exception as e:
        meta["error"] = str(e)
    return meta


def extract_tables_from_pdf(
    pdf_path: str,
    output_dir: str,
    pages_dir: str,
    table_settings: Optional[dict] = None,
    log_callback: Optional[Callable[[str], None]] = None,
) -> List[dict]:
    """
    Extract tables from PDF with multi-page detection (Repeated Header Check).
    First splits PDF into 1 page per file, then extracts tables from each page.
    Returns list of metadata dicts for each extracted table CSV.
    """
    def log(msg: str) -> None:
        logger.info(msg)
        if log_callback:
            log_callback(msg)

    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.basename(pdf_path)
    base_name = Path(filename).stem
    table_settings = table_settings or {}

    # Phase 0: Split PDF into 1 page per file
    log("[Phase 0] Memecah PDF menjadi 1 halaman per file...")
    page_paths = split_pdf_to_pages(pdf_path, pages_dir, log_callback=log_callback)

    # Phase 1: Extract tables per page with position metadata (bbox) + teks di atas tabel
    log("[Phase 1] Mengekstrak tabel dari setiap halaman (dengan metadata posisi + tentang)...")
    page_tables: List[Tuple[int, Tuple[float, float, float, float], List[List[Any]], str]] = []
    total_pages = len(page_paths)
    for page_num, page_path in enumerate(page_paths, start=1):
        with pdfplumber.open(page_path) as pdf:
            if len(pdf.pages) == 0:
                continue
            page = pdf.pages[0]
            words = page.extract_words() or []
            found = page.find_tables(table_settings=table_settings)
            for table_obj in found:
                try:
                    table_data = table_obj.extract()
                    bbox = table_obj.bbox
                except Exception:
                    continue
                if table_data and len(table_data) >= 2 and bbox:
                    tentang = _extract_text_above_table(page, bbox, words)
                    page_tables.append((page_num, bbox, table_data, tentang))
            if not found:
                for table in page.extract_tables(table_settings=table_settings):
                    if table and len(table) >= 2:
                        tentang = _extract_text_top_region(page, words) if words else ""
                        page_tables.append((page_num, (0, 0, 0, 0), table, tentang))
        if page_num % 50 == 0 or page_num == total_pages:
            log(f"[Phase 1] Ekstraksi halaman {page_num}/{total_pages} selesai (tabel terdeteksi: {len(page_tables)})")

    # Cleanup: remove split page files
    log("[Cleanup] Menghapus file halaman sementara...")
    for p in page_paths:
        try:
            os.remove(p)
        except OSError:
            pass
    try:
        if os.path.isdir(pages_dir) and not os.listdir(pages_dir):
            os.rmdir(pages_dir)
    except OSError:
        pass

    # Phase 2: Detect multi-page tables (Repeated Header Check) & merge
    log("[Phase 2] Mendeteksi dan menggabungkan multi-page table...")
    merged_tables: List[dict] = []
    for page_num, bbox, table, tentang in page_tables:
        header = table[0]
        data_rows = table[1:]
        x0, top, x1, bottom = bbox if len(bbox) >= 4 else (0, 0, 0, 0)

        found = False
        for mt in merged_tables:
            if _headers_match(mt["header"], header) and mt["end_page"] == page_num - 1:
                for row in data_rows:
                    mt["rows"].append((page_num, x0, top, x1, bottom, row))
                mt["end_page"] = page_num
                found = True
                break

        if not found:
            merged_tables.append({
                "header": header,
                "rows": [(page_num, x0, top, x1, bottom, row) for row in data_rows],
                "start_page": page_num,
                "end_page": page_num,
                "tentang": tentang or "",
            })

    log(f"[Phase 2] Selesai. {len(merged_tables)} tabel terdeteksi (setelah merge).")

    # Phase 3: Tulis CSV dengan kolom tentang + rename file berdasarkan tentang + urutan 3 digit
    log("[Phase 3] Menulis hasil ke CSV (kolom tentang + penamaan file)...")
    all_extracted: List[dict] = []
    for i, mt in enumerate(merged_tables):
        seq = i + 1
        meta = parse_tentang_metadata(mt.get("tentang") or "")
        slug = _slug_for_filename(meta["tentang"], base_name)
        csv_name = f"{slug}_{seq:03d}.csv"
        csv_path = os.path.join(output_dir, csv_name)
        if os.path.exists(csv_path):
            csv_name = f"{base_name}_{slug}_{seq:03d}.csv"
            csv_path = os.path.join(output_dir, csv_name)
        _write_merged_table(mt, filename, csv_path, csv_name, all_extracted, meta)
        log(f"  -> {csv_name} (halaman {mt['start_page']}-{mt['end_page']}, {len(mt['rows'])} baris)")

    # Phase 4: Postprocess each CSV (deduplicate chars, row by row)
    log("[Phase 4] Postprocessing: menghapus karakter double...")
    for ext in all_extracted:
        _postprocess_csv_file(ext["csv_path"], log_callback=log_callback)

    log(f"[Selesai] {len(all_extracted)} file CSV berhasil diekstrak.")
    return all_extracted


def _write_merged_table(
    table_data: dict,
    filename: str,
    csv_path: str,
    csv_name: str,
    all_extracted: List[dict],
    meta: Optional[dict] = None,
) -> None:
    """Write a merged table to CSV with metadata posisi tabel, tentang, dan field parse."""
    header = table_data["header"]
    rows = table_data["rows"]
    meta = meta or parse_tentang_metadata(table_data.get("tentang") or "")

    out_header = (
        ["page", "bbox_x0", "bbox_top", "bbox_x1", "bbox_bottom"]
        + list(META_TENTANG_COLS)
        + [_normalize_cell(c) or f"col_{i}" for i, c in enumerate(header)]
    )

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(out_header)
        for row in rows:
            page_num, x0, top, x1, bottom, row_data = row
            out_row = (
                [page_num, round(x0, 1), round(top, 1), round(x1, 1), round(bottom, 1)]
                + [
                    meta.get("tentang", ""),
                    meta.get("tipe_regional", ""),
                    meta.get("kelas_rs", ""),
                    meta.get("tipe_rs", ""),
                    meta.get("rawat_jalan", ""),
                ]
                + [_normalize_cell(c) for c in row_data]
            )
            writer.writerow(out_row)

    all_extracted.append({
        "csv_filename": csv_name,
        "csv_path": csv_path,
        "filename": filename,
        "start_page": table_data["start_page"],
        "end_page": table_data["end_page"],
        "row_count": len(rows),
    })
