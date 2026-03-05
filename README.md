# PDF to Database

Program Python untuk mengonversi dokumen PDF menjadi database (format CSV) dengan ekstraksi tabel multi-halaman. Mendukung dokumen hingga 700-800 halaman.

## Fitur

- **Ekstraksi tabel** menggunakan `pdfplumber` dan `pymupdf`
- **Deteksi multi-page table** dengan Repeated Header Check (tabel yang berlanjut di halaman berikutnya digabung)
- **Web interface** (Flask) untuk upload, proses, cari, lihat, unduh, dan rename file hasil ekstraksi

## Instalasi

```bash
pip install -r requirements.txt
```

## Menjalankan

```bash
python app.py
```

Buka http://localhost:5000 di browser.

## Flow (sesuai diagram)

1. **User Access Web** → Home
2. **Documents Processing** → Upload PDF → **Split PDF menjadi 1 halaman per file** → Extract Tables per Page (pdfplumber) → Detect Multi-Page Table (Repeated Header Check) → Merge & Normalize Structure → Streaming Write to CSV
3. **Result Data** → Cari, lihat, unduh, rename file hasil ekstraksi

UI loading ditampilkan saat processing berjalan (dokumen besar dapat memakan waktu beberapa menit).

## Struktur

```
pdf_to_db/
├── app.py              # Flask web app
├── config.py           # Konfigurasi
├── pdf_extractor.py    # Ekstraksi PDF (pymupdf + pdfplumber)
├── requirements.txt
├── uploads/            # File PDF sementara (untuk proses)
├── output/             # Output CSV hasil ekstraksi
└── templates/          # HTML templates
```

## Rename File

File hasil ekstraksi dapat di-rename dari halaman **Result Data** (di luar Documents Processing page) dengan tombol "Rename" pada setiap baris.
