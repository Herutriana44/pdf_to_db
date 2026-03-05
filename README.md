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

## Menjalankan di Google Colab (dengan ngrok)

1. Buka [Google Colab](https://colab.research.google.com) dan buat notebook baru
2. Daftar gratis di [ngrok](https://ngrok.com) → Dashboard → Copy auth token
3. Jalankan cell berikut:

```python
# Set token ngrok (dapatkan di https://dashboard.ngrok.com/get-started/your-authtoken)
import os
os.environ["NGROK_AUTH_TOKEN"] = "your_token_here"

# Clone, install, run
!git clone https://github.com/Herutriana44/pdf_to_db.git
%cd pdf_to_db
!pip install -q -r requirements.txt pyngrok

# Jalankan Flask di background
import threading, time
def run(): from app import app; app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
threading.Thread(target=run, daemon=True).start()
time.sleep(4)

# Port forwarding dengan ngrok
from pyngrok import ngrok
ngrok.set_auth_token(os.environ["NGROK_AUTH_TOKEN"])
url = ngrok.connect(5000)
print(f"Akses web: {url}")
```

4. Buka URL yang ditampilkan untuk mengakses aplikasi dari browser mana pun

## Flow (sesuai diagram)

1. **User Access Web** → Home
2. **Documents Processing** → Upload PDF → **Split PDF menjadi 1 halaman per file** → Extract Tables per Page (pdfplumber) → Detect Multi-Page Table (Repeated Header Check) → Merge & Normalize Structure → Streaming Write to CSV
3. **Result Data** → Cari, lihat, unduh, rename file hasil ekstraksi

UI loading ditampilkan saat processing berjalan (dokumen besar dapat memakan waktu beberapa menit).

## Struktur

```
pdf_to_db/
├── app.py              # Flask web app
├── colab_run.ipynb     # Notebook untuk Google Colab
├── run_colab.py        # Script alternatif untuk Colab
├── config.py           # Konfigurasi
├── pdf_extractor.py    # Ekstraksi PDF (pymupdf + pdfplumber)
├── requirements.txt
├── uploads/            # File PDF sementara (untuk proses)
├── output/             # Output CSV hasil ekstraksi
└── templates/          # HTML templates
```

## Rename File

File hasil ekstraksi dapat di-rename dari halaman **Result Data** (di luar Documents Processing page) dengan tombol "Rename" pada setiap baris.
