"""
Script untuk menjalankan PDF to DB di Google Colab dengan ngrok port forwarding.
Jalankan di Colab: !python run_colab.py
Atau copy-paste isi script ini ke cell Colab.
"""
import os
import subprocess
import sys
import time

# ============ KONFIGURASI ============
GITHUB_REPO = "https://github.com/Herutriana44/pdf_to_db.git"
NGROK_AUTH_TOKEN = os.environ.get("NGROK_AUTH_TOKEN", "")  # Set di Colab: os.environ["NGROK_AUTH_TOKEN"] = "your_token"
FLASK_PORT = 5000

# ============ 1. Clone Repository ============
def clone_repo():
    if os.path.isdir("pdf_to_db"):
        print("[OK] Folder pdf_to_db sudah ada.")
        os.chdir("pdf_to_db")
        return
    print("[1/5] Cloning repository...")
    subprocess.run(["git", "clone", GITHUB_REPO], check=True)
    os.chdir("pdf_to_db")
    print("[OK] Clone selesai.")

# ============ 2. Install Dependencies ============
def install_deps():
    print("[2/5] Menginstal dependencies...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"], check=True)
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pyngrok"], check=True)
    print("[OK] Dependencies terinstal.")

# ============ 3. Setup Ngrok ============
def setup_ngrok():
    print("[3/5] Setup ngrok...")
    try:
        from pyngrok import ngrok
        if NGROK_AUTH_TOKEN:
            ngrok.set_auth_token(NGROK_AUTH_TOKEN)
        public_url = ngrok.connect(FLASK_PORT)
        print(f"[OK] Ngrok tunnel aktif: {public_url}")
        return str(public_url)
    except Exception as e:
        print(f"[ERROR] Ngrok gagal: {e}")
        print("\nUntuk menggunakan ngrok, daftar gratis di https://ngrok.com dan set token:")
        print('  os.environ["NGROK_AUTH_TOKEN"] = "your_token_from_ngrok_dashboard"')
        return None

# ============ 4. Run Flask App ============
def run_flask():
    import threading
    def _run():
        from app import app
        app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    time.sleep(3)
    print("[OK] Flask app berjalan di background.")

# ============ MAIN ============
def main():
    original_dir = os.getcwd()
    try:
        clone_repo()
        install_deps()
        run_flask()
        public_url = setup_ngrok()
        print("\n" + "="*60)
        print("PDF to DB siap digunakan!")
        if public_url:
            print(f"Akses web: {public_url}")
            print("="*60)
        else:
            print("Akses lokal: http://localhost:5000")
            print("(Untuk akses dari luar, set NGROK_AUTH_TOKEN)")
            print("="*60)
        print("\nTekan Ctrl+C untuk menghentikan.")
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nDihentikan.")
    finally:
        os.chdir(original_dir)

if __name__ == "__main__":
    main()
