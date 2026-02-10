# APEPDCL Data Scraper

A robust, multi-threaded Streamlit web application for bulk scraping billing/consumer data from the APEPDCL portal using Selenium. Supports pause/resume, restart, retry gaps, and exports to Excel/CSV with full job history.

## Features

- 📥 **Upload Excel** with consumer service numbers (SCNO) and other columns
- 🧹 **Clean and preprocess** data (Full Address merge, column mapping)
- 🚀 **Parallel scraping** with up to 100+ workers (configurable)
- ⏸️ **Pause/Resume/Stop/Restart** controls with crash recovery
- 📊 **Scrap History** with per-job progress, preview, and download
- ⬇️ **Download results** as Excel or CSV (including failed rows info)
- 🔄 **Retry unscraped rows** after completion
- 💾 **SQLite persistence** for jobs, results, and failures
- 🛡️ **Robust driver recovery** and internet wait logic

## Quick Start

### Prerequisites

- Python 3.9+ (recommended 3.11)
- Google Chrome/Chromium browser
- Windows/Linux (tested on both)

### Installation

1. Clone or download this project
2. Create and activate a virtual environment:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
3. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

### Running the app

```powershell
python -m streamlit run streamlit_app.py
```

Open the URL shown (usually `http://localhost:8501`).

## How to Use

### 1. Clean Raw Data
- Upload an Excel file with consumer data.
- Map columns (SCNO, NAME, Full Address, etc.).
- Clean and merge addresses.
- Save to database for scraping.

### 2. Scrap Data
- Choose a cleaned file from history.
- Select the SCNO column to scrape.
- Set number of workers (start low; increase if your PC supports it).
- Click **Start Scraping**.
- Use Pause/Resume/Stop as needed.

### 3. Scrap History
- View all scraping jobs, progress, and status.
- Preview first 30 rows.
- Download full results (Excel/CSV) or unscraped rows.
- Restart or retry gaps for incomplete jobs.

## Performance & Scaling

- **Workers:** Start with 5–10 workers; increase up to 100+ on a high-spec PC.
- **Hardware tips:** See the “Custom PC Build” section for a 100+ worker setup.
- **Optimizations:**
  - Cached job list and downloads (TTL 30–60s)
  - Vectorized DataFrame building for fast Excel/CSV generation
  - SQLite WAL mode for concurrent access

## Project Structure

```
APEPDCL/
├── streamlit_app.py      # Main Streamlit frontend + job management
├── vsk2.py               # Core Selenium scraper (do not modify)
├── requirements.txt      # Python dependencies
├── .gitignore           # Git ignore file
├── run_data/            # SQLite DB + output files (auto-created)
└── Sample data/         # Example input/output files
```

## Configuration

- All runtime data is stored under `run_data/` (SQLite, Excel outputs, failed JSONs).
- The app uses `vsk2.py` for scraping; keep it unchanged.
- Chrome options are set inside `vsk2.py`; you can add flags like `--no-sandbox` if needed.

## Troubleshooting

- **Chrome crashes:** Reduce workers or ensure enough RAM/CPU.
- **Browser session lost:** The app auto-retries driver initialization.
- **Job status stuck:** Refresh the Scrap History tab or restart Streamlit.
- **Slow downloads:** Use CSV option for large files; Excel is slower for 50k+ rows.

## Custom PC Build for 100+ Workers

If you plan to run 100+ Selenium workers smoothly, consider this build:

- **CPU:** Intel Core i9-14900K (24 cores / 32 threads)
- **Motherboard:** MSI MAG Z790 TOMAHAWK WIFI (4× DDR5 slots, up to 192 GB)
- **RAM:** 64 GB DDR5-6000 (2×32 GB, upgradeable to 128 GB)
- **Primary SSD:** 2 TB Samsung 990 PRO (PCIe 4.0 NVMe)
- **Secondary SSD:** 4 TB Samsung 980 PRO (PCIe 4.0)
- **GPU:** AMD Radeon RX 7700 XT 12 GB (or NVIDIA RTX 4060 Ti)
- **CPU Cooler:** Thermalright Phantom Spirit 120 SE (air) or Arctic Liquid Freezer II 360 AIO
- **PSU:** Seasonic PRIME TX-1000 (1000W 80+ Gold)
- **Case:** DeepCool MATREXX 30 (budget-friendly, good airflow)
- **OS:** Windows 11 Pro or Ubuntu 22.04/24.04

This setup provides enough cores, RAM, and cooling to sustain 100+ Chrome instances without crashes.

## License

This project is provided as-is for educational and operational use. Respect the target website’s terms of service.

---

**Happy scraping!** 🚀
