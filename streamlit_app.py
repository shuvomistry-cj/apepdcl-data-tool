import io

import json

import os

import re

import sqlite3

import threading

import time

from dataclasses import dataclass

from typing import List, Optional



import pandas as pd

import streamlit as st





CID_COLUMN_CANDIDATES = [

    "cid",

    "service",

    "service_number",

    "service no",

    "service number",

    "scno",

    "consumer",

    "consumer number",

    "ltscno",

]





RAW_REQUIRED_COLUMNS = [

    "CIRCLE_NAME",

    "DIVISION_NAME",

    "SUBDIV_NAME",

    "ERO_NAME",

    "SECTION_NAME",

    "DISTNAME",

    "SCNO",

    "NAME",

    "MOBILE_NUM",

    "CATEGORY",

    "SC_STAT",

]





@dataclass

class DetectionResult:

    detected: bool

    reason: str

    cids: List[str]

    rows_total: int

    rows_non_empty: int





def _normalize_col(col: str) -> str:

    return re.sub(r"\s+", " ", str(col).strip().lower())





def _clean_cid(value) -> Optional[str]:

    if value is None:

        return None

    s = str(value).strip()

    if not s or s.lower() in {"nan", "none"}:

        return None

    return s





def parse_cids_from_text(text: str) -> List[str]:

    if not text:

        return []



    # Split on commas, newlines, or semicolons; also tolerate extra spaces.

    parts = re.split(r"[,;\n\r]+", text)

    cids = []

    for p in parts:

        cid = _clean_cid(p)

        if cid:

            cids.append(cid)

    return cids





def detect_cids_from_excel(file_bytes: bytes, filename: str) -> DetectionResult:

    try:

        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")

    except Exception as e:

        return DetectionResult(

            detected=False,

            reason=f"Could not read Excel file: {e}",

            cids=[],

            rows_total=0,

            rows_non_empty=0,

        )



    if df.empty:

        return DetectionResult(

            detected=False,

            reason="Excel file has no rows.",

            cids=[],

            rows_total=0,

            rows_non_empty=0,

        )



    # Try to find a column by name.

    normalized_cols = { _normalize_col(c): c for c in df.columns }

    matched_col = None

    for cand in CID_COLUMN_CANDIDATES:

        for ncol, orig in normalized_cols.items():

            if cand == ncol or cand in ncol:

                matched_col = orig

                break

        if matched_col:

            break



    # Fallback: if single column, assume it holds CIDs.

    if matched_col is None and df.shape[1] == 1:

        matched_col = df.columns[0]



    if matched_col is None:

        return DetectionResult(

            detected=False,

            reason=(

                "No CID/service-number column detected. "

                "Upload an Excel with a single column of service numbers OR a column named like 'CID'/'Service Number'."

            ),

            cids=[],

            rows_total=int(df.shape[0]),

            rows_non_empty=0,

        )



    series = df[matched_col]

    cleaned = [_clean_cid(v) for v in series.tolist()]

    cids = [c for c in cleaned if c is not None]



    return DetectionResult(

        detected=True,

        reason=f"Detected column: '{matched_col}'",

        cids=cids,

        rows_total=int(df.shape[0]),

        rows_non_empty=len(cids),

    )





def _normalize_space(s: str) -> str:

    s = re.sub(r"\s+", " ", s)

    return s.strip()





def _split_address_chunks(full: str) -> List[str]:

    parts = [p.strip() for p in full.split(",")]

    return [p for p in parts if p]





def clean_full_address(chunks: List[str]) -> str:

    cleaned_chunks: List[str] = []

    seen = set()



    for chunk in chunks:

        if chunk is None:

            continue

        s0 = str(chunk)

        s0 = s0.replace("\u00a0", " ")

        s0 = _normalize_space(s0)

        s0 = s0.strip(" ,")

        if not s0 or s0.lower() in {"nan", "none"}:

            continue



        # Some source columns already contain comma-separated sub-parts (e.g. "R.C.Palem,Kotauratla").

        # Split those so duplicates can be removed across the entire Full Address.

        subparts = _split_address_chunks(s0) if "," in s0 else [s0]

        for s in subparts:

            s = s.replace("\u00a0", " ")

            s = _normalize_space(s)

            s = s.strip(" ,")

            if not s or s.lower() in {"nan", "none"}:

                continue



            # Remove immediate repeated words inside a token (case-insensitive)

            words = [w for w in re.split(r"\s+", s) if w]

            dedup_words: List[str] = []

            prev = None

            for w in words:

                wl = w.lower()

                if prev == wl:

                    continue

                dedup_words.append(w)

                prev = wl

            s = " ".join(dedup_words)

            s = _normalize_space(s)



            key = s.lower()

            if key in seen:

                continue

            seen.add(key)

            cleaned_chunks.append(s)



    full = ",".join(cleaned_chunks)

    full = re.sub(r",{2,}", ",", full)

    full = full.strip(" ,")

    return full





def build_full_address_from_row(row: pd.Series, selected_columns: List[str]) -> str:

    chunks = []

    for col in selected_columns:

        if col not in row.index:

            continue

        chunks.append(row[col])

    return clean_full_address(chunks)





def build_input_excel_bytes(cids: List[str]) -> bytes:

    # vsk2.py reads with header=None and uses first column, so write as a single column without header.

    df = pd.DataFrame({0: cids})

    buf = io.BytesIO()

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:

        df.to_excel(writer, index=False, header=False)

    return buf.getvalue()





def ensure_parent_dir(path: str) -> None:

    parent = os.path.dirname(os.path.abspath(path))

    if parent:

        os.makedirs(parent, exist_ok=True)





def get_db_path(base_folder: str) -> str:

    base = os.path.abspath(base_folder)

    os.makedirs(base, exist_ok=True)

    return os.path.join(base, "preprocessed.db")





def init_db(db_path: str) -> None:

    ensure_parent_dir(db_path)

    with sqlite3.connect(db_path) as conn:

        conn.execute("PRAGMA journal_mode=WAL")

        conn.execute("PRAGMA synchronous=NORMAL")

        conn.execute(

            """

            CREATE TABLE IF NOT EXISTS preprocessed_files (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                original_filename TEXT,

                created_at_utc INTEGER,

                row_count INTEGER,

                address_columns_json TEXT

            )

            """

        )

        conn.execute(

            """

            CREATE TABLE IF NOT EXISTS preprocessed_rows (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                file_id INTEGER NOT NULL,

                row_index INTEGER NOT NULL,

                circle_name TEXT,

                division_name TEXT,

                subdiv_name TEXT,

                ero_name TEXT,

                section_name TEXT,

                distname TEXT,

                scno TEXT,

                name TEXT,

                full_address TEXT,

                mobile_num TEXT,

                category TEXT,

                sc_stat TEXT,

                FOREIGN KEY(file_id) REFERENCES preprocessed_files(id)

            )

            """

        )



        conn.execute(

            """

            CREATE TABLE IF NOT EXISTS scraped_jobs (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                file_id INTEGER NOT NULL,

                file_name TEXT,

                scno_column TEXT,

                created_at_utc INTEGER,

                updated_at_utc INTEGER,

                status TEXT,

                total_rows INTEGER,

                scraped_count INTEGER,

                failed_count INTEGER,

                last_error TEXT,

                workers INTEGER,

                FOREIGN KEY(file_id) REFERENCES preprocessed_files(id)

            )

            """

        )

        conn.execute(

            """

            CREATE TABLE IF NOT EXISTS scraped_results (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                job_id INTEGER NOT NULL,

                file_id INTEGER NOT NULL,

                row_index INTEGER NOT NULL,

                scno TEXT,

                scraped_json TEXT,

                status TEXT,

                error TEXT,

                updated_at_utc INTEGER,

                UNIQUE(job_id, row_index),

                FOREIGN KEY(job_id) REFERENCES scraped_jobs(id)

            )

            """

        )

        conn.execute(

            """

            CREATE TABLE IF NOT EXISTS scraped_failed (

                id INTEGER PRIMARY KEY AUTOINCREMENT,

                job_id INTEGER NOT NULL,

                file_id INTEGER NOT NULL,

                row_index INTEGER NOT NULL,

                scno TEXT,

                error TEXT,

                created_at_utc INTEGER,

                UNIQUE(job_id, row_index),

                FOREIGN KEY(job_id) REFERENCES scraped_jobs(id)

            )

            """

        )

        conn.commit()





def update_job_workers(db_path: str, job_id: int, workers: int) -> None:

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        conn.execute(

            "UPDATE scraped_jobs SET workers = ?, updated_at_utc = ? WHERE id = ?",

            (int(workers), now, int(job_id)),

        )

        conn.commit()





def delete_cleaned_file(db_path: str, file_id: int) -> None:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        conn.execute("PRAGMA foreign_keys=OFF")

        job_ids = [r[0] for r in conn.execute("SELECT id FROM scraped_jobs WHERE file_id = ?", (int(file_id),)).fetchall()]

        for job_id in job_ids:

            conn.execute("DELETE FROM scraped_results WHERE job_id = ?", (int(job_id),))

            conn.execute("DELETE FROM scraped_failed WHERE job_id = ?", (int(job_id),))

        conn.execute("DELETE FROM scraped_jobs WHERE file_id = ?", (int(file_id),))

        conn.execute("DELETE FROM preprocessed_rows WHERE file_id = ?", (int(file_id),))

        conn.execute("DELETE FROM preprocessed_files WHERE id = ?", (int(file_id),))

        conn.commit()





def delete_scrape_job(db_path: str, job_id: int) -> None:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        conn.execute("DELETE FROM scraped_results WHERE job_id = ?", (int(job_id),))

        conn.execute("DELETE FROM scraped_failed WHERE job_id = ?", (int(job_id),))

        conn.execute("DELETE FROM scraped_jobs WHERE id = ?", (int(job_id),))

        conn.commit()





def write_failed_to_folder(base_folder: str, job_id: int, row_index: int, scno: str, error: str) -> None:

    try:

        failed_dir = os.path.join(os.path.abspath(base_folder), "failed")

        os.makedirs(failed_dir, exist_ok=True)

        path = os.path.join(failed_dir, f"job_{int(job_id)}_failed.json")

        payload = {

            "job_id": int(job_id),

            "row_index": int(row_index),

            "scno": scno,

            "error": error,

            "ts": int(time.time()),

        }

        existing = []

        if os.path.exists(path) and os.path.getsize(path) > 0:

            try:

                with open(path, "r", encoding="utf-8") as f:

                    existing = json.load(f)

                if not isinstance(existing, list):

                    existing = []

            except Exception:

                existing = []

        existing.append(payload)

        with open(path, "w", encoding="utf-8") as f:

            json.dump(existing, f, indent=2)

    except Exception:

        pass





def save_preprocessed_to_db(

    db_path: str,

    original_filename: str,

    df_out: pd.DataFrame,

    address_columns: List[str],

) -> int:

    init_db(db_path)

    created_at = int(time.time())

    with sqlite3.connect(db_path) as conn:

        cur = conn.cursor()

        cur.execute(

            "INSERT INTO preprocessed_files (original_filename, created_at_utc, row_count, address_columns_json) VALUES (?,?,?,?)",

            (original_filename, created_at, int(df_out.shape[0]), json.dumps(address_columns)),

        )

        file_id = int(cur.lastrowid)



        rows = []

        for idx, r in df_out.iterrows():

            rows.append(

                (

                    file_id,

                    int(idx),

                    str(r.get("CIRCLE_NAME", "")),

                    str(r.get("DIVISION_NAME", "")),

                    str(r.get("SUBDIV_NAME", "")),

                    str(r.get("ERO_NAME", "")),

                    str(r.get("SECTION_NAME", "")),

                    str(r.get("DISTNAME", "")),

                    str(r.get("SCNO", "")),

                    str(r.get("NAME", "")),

                    str(r.get("Full Address", "")),

                    str(r.get("MOBILE_NUM", "")),

                    str(r.get("CATEGORY", "")),

                    str(r.get("SC_STAT", "")),

                )

            )



        conn.executemany(

            """

            INSERT INTO preprocessed_rows (

                file_id, row_index, circle_name, division_name, subdiv_name, ero_name, section_name, distname,

                scno, name, full_address, mobile_num, category, sc_stat

            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)

            """,

            rows,

        )

        conn.commit()

    return file_id





def list_preprocessed_files(db_path: str, limit: int = 20) -> pd.DataFrame:

    if not os.path.exists(db_path):

        return pd.DataFrame(columns=["id", "original_filename", "created_at_utc", "row_count", "address_columns_json"])

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        df = pd.read_sql_query(

            "SELECT id, original_filename, created_at_utc, row_count, address_columns_json FROM preprocessed_files ORDER BY id DESC LIMIT ?",

            conn,

            params=(limit,),

        )

    return df





def get_preprocessed_columns(db_path: str, file_id: int) -> List[str]:

    # Cleaned output schema

    return [

        "CIRCLE_NAME",

        "DIVISION_NAME",

        "SUBDIV_NAME",

        "ERO_NAME",

        "SECTION_NAME",

        "DISTNAME",

        "SCNO",

        "NAME",

        "Full Address",

        "MOBILE_NUM",

        "CATEGORY",

        "SC_STAT",

    ]





def get_preprocessed_scno_list(db_path: str, file_id: int, scno_column: str) -> List[tuple]:

    # scno_column is one of the cleaned columns; internally we can only read from preprocessed_rows fields.

    # Map cleaned columns back to DB fields.

    col_map = {

        "SCNO": "scno",

        "NAME": "name",

        "Full Address": "full_address",

        "MOBILE_NUM": "mobile_num",

        "CATEGORY": "category",

        "SC_STAT": "sc_stat",

        "CIRCLE_NAME": "circle_name",

        "DIVISION_NAME": "division_name",

        "SUBDIV_NAME": "subdiv_name",

        "ERO_NAME": "ero_name",

        "SECTION_NAME": "section_name",

        "DISTNAME": "distname",

    }

    db_col = col_map.get(scno_column)

    if not db_col:

        raise ValueError(f"Unsupported SCNO column: {scno_column}")



    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        rows = conn.execute(

            f"SELECT row_index, {db_col} FROM preprocessed_rows WHERE file_id = ? ORDER BY row_index ASC",

            (file_id,),

        ).fetchall()

    return [(int(r[0]), str(r[1]) if r[1] is not None else "") for r in rows]





def create_scrape_job(

    db_path: str,

    file_id: int,

    file_name: str,

    scno_column: str,

    total_rows: int,

    workers: int,

) -> int:

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        cur = conn.cursor()

        cur.execute(

            """

            INSERT INTO scraped_jobs (

                file_id, file_name, scno_column, created_at_utc, updated_at_utc, status,

                total_rows, scraped_count, failed_count, last_error, workers

            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)

            """,

            (file_id, file_name, scno_column, now, now, "Scraping", total_rows, 0, 0, None, int(workers)),

        )

        job_id = int(cur.lastrowid)

        conn.commit()

    return job_id





def get_active_job_for_file(db_path: str, file_id: int) -> Optional[dict]:

    if not os.path.exists(db_path):

        return None

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        row = conn.execute(

            """

            SELECT id, status FROM scraped_jobs

            WHERE file_id = ? AND status IN ('Scraping','Paused')

            ORDER BY id DESC LIMIT 1

            """,

            (file_id,),

        ).fetchone()

    if not row:

        return None

    return {"id": int(row[0]), "status": str(row[1])}





def list_scrape_jobs(db_path: str, limit: int = 50) -> pd.DataFrame:

    if not os.path.exists(db_path):

        return pd.DataFrame(

            columns=[

                "id",

                "file_id",

                "file_name",

                "scno_column",

                "status",

                "total_rows",

                "scraped_count",

                "failed_count",

                "workers",

                "updated_at_utc",

            ]

        )

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        df = pd.read_sql_query(

            """

            SELECT id, file_id, file_name, scno_column, status, total_rows, scraped_count, failed_count, workers, updated_at_utc

            FROM scraped_jobs

            ORDER BY id DESC

            LIMIT ?

            """,

            conn,

            params=(limit,),

        )

    return df





@st.cache_data(show_spinner=True, ttl=60)

def _build_job_output_bytes_cached(db_path: str, job_id: int) -> bytes:

    df_out = build_scraped_output_dataframe(db_path, int(job_id))

    buf = io.BytesIO()

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:

        df_out.to_excel(writer, index=False)

    return buf.getvalue()





@st.cache_data(show_spinner=True, ttl=60)

def _build_job_unscraped_bytes_cached(db_path: str, job_id: int) -> bytes:

    df_uns = get_unscraped_cleaned_dataframe(db_path, int(job_id))

    buf = io.BytesIO()

    with pd.ExcelWriter(buf, engine="openpyxl") as writer:

        df_uns.to_excel(writer, index=False)

    return buf.getvalue()





def update_job_status(db_path: str, job_id: int, status: str, last_error: Optional[str] = None) -> None:

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        conn.execute(

            "UPDATE scraped_jobs SET status = ?, updated_at_utc = ?, last_error = ? WHERE id = ?",

            (status, now, last_error, int(job_id)),

        )

        conn.commit()





def increment_job_counters(db_path: str, job_id: int, scraped_delta: int = 0, failed_delta: int = 0) -> None:

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        conn.execute(

            """

            UPDATE scraped_jobs

            SET scraped_count = scraped_count + ?, failed_count = failed_count + ?, updated_at_utc = ?

            WHERE id = ?

            """,

            (int(scraped_delta), int(failed_delta), now, int(job_id)),

        )

        conn.commit()





def upsert_scraped_result(

    db_path: str,

    job_id: int,

    file_id: int,

    row_index: int,

    scno: str,

    scraped: Optional[dict],

    status: str,

    error: Optional[str],

) -> None:

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        conn.execute(

            """

            INSERT INTO scraped_results (job_id, file_id, row_index, scno, scraped_json, status, error, updated_at_utc)

            VALUES (?,?,?,?,?,?,?,?)

            ON CONFLICT(job_id, row_index) DO UPDATE SET

                scno=excluded.scno,

                scraped_json=excluded.scraped_json,

                status=excluded.status,

                error=excluded.error,

                updated_at_utc=excluded.updated_at_utc

            """,

            (

                int(job_id),

                int(file_id),

                int(row_index),

                scno,

                json.dumps(scraped or {}),

                status,

                error,

                now,

            ),

        )

        conn.commit()





def upsert_failed(db_path: str, job_id: int, file_id: int, row_index: int, scno: str, error: str) -> None:

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        conn.execute(

            """

            INSERT INTO scraped_failed (job_id, file_id, row_index, scno, error, created_at_utc)

            VALUES (?,?,?,?,?,?)

            ON CONFLICT(job_id, row_index) DO UPDATE SET

                scno=excluded.scno,

                error=excluded.error

            """,

            (int(job_id), int(file_id), int(row_index), scno, error, now),

        )

        conn.commit()





def get_unscraped_rows(db_path: str, job_id: int, file_id: int, scno_column: str) -> List[tuple]:

    all_rows = get_preprocessed_scno_list(db_path, file_id, scno_column)

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        done = conn.execute(

            "SELECT row_index FROM scraped_results WHERE job_id = ? AND status = 'success'",

            (int(job_id),),

        ).fetchall()

    done_set = {int(r[0]) for r in done}

    return [(idx, scno) for (idx, scno) in all_rows if idx not in done_set and scno and scno.lower() != 'nan']





def build_scraped_output_dataframe(db_path: str, job_id: int) -> pd.DataFrame:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        job = conn.execute(

            "SELECT file_id FROM scraped_jobs WHERE id = ?",

            (int(job_id),),

        ).fetchone()

        if not job:

            return pd.DataFrame()

        file_id = int(job[0])



        base_df = pd.read_sql_query(

            """

            SELECT

                circle_name AS CIRCLE_NAME,

                division_name AS DIVISION_NAME,

                subdiv_name AS SUBDIV_NAME,

                ero_name AS ERO_NAME,

                section_name AS SECTION_NAME,

                distname AS DISTNAME,

                scno AS SCNO,

                name AS NAME,

                full_address AS [Full Address],

                mobile_num AS MOBILE_NUM,

                category AS CATEGORY,

                sc_stat AS SC_STAT,

                row_index AS __row_index

            FROM preprocessed_rows

            WHERE file_id = ?

            ORDER BY row_index ASC

            """,

            conn,

            params=(file_id,),

        )

        res = conn.execute(

            "SELECT row_index, scraped_json, status FROM scraped_results WHERE job_id = ?",

            (int(job_id),),

        ).fetchall()



    months = set()

    parsed = {}

    for row_index, scraped_json, status in res:

        if status != "success":

            continue

        try:

            data = json.loads(scraped_json) if scraped_json else {}

        except Exception:

            data = {}

        parsed[int(row_index)] = data

        for k in data.keys():

            months.add(k)



    # To match Sample Scraped Output ordering (reverse-sorted seems used in vsk2 pivot), but keep stable.

    month_cols = sorted(list(months), reverse=True)

    for m in month_cols:

        base_df[m] = None



    for idx, data in parsed.items():

        for m, val in data.items():

            try:

                base_df.loc[base_df["__row_index"] == idx, m] = val

            except Exception:

                pass



    base_df = base_df.drop(columns=["__row_index"])

    return base_df





class JobRunner:

    def __init__(self, db_path: str, job_id: int, file_id: int, scno_column: str, workers: int, vsk2_module, run_folder: str):

        self.db_path = db_path

        self.job_id = int(job_id)

        self.file_id = int(file_id)

        self.scno_column = scno_column

        # Safety clamp: running too many Selenium browsers on a single machine is typically not practical.

        self.workers = int(min(100, max(1, workers)))

        self.vsk2 = vsk2_module

        self.run_folder = run_folder



        self._pause_event = threading.Event()

        self._stop_event = threading.Event()

        self._threads: List[threading.Thread] = []

        self._queue: List[tuple] = []

        self._queue_lock = threading.Lock()

        self._min_row_index: Optional[int] = None



    def _pop_task(self) -> Optional[tuple]:

        with self._queue_lock:

            if not self._queue:

                return None

            return self._queue.pop(0)



    def _load_queue(self):

        q = get_unscraped_rows(self.db_path, self.job_id, self.file_id, self.scno_column)

        if self._min_row_index is not None:

            q = [t for t in q if int(t[0]) >= int(self._min_row_index)]

        self._queue = q



    def pause(self):

        self._pause_event.set()

        update_job_status(self.db_path, self.job_id, "Paused")



    def resume(self):

        self._pause_event.clear()

        update_job_status(self.db_path, self.job_id, "Scraping")



    def stop(self):

        self._stop_event.set()

        update_job_status(self.db_path, self.job_id, "Stopped")



        def joiner():

            for t in list(self._threads):

                try:

                    t.join(timeout=5)

                except Exception:

                    pass



        threading.Thread(target=joiner, daemon=True).start()



    def set_workers(self, workers: int):

        self.workers = int(min(100, max(1, workers)))



    def set_min_row_index(self, min_row_index: Optional[int]):

        self._min_row_index = None if min_row_index is None else int(min_row_index)



    def is_running(self) -> bool:

        return any(t.is_alive() for t in self._threads)



    def start(self):

        if self.is_running():

            return



        self._stop_event.clear()

        self._pause_event.clear()

        self._load_queue()

        if not self._queue:

            update_job_status(self.db_path, self.job_id, "Done")

            return



        def worker_loop(worker_idx: int):

            driver = None

            def new_driver_with_retries() -> Optional[object]:

                last_exc = None

                for attempt in range(1, 6):

                    try:

                        return self.vsk2.get_new_driver()

                    except Exception as e:

                        last_exc = e

                        try:

                            time.sleep(min(8, attempt * 1.5))

                        except Exception:

                            pass

                try:

                    update_job_status(self.db_path, self.job_id, "Stopped", last_error=str(last_exc))

                except Exception:

                    pass

                return None

            driver = new_driver_with_retries()

            if driver is None:

                return

            consecutive_driver_failures = 0



            while not self._stop_event.is_set():

                if self._pause_event.is_set():

                    time.sleep(0.5)

                    continue



                task = self._pop_task()

                if task is None:

                    break



                row_index, scno = task

                scno = str(scno).strip()

                if not scno or scno.lower() == "nan":

                    continue



                # Auto-recover on internet issues (wait until connectivity returns)

                try:

                    if hasattr(self.vsk2, "check_internet_connection") and hasattr(self.vsk2, "wait_for_internet"):

                        if not self.vsk2.check_internet_connection():

                            self.vsk2.wait_for_internet()

                except Exception:

                    pass



                # If driver is dead, recreate it

                try:

                    _ = driver.title

                except Exception:

                    consecutive_driver_failures += 1

                    try:

                        driver.quit()

                    except Exception:

                        pass

                    driver = new_driver_with_retries()

                    if driver is None:

                        return

                    if consecutive_driver_failures >= 10:

                        update_job_status(self.db_path, self.job_id, "Stopped", last_error="Repeated browser session loss")

                        return

                    continue



                try:

                    scraped = self.vsk2.process_cid(driver, scno)

                    upsert_scraped_result(self.db_path, self.job_id, self.file_id, row_index, scno, scraped, "success", None)

                    increment_job_counters(self.db_path, self.job_id, scraped_delta=1, failed_delta=0)

                    consecutive_driver_failures = 0

                except Exception as e:

                    err = str(e)

                    upsert_scraped_result(self.db_path, self.job_id, self.file_id, row_index, scno, None, "failed", err)

                    upsert_failed(self.db_path, self.job_id, self.file_id, row_index, scno, err)

                    write_failed_to_folder(self.run_folder, self.job_id, row_index, scno, err)

                    increment_job_counters(self.db_path, self.job_id, scraped_delta=0, failed_delta=1)



            try:

                if driver:

                    driver.quit()

            except Exception:

                pass



        update_job_status(self.db_path, self.job_id, "Scraping")

        self._threads = []

        for i in range(self.workers):

            t = threading.Thread(target=worker_loop, args=(i,), daemon=True)

            t.start()

            self._threads.append(t)



        def finalize_watcher():

            for t in self._threads:

                t.join()

            if self._stop_event.is_set():

                return

            # If paused, keep status

            job = get_active_job_for_file(self.db_path, self.file_id)

            if job and job.get("status") == "Paused":

                return

            # Check if any tasks left

            remaining = get_unscraped_rows(self.db_path, self.job_id, self.file_id, self.scno_column)

            if not remaining:

                update_job_status(self.db_path, self.job_id, "Done")



        threading.Thread(target=finalize_watcher, daemon=True).start()





_JOB_RUNNERS: dict = {}





def get_or_create_runner(db_path: str, job_id: int, file_id: int, scno_column: str, workers: int, vsk2_module) -> JobRunner:

    key = int(job_id)

    runner = _JOB_RUNNERS.get(key)

    if runner is None:

        runner = JobRunner(db_path, job_id, file_id, scno_column, workers, vsk2_module, run_folder=st.session_state.get("run_folder", ""))

        _JOB_RUNNERS[key] = runner

    return runner





def get_job_details(db_path: str, job_id: int) -> Optional[dict]:

    if not os.path.exists(db_path):

        return None

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        row = conn.execute(

            """

            SELECT id, file_id, file_name, scno_column, status, total_rows, scraped_count, failed_count, workers

            FROM scraped_jobs WHERE id = ?

            """,

            (int(job_id),),

        ).fetchone()

    if not row:

        return None

    return {

        "id": int(row[0]),

        "file_id": int(row[1]),

        "file_name": str(row[2]),

        "scno_column": str(row[3]),

        "status": str(row[4]),

        "total_rows": int(row[5] or 0),

        "scraped_count": int(row[6] or 0),

        "failed_count": int(row[7] or 0),

        "workers": int(row[8] or 1),

    }





def get_success_row_index_set(db_path: str, job_id: int) -> set:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        rows = conn.execute(

            "SELECT row_index FROM scraped_results WHERE job_id = ? AND status = 'success'",

            (int(job_id),),

        ).fetchall()

    return {int(r[0]) for r in rows}





def get_next_unscraped_row_index(db_path: str, job_id: int, total_rows: int) -> Optional[int]:

    done = get_success_row_index_set(db_path, job_id)

    for i in range(int(total_rows)):

        if i not in done:

            return i

    return None





def get_last_attempted_row_index(db_path: str, job_id: int) -> Optional[int]:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        row = conn.execute(

            "SELECT MAX(row_index) FROM scraped_results WHERE job_id = ?",

            (int(job_id),),

        ).fetchone()

    if not row:

        return None

    if row[0] is None:

        return None

    return int(row[0])





def recover_orphaned_jobs(db_path: str) -> None:

    # If Streamlit/server crashes while a job is marked Scraping, threads are gone.

    # Mark it as Stopped so user can safely restart.

    init_db(db_path)

    now = int(time.time())

    with sqlite3.connect(db_path) as conn:

        conn.execute(

            """

            UPDATE scraped_jobs

            SET status = 'Stopped', updated_at_utc = ?, last_error = COALESCE(last_error, 'Recovered after restart')

            WHERE status = 'Scraping'

            """,

            (now,),

        )

        conn.commit()





def get_unscraped_cleaned_dataframe(db_path: str, job_id: int) -> pd.DataFrame:

    details = get_job_details(db_path, job_id)

    if not details:

        return pd.DataFrame()

    file_id = int(details["file_id"])

    base_df = get_preprocessed_dataframe(db_path, file_id)

    done = get_success_row_index_set(db_path, job_id)

    # row_index in DB is preserved ordering; since base_df is ordered by row_index, filter by positional index

    # We stored row_index as the DataFrame index when saving; here base_df has default RangeIndex aligned to row_index.

    mask = [i not in done for i in range(len(base_df))]

    return base_df.loc[mask].reset_index(drop=True)





def get_preprocessed_dataframe(db_path: str, file_id: int) -> pd.DataFrame:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        df = pd.read_sql_query(

            """

            SELECT

                circle_name AS CIRCLE_NAME,

                division_name AS DIVISION_NAME,

                subdiv_name AS SUBDIV_NAME,

                ero_name AS ERO_NAME,

                section_name AS SECTION_NAME,

                distname AS DISTNAME,

                scno AS SCNO,

                name AS NAME,

                full_address AS [Full Address],

                mobile_num AS MOBILE_NUM,

                category AS CATEGORY,

                sc_stat AS SC_STAT

            FROM preprocessed_rows

            WHERE file_id = ?

            ORDER BY row_index ASC

            """,

            conn,

            params=(file_id,),

        )

    return df





def get_preprocessed_dataframe_limit(db_path: str, file_id: int, limit: int = 30) -> pd.DataFrame:

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:

        df = pd.read_sql_query(

            """

            SELECT

                circle_name AS CIRCLE_NAME,

                division_name AS DIVISION_NAME,

                subdiv_name AS SUBDIV_NAME,

                ero_name AS ERO_NAME,

                section_name AS SECTION_NAME,

                distname AS DISTNAME,

                scno AS SCNO,

                name AS NAME,

                full_address AS [Full Address],

                mobile_num AS MOBILE_NUM,

                category AS CATEGORY,

                sc_stat AS SC_STAT

            FROM preprocessed_rows

            WHERE file_id = ?

            ORDER BY row_index ASC

            LIMIT ?

            """,

            conn,

            params=(file_id, int(limit)),

        )

    return df





@st.cache_resource

def get_vsk2_module():

    import signal



    original_signal = signal.signal



    def safe_signal(signalnum, handler):

        try:

            return original_signal(signalnum, handler)

        except ValueError:

            return None



    try:

        signal.signal = safe_signal

        import vsk2

        return vsk2

    finally:

        signal.signal = original_signal





def configure_vsk2_paths(vsk2, base_folder: str) -> dict:

    base = os.path.abspath(base_folder)

    paths = {

        "INPUT_FILE": os.path.join(base, "INPUT", "input.xlsx"),

        "OUTPUT_FILE": os.path.join(base, "OUTPUT", "output.xlsx"),

        "FAILED_FILE": os.path.join(base, "FAILED", "failed.json"),

        "STATUS_FILE": os.path.join(base, "STATUS", "status.json"),

    }



    for p in paths.values():

        ensure_parent_dir(p)



    vsk2.INPUT_FILE = paths["INPUT_FILE"]

    vsk2.OUTPUT_FILE = paths["OUTPUT_FILE"]

    vsk2.FAILED_FILE = paths["FAILED_FILE"]

    vsk2.STATUS_FILE = paths["STATUS_FILE"]

    return paths





def write_input_excel(path: str, cids: List[str]) -> None:

    ensure_parent_dir(path)

    df = pd.DataFrame({0: cids})

    df.to_excel(path, index=False, header=False, engine="openpyxl")





def read_json_file(path: str):

    try:

        if not os.path.exists(path) or os.path.getsize(path) == 0:

            return None

        with open(path, "r", encoding="utf-8") as f:

            return json.load(f)

    except Exception:

        return None





def is_scraping_running(vsk2) -> bool:

    t = getattr(vsk2, "scraper_thread", None)

    return bool(t and t.is_alive())





st.set_page_config(page_title="VSK CID Uploader", layout="wide")



st.markdown(

    """

    <style>

    .stButton>button, .stDownloadButton>button {

        padding: 0.25rem 0.65rem;

        border-radius: 10px;

        min-height: 2.2rem;

        white-space: nowrap;

    }

    </style>

    """,

    unsafe_allow_html=True,

)



st.title("APEPDCL - DATA TOOL")

st.caption("Clean raw data and scrape bill details.")



vsk2 = get_vsk2_module()



run_folder = os.path.join(os.path.dirname(__file__), "run_data")

st.session_state["run_folder"] = run_folder

paths = configure_vsk2_paths(vsk2, run_folder)

db_path = get_db_path(run_folder)

init_db(db_path)



# Crash recovery: if the app restarts while jobs were marked Scraping, mark them recoverable.

# IMPORTANT: Streamlit re-runs the script frequently; only do this once per app session.

if "_orphan_recovery_done" not in st.session_state:

    recover_orphaned_jobs(db_path)

    st.session_state["_orphan_recovery_done"] = True





tab_clean, tab_scrape, tab_history = st.tabs(["Clean Raw Data", "Scrap Data", "Scrap History"])





with tab_clean:

    st.subheader("Clean Raw Data")

    st.write("Upload the raw Excel, select columns to merge as Full Address, clean, and store into SQLite.")



    raw_upload = st.file_uploader("Upload raw Excel (.xlsx)", type=["xlsx"], key="raw_upload")



    raw_df = None

    original_filename = None

    if raw_upload is not None:

        original_filename = raw_upload.name

        try:

            raw_df = pd.read_excel(io.BytesIO(raw_upload.getvalue()), engine="openpyxl")

        except Exception as e:

            st.error(f"Could not read Excel: {e}")



    if raw_df is not None:

        st.write({"rows": int(raw_df.shape[0]), "columns": int(raw_df.shape[1])})

        st.dataframe(raw_df.head(25), use_container_width=True)



        missing_required = [c for c in RAW_REQUIRED_COLUMNS if c not in raw_df.columns]

        if missing_required:

            st.warning(f"Missing expected columns: {', '.join(missing_required)}")



        address_columns = [c for c in raw_df.columns if c.upper().startswith("ADDRESS")]

        default_address_cols = [c for c in ["ADDRESS_1", "ADDRESS_2", "ADDRESS_3", "ADDRESS_4"] if c in raw_df.columns]



        selected_cols = st.multiselect(

            "Select columns to merge into Full Address",

            options=list(raw_df.columns),

            default=default_address_cols,

        )



        st.caption("Tip: include other columns like DISTNAME or CIRCLE_NAME if you want them in Full Address.")

        include_dist = st.checkbox("Auto-append DISTNAME to Full Address", value=True)

        include_circle = st.checkbox("Auto-append CIRCLE_NAME to Full Address", value=True)



        cols_for_full = list(selected_cols)

        if include_dist and "DISTNAME" in raw_df.columns and "DISTNAME" not in cols_for_full:

            cols_for_full.append("DISTNAME")

        if include_circle and "CIRCLE_NAME" in raw_df.columns and "CIRCLE_NAME" not in cols_for_full:

            cols_for_full.append("CIRCLE_NAME")



        start_clean = st.button("Start Clean", type="primary", disabled=(len(cols_for_full) == 0))

        if start_clean:

            total = int(raw_df.shape[0])

            prog = st.progress(0)

            stats = st.empty()

            out_rows = []



            for i in range(total):

                row = raw_df.iloc[i]

                full_addr = build_full_address_from_row(row, cols_for_full)



                out_rows.append(

                    {

                        "CIRCLE_NAME": row.get("CIRCLE_NAME"),

                        "DIVISION_NAME": row.get("DIVISION_NAME"),

                        "SUBDIV_NAME": row.get("SUBDIV_NAME"),

                        "ERO_NAME": row.get("ERO_NAME"),

                        "SECTION_NAME": row.get("SECTION_NAME"),

                        "DISTNAME": row.get("DISTNAME"),

                        "SCNO": row.get("SCNO"),

                        "NAME": row.get("NAME"),

                        "Full Address": full_addr,

                        "MOBILE_NUM": row.get("MOBILE_NUM"),

                        "CATEGORY": row.get("CATEGORY"),

                        "SC_STAT": row.get("SC_STAT"),

                    }

                )



                if (i + 1) % 10 == 0 or (i + 1) == total:

                    prog.progress(int((i + 1) * 100 / max(total, 1)))

                    stats.write({"processed": i + 1, "total": total})



            df_out = pd.DataFrame(out_rows)

            file_id = save_preprocessed_to_db(

                db_path=db_path,

                original_filename=original_filename or "raw.xlsx",

                df_out=df_out,

                address_columns=cols_for_full,

            )



            st.success(f"Cleaning completed and saved to DB (file_id={file_id})")



            buf = io.BytesIO()

            with pd.ExcelWriter(buf, engine="openpyxl") as writer:

                df_out.to_excel(writer, index=False)

            st.download_button(

                "Download cleaned Excel",

                data=buf.getvalue(),

                file_name=f"cleaned_{file_id}.xlsx",

                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

            )



    st.divider()

    st.subheader("History")

    files_df = list_preprocessed_files(db_path, limit=25)

    if files_df.empty:

        st.info("No cleaned files yet")

    else:

        if "history_preview_id" not in st.session_state:

            st.session_state["history_preview_id"] = None

        if "history_scrap_id" not in st.session_state:

            st.session_state["history_scrap_id"] = None



        if "confirm_delete_file_id" not in st.session_state:

            st.session_state["confirm_delete_file_id"] = None



        header = st.columns([6, 1, 1, 1.2, 1.2, 1.2, 1.2])

        header[0].write("File name")

        header[1].write("Rows")

        header[2].write("Cols")

        header[3].write("Preview")

        header[4].write("Download")

        header[5].write("Scrap")

        header[6].write("Delete")



        for _, r in files_df.iterrows():

            file_id = int(r["id"])

            filename = str(r.get("original_filename", ""))

            row_count = int(r.get("row_count", 0) or 0)

            col_count = 12



            cols = st.columns([6, 1, 1, 1.2, 1.2, 1.2, 1.2])

            cols[0].write(filename)

            cols[1].write(row_count)

            cols[2].write(col_count)



            if cols[3].button("👁️", key=f"hist_preview_{file_id}", help="Preview first 30 rows"):

                st.session_state["history_preview_id"] = file_id



            df_all_bytes = None

            try:

                df_all = get_preprocessed_dataframe(db_path, file_id)

                buf_dl = io.BytesIO()

                with pd.ExcelWriter(buf_dl, engine="openpyxl") as writer:

                    df_all.to_excel(writer, index=False)

                df_all_bytes = buf_dl.getvalue()

            except Exception:

                df_all_bytes = None



            cols[4].download_button(

                "⬇️",

                data=df_all_bytes or b"",

                file_name=f"cleaned_{file_id}.xlsx",

                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

                disabled=df_all_bytes is None,

                help="Download cleaned Excel",

                key=f"hist_download_{file_id}",

            )



            if cols[5].button("🕷️", key=f"hist_scrap_{file_id}", help="Scrap using this cleaned file"):

                st.session_state["history_scrap_id"] = file_id



            if cols[6].button("🗑️", key=f"hist_delete_{file_id}", help="Delete from DB permanently"):

                st.session_state["confirm_delete_file_id"] = file_id



        if st.session_state.get("history_preview_id"):

            st.divider()

            st.subheader(f"Preview: file_id={int(st.session_state['history_preview_id'])}")

            df_prev = get_preprocessed_dataframe_limit(db_path, int(st.session_state["history_preview_id"]), limit=30)

            st.dataframe(df_prev, use_container_width=True)



        if st.session_state.get("confirm_delete_job_id"):

            st.divider()

            jid = int(st.session_state["confirm_delete_job_id"])

            st.warning(f"This will permanently delete job_id={jid} and its results.")

            c1, c2 = st.columns(2)

            if c1.button("Confirm Delete Job", type="primary", key=f"confirm_del_job_{jid}"):

                runner = _JOB_RUNNERS.get(jid)

                if runner:

                    runner.stop()

                delete_scrape_job(db_path, jid)

                st.session_state["confirm_delete_job_id"] = None

                st.success("Deleted")

                st.rerun()

            if c2.button("Cancel", key=f"cancel_del_job_{jid}"):

                st.session_state["confirm_delete_job_id"] = None



        if st.session_state.get("history_scrap_id"):

            st.divider()

            target_file_id = int(st.session_state["history_scrap_id"])

            row = files_df.loc[files_df["id"] == target_file_id]

            target_name = str(row.iloc[0]["original_filename"]) if not row.empty else f"file_id={target_file_id}"

            target_rows = int(row.iloc[0]["row_count"]) if not row.empty else 0



            with st.expander(f"Scrap settings: {target_name}", expanded=True):

                active = get_active_job_for_file(db_path, target_file_id)

                if active:

                    st.warning("Scraping job is already running. Please check the status in the Scraped History tab.")

                else:

                    columns = get_preprocessed_columns(db_path, target_file_id)

                    default_col = "SCNO" if "SCNO" in columns else columns[0]

                    scno_col = st.selectbox(

                        "Select the SCNO / Service Number column",

                        options=columns,

                        index=columns.index(default_col),

                        key=f"scno_col_{target_file_id}",

                    )

                    workers = st.slider(

                        "Workers (parallel browsers)",

                        min_value=1,

                        max_value=100,

                        value=10,

                        step=1,

                        key=f"workers_{target_file_id}",

                    )



                    if workers >= 25:

                        st.warning(

                            "High worker count will open many Chrome instances. "

                            "This can crash the machine or trigger website blocking. "

                            "For lakhs of rows, consider running distributed workers on multiple machines."

                        )



                    if st.button("Start Scraping", type="primary", key=f"start_scrape_job_{target_file_id}"):

                        job_id = create_scrape_job(

                            db_path=db_path,

                            file_id=target_file_id,

                            file_name=target_name,

                            scno_column=scno_col,

                            total_rows=target_rows,

                            workers=workers,

                        )

                        runner = get_or_create_runner(db_path, job_id, target_file_id, scno_col, workers, vsk2)

                        runner.start()

                        st.success(f"Scraping started (job_id={job_id}). Open Tab 3 to monitor.")



        if st.session_state.get("confirm_delete_file_id"):

            st.divider()

            fid = int(st.session_state["confirm_delete_file_id"])

            st.warning(f"This will permanently delete cleaned file_id={fid} and all related scrape jobs/results.")

            c1, c2 = st.columns(2)

            if c1.button("Confirm Delete", type="primary", key=f"confirm_del_file_{fid}"):

                delete_cleaned_file(db_path, fid)

                st.session_state["confirm_delete_file_id"] = None

                st.success("Deleted")

                st.rerun()

            if c2.button("Cancel", key=f"cancel_del_file_{fid}"):

                st.session_state["confirm_delete_file_id"] = None





def render_scrape_tab():

    st.subheader("Scrap Data")

    st.write("Provide service numbers (CIDs) and run the scraper.")



    method = st.radio("Input method", ["Comma-separated", "Excel upload"], horizontal=True, key="scrape_method")



    cids: List[str] = []

    if method == "Comma-separated":

        text = st.text_area(

            "Paste service numbers",

            placeholder="Example: 1234567890, 2345678901, 3456789012",

            height=140,

            key="scrape_text",

        )

        cids = parse_cids_from_text(text)

        st.write({"count": len(cids), "unique_count": len(list(dict.fromkeys(cids)))})

        if cids:

            st.dataframe(pd.DataFrame({"CID": cids}).head(50), use_container_width=True)

    else:

        uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], key="scrape_upload")

        if uploaded is not None:

            data = uploaded.getvalue()

            result = detect_cids_from_excel(data, uploaded.name)

            st.write(

                {

                    "detected": result.detected,

                    "reason": result.reason,

                    "rows_total": result.rows_total,

                    "rows_with_service_number": result.rows_non_empty,

                }

            )

            if result.cids:

                st.dataframe(pd.DataFrame({"CID": result.cids}).head(50), use_container_width=True)

            cids = result.cids



    st.divider()

    remove_duplicates = st.checkbox("Remove duplicates", value=True, key="scrape_dedup")

    final_cids = list(dict.fromkeys(cids)) if remove_duplicates else cids



    st.write({"final_count": len(final_cids)})

    if not final_cids:

        st.warning("No service numbers detected yet.")

        return



    st.subheader("Scraper controls")

    col_a, col_b, col_c, col_d = st.columns(4)

    with col_a:

        start_clicked = st.button("Start", type="primary", disabled=is_scraping_running(vsk2), key="scrape_start")

    with col_b:

        pause_clicked = st.button("Pause", disabled=not is_scraping_running(vsk2), key="scrape_pause")

    with col_c:

        resume_clicked = st.button("Resume", disabled=not is_scraping_running(vsk2), key="scrape_resume")

    with col_d:

        stop_clicked = st.button("Stop", disabled=not is_scraping_running(vsk2), key="scrape_stop")



    if start_clicked:

        write_input_excel(vsk2.INPUT_FILE, final_cids)

        vsk2.start_scraping()

        st.success("Scraping started")



    if pause_clicked:

        vsk2.pause_scraping()



    if resume_clicked:

        vsk2.resume_scraping()



    if stop_clicked:

        vsk2.stop_scraping()



    st.divider()

    st.subheader("Progress")

    status = read_json_file(vsk2.STATUS_FILE) or {}

    failed = read_json_file(vsk2.FAILED_FILE) or []

    st.write(

        {

            "running": is_scraping_running(vsk2),

            "paused": bool(getattr(vsk2, "should_pause", False)),

            "stopping": bool(getattr(vsk2, "should_stop", False)),

            "last_processed_index": status.get("last_processed"),

            "total_processed": status.get("total_processed"),

            "failed_count": len(failed) if isinstance(failed, list) else None,

        }

    )

    if st.button("Refresh status", key="scrape_refresh"):

        st.rerun()



    if os.path.exists(vsk2.OUTPUT_FILE):

        st.download_button(

            "Download output Excel",

            data=open(vsk2.OUTPUT_FILE, "rb").read(),

            file_name=os.path.basename(vsk2.OUTPUT_FILE),

            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

        )

    if os.path.exists(vsk2.FAILED_FILE):

        st.download_button(

            "Download failed JSON",

            data=open(vsk2.FAILED_FILE, "rb").read(),

            file_name=os.path.basename(vsk2.FAILED_FILE),

            mime="application/json",

        )





with tab_scrape:

    render_scrape_tab()





with tab_history:

    st.subheader("Scrap History")

    st.write("Monitor and manage scraping jobs.")



    if "confirm_delete_job_id" not in st.session_state:

        st.session_state["confirm_delete_job_id"] = None



    if "restart_job_id" not in st.session_state:

        st.session_state["restart_job_id"] = None



    if "job_download_prepare" not in st.session_state:

        st.session_state["job_download_prepare"] = None



    jobs_df = list_scrape_jobs(db_path, limit=50)

    if jobs_df.empty:

        st.info("No scraping jobs yet. Start from Clean Raw Data -> History -> Scrap.")

    else:

        header = st.columns([4, 1, 1, 1.2, 1.2, 1.2, 2.2, 1.4, 1.2])

        header[0].write("File name")

        header[1].write("Rows")

        header[2].write("Cols")

        header[3].write("Scraped")

        header[4].write("Unscraped")

        header[5].write("Status")

        header[6].write("Progress")

        header[7].write("Actions")

        header[8].write("Download")



        for _, r in jobs_df.iterrows():

            job_id = int(r["id"])

            file_id = int(r["file_id"])

            file_name = str(r.get("file_name", ""))

            scno_col = str(r.get("scno_column", "SCNO"))

            status = str(r.get("status", ""))

            total_rows = int(r.get("total_rows", 0) or 0)

            scraped_count = int(r.get("scraped_count", 0) or 0)

            failed_count = int(r.get("failed_count", 0) or 0)

            workers = int(r.get("workers", 1) or 1)

            col_count = 22

            unscraped_count = max(total_rows - scraped_count, 0)



            cols = st.columns([4, 1, 1, 1.2, 1.2, 1.2, 2.2, 1.4, 1.2])

            cols[0].write(file_name)

            cols[1].write(total_rows)

            cols[2].write(col_count)

            cols[3].write(scraped_count)

            cols[4].write(unscraped_count)

            cols[5].write(status)



            ratio = 0.0

            if total_rows > 0:

                ratio = min(max(scraped_count / total_rows, 0.0), 1.0)

            cols[6].progress(ratio)



            runner = get_or_create_runner(db_path, job_id, file_id, scno_col, workers, vsk2)



            actions_col = cols[7]

            dl_col = cols[8]



            if status == "Scraping":

                if actions_col.button("⏸", key=f"job_pause_{job_id}", help="Pause"):

                    runner.pause()

            elif status == "Paused":

                # Resume must continue with the same worker count saved for this job.

                if actions_col.button("▶", key=f"job_resume_{job_id}", help="Resume"):

                    runner.set_workers(workers)

                    runner.resume()

                    if not runner.is_running():

                        runner.start()

            elif status == "Stopped":

                # Restart flow asks for workers again.

                if actions_col.button("🔁", key=f"job_restart_{job_id}", help="Restart"):

                    st.session_state["restart_job_id"] = job_id

                    print(f"[restart] open restart panel job_id={job_id}")

                    st.rerun()

            elif status == "Done" and unscraped_count > 0:

                # Job finished but there are still missing success rows; allow retrying only the gaps.

                if actions_col.button("♻", key=f"job_retry_{job_id}", help="Retry unscraped rows"):

                    st.session_state["restart_job_id"] = job_id

                    st.session_state[f"restart_mode_{job_id}"] = "Retry gaps (first missing success row)"

                    print(f"[retry] open retry panel job_id={job_id}")

                    st.rerun()

            else:

                actions_col.write("-")



            if status in {"Scraping", "Paused"}:

                if actions_col.button("⏹", key=f"job_stop_{job_id}", help="Stop"):

                    runner.stop()



            if actions_col.button("👁️", key=f"job_preview_{job_id}", help="Preview first 30 rows"):

                st.session_state["job_preview_id"] = job_id



            if actions_col.button("🗑️", key=f"job_delete_{job_id}", help="Delete job permanently"):

                st.session_state["confirm_delete_job_id"] = job_id



            if dl_col.button("⬇️", key=f"job_dl_prepare_{job_id}", help="Prepare download (scraped)"):

                st.session_state["job_download_prepare"] = {"job_id": job_id, "kind": "scraped"}

                st.rerun()



            if dl_col.button("🧾", key=f"job_dl_uns_prepare_{job_id}", help="Prepare download (unscraped)"):

                st.session_state["job_download_prepare"] = {"job_id": job_id, "kind": "unscraped"}

                st.rerun()



        if st.session_state.get("job_download_prepare"):

            st.divider()

            payload = st.session_state["job_download_prepare"]

            jid = int(payload.get("job_id"))

            kind = str(payload.get("kind"))

            if kind == "scraped":

                data = _build_job_output_bytes_cached(db_path, jid)

                st.download_button(

                    "Download scraped Excel",

                    data=data,

                    file_name=f"scraped_job_{jid}.xlsx",

                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

                    key=f"job_dl_ready_{jid}",

                )

            else:

                data = _build_job_unscraped_bytes_cached(db_path, jid)

                st.download_button(

                    "Download unscraped Excel",

                    data=data,

                    file_name=f"unscraped_job_{jid}.xlsx",

                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

                    key=f"job_dl_uns_ready_{jid}",

                )

            if st.button("Close download", key=f"job_dl_close_{jid}"):

                st.session_state["job_download_prepare"] = None

                st.rerun()



        if st.session_state.get("restart_job_id"):

            st.divider()

            jid = int(st.session_state["restart_job_id"])

            det = get_job_details(db_path, jid) or {}

            if not det:

                st.session_state["restart_job_id"] = None

            else:

                total_rows2 = int(det.get("total_rows", 0) or 0)

                scraped_count2 = int(det.get("scraped_count", 0) or 0)

                first_missing_success = get_next_unscraped_row_index(db_path, jid, total_rows2)

                last_attempted = get_last_attempted_row_index(db_path, jid)

                continue_from = None

                if last_attempted is not None:

                    continue_from = int(last_attempted) + 1

                remaining2 = max(total_rows2 - scraped_count2, 0)



                st.subheader(f"Restart job_id={jid}")

                st.write(

                    {

                        "file": det.get("file_name"),

                        "total_rows": total_rows2,

                        "scraped_count": scraped_count2,

                        "remaining": remaining2,

                        "first_missing_success_row_index": first_missing_success,

                        "continue_from_row_index": continue_from,

                    }

                )



                restart_mode = st.radio(

                    "Restart mode",

                    options=["Continue from last attempted row", "Retry gaps (first missing success row)"] ,

                    index=0,

                    key=f"restart_mode_{jid}",

                )



                retryable_count = None

                try:

                    # Count rows that will actually be retried (only non-success rows, optionally filtered by mode).

                    q_all = get_unscraped_rows(db_path, jid, int(det.get("file_id")), str(det.get("scno_column")))

                    if restart_mode == "Continue from last attempted row" and continue_from is not None:

                        q_all = [t for t in q_all if int(t[0]) >= int(continue_from)]

                    elif restart_mode != "Continue from last attempted row" and first_missing_success is not None:

                        q_all = [t for t in q_all if int(t[0]) >= int(first_missing_success)]

                    retryable_count = len(q_all)

                except Exception:

                    retryable_count = None



                if retryable_count is not None:

                    st.write({"retryable_rows": retryable_count})



                workers_new = st.slider(

                    "Workers for restart (parallel browsers)",

                    min_value=1,

                    max_value=100,

                    value=int(det.get("workers", 1) or 1),

                    step=1,

                    key=f"restart_workers_{jid}",

                )

                if workers_new >= 25:

                    st.warning(

                        "High worker count will open many Chrome instances. "

                        "This can crash the machine or trigger website blocking."

                    )



                c1, c2 = st.columns(2)

                if c1.button("Start Restart", type="primary", key=f"restart_start_{jid}"):

                    print(f"[restart] start restart job_id={jid} workers={workers_new}")

                    update_job_workers(db_path, jid, workers_new)

                    update_job_status(db_path, jid, "Scraping")



                    min_row_index = None

                    if restart_mode == "Continue from last attempted row":

                        min_row_index = continue_from

                    else:

                        min_row_index = first_missing_success



                    runner2 = get_or_create_runner(

                        db_path,

                        jid,

                        int(det.get("file_id")),

                        str(det.get("scno_column")),

                        workers_new,

                        vsk2,

                    )

                    runner2.set_workers(workers_new)

                    runner2.set_min_row_index(min_row_index)

                    runner2.start()

                    st.session_state["restart_job_id"] = None

                    st.success("Restart started")

                    st.rerun()

                if c2.button("Cancel Restart", key=f"restart_cancel_{jid}"):

                    print(f"[restart] cancel restart job_id={jid}")

                    st.session_state["restart_job_id"] = None



        st.caption("Use Refresh to update live counts. Scraping continues in background even if you switch tabs.")

        if st.button("Refresh", key="jobs_refresh"):

            st.rerun()



        if st.session_state.get("job_preview_id"):

            st.divider()

            job_id = int(st.session_state["job_preview_id"])

            st.subheader(f"Preview scraped output: job_id={job_id}")

            df_prev = build_scraped_output_dataframe(db_path, job_id).head(30)

            st.dataframe(df_prev, use_container_width=True)

