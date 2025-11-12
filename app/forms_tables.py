from main_file import run_sql
import openpyxl
import os
from sqlalchemy import text
from datetime import datetime


def create_processed_tables(conn):
    os.makedirs("data/processed", exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Noise Reading"

    rows = conn.execute(text("""
        SELECT ts_utc, db_level, part_of_day, src_month
        FROM noise_reading
        ORDER BY reading_id;
    """)).fetchall()

    ws.append(["ts_utc", "db_level", "part_of_day", "src_month"])

    for row in rows:
        cleaned = []
        for val in row:
            if isinstance(val, datetime) and val.tzinfo is not None:
                val = val.replace(tzinfo=None)
            cleaned.append(val)
        ws.append(cleaned)

    try:
        wb.save("data/processed/nr_processed.xlsx")
        print("File successfuly made : data/processed/nr_processed.xlsx")
    except PermissionError:
        print("Already open in Excel.")


if __name__ == "__main__":
    from main_file import connect_engine
    engine = connect_engine()
    with engine.begin() as conn:
        print("→ Creating processed tables...")
        create_processed_tables(conn)
        print("Done.")
