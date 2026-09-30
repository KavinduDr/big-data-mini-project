from datetime import datetime, timedelta
import os
import shutil
import glob
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import pandas as pd

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'fleet_daily_profitability_reconciliation',
    default_args=default_args,
    description='Automated batch reconciliation of vehicle telemetry earnings and garage expenses',
    schedule_interval='*/5 * * * *',  # Every 5 minutes (matching 1 simulated day = 5 mins)
    catchup=False,
    max_active_runs=1,
    tags=['fleet', 'batch', 'reconciliation'],
    doc_md="""
    ### Fleet daily profitability reconciliation
    1. `check_for_new_batch_files` short-circuits the run when the drop folder is empty
       (previously the check result was ignored and downstream tasks ran anyway).
    2. `execute_batch_reconciliation` runs the shared batch engine
       (`batch_layer.batch_processor.run_batch_reconciliation`).
    3. `archive_batch_files` moves processed drops to the archive folder.
    4. `generate_consolidated_report` writes the daily executive summary CSV.
    """,
)

def check_for_new_batch_files(**kwargs):
    drop_dir = os.getenv("DATA_DROP_DIR", "/app/data_lake/daily_expenses")
    files = glob.glob(os.path.join(drop_dir, "*.csv"))
    if not files:
        print(f"No new batch files detected in {drop_dir}.")
        return False
    print(f"Detected {len(files)} batch expense files ready for reconciliation.")
    return True

def execute_batch_reconciliation(**kwargs):
    from batch_layer.batch_processor import run_batch_reconciliation
    summary = run_batch_reconciliation()
    print(f"Reconciliation summary: {summary}")
    if not summary.get("reconciled_records"):
        print("No new simulated dates required reconciliation (idempotent run).")

def archive_batch_files(**kwargs):
    drop_dir = os.getenv("DATA_DROP_DIR", "/app/data_lake/daily_expenses")
    archive_dir = os.getenv("ARCHIVE_DIR", "/app/data_lake/archived_expenses")
    os.makedirs(archive_dir, exist_ok=True)
    files = glob.glob(os.path.join(drop_dir, "*.csv")) + glob.glob(os.path.join(drop_dir, "*.json"))
    for f in files:
        fname = os.path.basename(f)
        dest = os.path.join(archive_dir, fname)
        shutil.move(f, dest)
        print(f"Archived {fname} -> {dest}")

def generate_consolidated_report(**kwargs):
    import psycopg2
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", 5432))
    db = os.getenv("POSTGRES_DB", "fleet_db")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "postgrespassword")
    report_dir = os.getenv("REPORT_OUTPUT_DIR", "/app/data_lake/consolidated_reports")
    os.makedirs(report_dir, exist_ok=True)

    query = """
        SELECT simulated_date, 
               COUNT(*) as fleet_size,
               SUM(gross_earnings) as total_revenue,
               SUM(total_expenses) as total_expenses,
               SUM(net_profit) as net_profit,
               COUNT(*) FILTER (WHERE is_unprofitable = TRUE) as unprofitable_vehicles
        FROM batch_daily_profitability
        GROUP BY simulated_date
        ORDER BY simulated_date DESC
        LIMIT 10;
    """
    # `with` guarantees the connection is closed even if the query fails.
    with psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd) as conn:
        # read_sql_query avoids the SQLAlchemy/UserWarning path of read_sql.
        df = pd.read_sql_query(query, conn)
    report_file = os.path.join(report_dir, "daily_fleet_executive_summary.csv")
    df.to_csv(report_file, index=False)
    print(f"Generated daily executive summary report at {report_file}")

task_check = ShortCircuitOperator(
    task_id='check_for_new_batch_files',
    python_callable=check_for_new_batch_files,
    dag=dag,
)

task_reconcile = PythonOperator(
    task_id='execute_batch_reconciliation',
    python_callable=execute_batch_reconciliation,
    dag=dag,
)

task_archive = PythonOperator(
    task_id='archive_batch_files',
    python_callable=archive_batch_files,
    dag=dag,
)

task_report = PythonOperator(
    task_id='generate_consolidated_report',
    python_callable=generate_consolidated_report,
    dag=dag,
)

task_check >> task_reconcile >> task_archive >> task_report
