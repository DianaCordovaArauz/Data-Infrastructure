# dags/dag_nrt_seismic_files_to_es.py
from datetime import datetime
import os, glob, json, shutil
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch, helpers

ES_URL     = os.getenv("ES_URL", "http://localhost:9200")
DATA_DIR   = Path(os.getenv("RT_DIR", "/tmp/rt_events"))
ARCHIVE    = DATA_DIR / "processed"
INDEX_PREF = os.getenv("SEIS_INDEX_PREFIX", "seismic_rt")

def _es():
    return Elasticsearch(ES_URL, request_timeout=60)

def es_index_name():
    from datetime import datetime, timezone
    return f"{INDEX_PREF}-{datetime.now().strftime('%Y.%m.%d')}"

def list_done_markers(**_):
    markers = sorted(glob.glob(str(DATA_DIR / "seismic_*.jsonl_*_DONE")))
    # procesa máx. 20 por corrida para no saturar
    return markers[:20]

def bulk_index_from_markers(ti, **_):
    es = _es()
    idx = es_index_name()
    if not es.indices.exists(index=idx):
        # mapping minimal (ya definido en tu script NRT); crea si hace falta
        es.indices.create(index=idx, body={
            "mappings": {
                "properties": {
                    "id": {"type":"keyword"},
                    "event_id":{"type":"keyword"},
                    "waveform_id":{"type":"keyword"},
                    "cluster_id":{"type":"keyword"},
                    "station_id":{"type":"keyword"},
                    "ts_event":{"type":"date"},
                    "event_type":{"type":"keyword"},
                    "magnitude_ml":{"type":"float"},
                    "depth_km":{"type":"float"},
                    "intensity_mmi":{"type":"short"},
                    "phase_picks_p":{"type":"short"},
                    "phase_picks_s":{"type":"short"},
                    "snr_db":{"type":"float"},
                    "rms_residual":{"type":"float"},
                    "azimuthal_gap":{"type":"float"},
                    "h_error_km":{"type":"float"},
                    "v_error_km":{"type":"float"},
                    "quality":{"type":"keyword"},
                    "latitude":{"type":"float"},
                    "longitude":{"type":"float"},
                    "near_volcano_km":{"type":"float"},
                    "is_aftershock":{"type":"boolean"},
                    "parent_event":{"type":"keyword"},
                    "source":{"type":"keyword"},
                    "updated_at":{"type":"date"}
                }
            }
        })
    markers = ti.xcom_pull(task_ids="list_done_markers") or []
    total = 0
    for mk in markers:
        jsonl = mk.split("_DONE")[0]     # mismo prefijo sin sufijo
        if not os.path.exists(jsonl):
            continue
        actions = []
        with open(jsonl, "r", encoding="utf-8") as f:
            for line in f:
                doc = json.loads(line)
                actions.append({"_op_type":"index","_index":idx,"_source":doc})
                if len(actions) >= 2000:
                    helpers.bulk(es, actions); actions.clear()
        if actions: helpers.bulk(es, actions)
        total += 1
    es.indices.refresh(index=idx)
    return {"processed_markers": markers, "index": idx}

def archive_and_cleanup(ti, **_):
    info = ti.xcom_pull(task_ids="bulk_index_from_markers") or {}
    markers = info.get("processed_markers", [])
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    for mk in markers:
        jsonl = mk.split("_DONE")[0]
        base  = os.path.basename(jsonl)
        # mueve jsonl y marker
        if os.path.exists(jsonl):
            shutil.move(jsonl, ARCHIVE / base)
        if os.path.exists(mk):
            shutil.move(mk, ARCHIVE / os.path.basename(mk))

with DAG(
    dag_id="nrt_seismic_files_to_es",
    start_date=datetime(2025,1,1),
    schedule_interval="*/2 * * * *",  # cada 2 minutos
    catchup=False,
    max_active_runs=1,
    tags=["nrt","seismic","es"],
) as dag:
    t1 = PythonOperator(task_id="list_done_markers", python_callable=list_done_markers)
    t2 = PythonOperator(task_id="bulk_index_from_markers", python_callable=bulk_index_from_markers)
    t3 = PythonOperator(task_id="archive_and_cleanup", python_callable=archive_and_cleanup)
    t1 >> t2 >> t3
