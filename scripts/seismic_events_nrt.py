#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, time, json, uuid, random, signal
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ===================== Config via env =====================
MODE          = os.getenv("MODE", "file")           # "file" | "es" | "both"
ES_URL        = os.getenv("ES_URL", "http://localhost:9200")
INDEX_PREFIX  = os.getenv("INDEX_PREFIX", "seismic_rt")
OUT_DIR       = Path(os.getenv("OUT_DIR", "/tmp/rt_events"))
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "25"))  # events per batch
INTERVAL_SEC  = float(os.getenv("INTERVAL_SEC", "1.0"))
STATIONS      = [s.strip() for s in os.getenv("STATIONS", "COTO,PUEL,QUIL,GUAM").split(",") if s.strip()]

# Quito/Cotopaxi approx as reference for lat/lon
LAT0, LON0 = -0.680, -78.436
LAT_JITTER = 0.35
LON_JITTER = 0.35

# Toggle proximity field (it’s seismic geometry, not socioeconomic)
INCLUDE_PROXIMITY_KM = os.getenv("INCLUDE_PROXIMITY_KM", "1") == "1"

# ===================== Optional ES init =====================
ES = None
helpers = None

def _init_es():
    """Initialize ES client if needed."""
    global ES, helpers
    try:
        from elasticsearch import Elasticsearch, helpers as es_helpers
        ES = Elasticsearch(ES_URL, request_timeout=30)
        helpers = es_helpers
        try:
            ES.ping()
        except Exception:
            pass
    except Exception as e:
        print(f"[ERROR] Initializing ES: {e}", file=sys.stderr)

def es_index_name():
    """Daily index: seismic_rt-YYYY.MM.DD"""
    return f"{INDEX_PREFIX}-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"

def ensure_index():
    """Create index with explicit seismic mapping (no socioeconomic fields)."""
    if ES is None:
        return
    idx = es_index_name()
    try:
        if not ES.indices.exists(index=idx):
            body = {
                "mappings": {
                    "properties": {
                        "id":              {"type": "keyword"},
                        "event_id":        {"type": "keyword"},
                        "waveform_id":     {"type": "keyword"},
                        "cluster_id":      {"type": "keyword"},
                        "station_id":      {"type": "keyword"},
                        "ts_event":        {"type": "date"},
                        "event_type":      {"type": "keyword"},   # VT, LP, TR, EX
                        "magnitude_ml":    {"type": "float"},
                        "depth_km":        {"type": "float"},
                        "intensity_mmi":   {"type": "short"},
                        "phase_picks_p":   {"type": "short"},
                        "phase_picks_s":   {"type": "short"},
                        "snr_db":          {"type": "float"},
                        "rms_residual":    {"type": "float"},
                        "azimuthal_gap":   {"type": "float"},
                        "h_error_km":      {"type": "float"},
                        "v_error_km":      {"type": "float"},
                        "quality":         {"type": "keyword"},   # A/B/C/D
                        "latitude":        {"type": "float"},
                        "longitude":       {"type": "float"},
                        "near_volcano_km": {"type": "float"},
                        "is_aftershock":   {"type": "boolean"},
                        "parent_event":    {"type": "keyword"},
                        "source":          {"type": "keyword"},
                        "updated_at":      {"type": "date"}
                    }
                }
            }
            if not INCLUDE_PROXIMITY_KM:
                del body["mappings"]["properties"]["near_volcano_km"]
            ES.indices.create(index=idx, body=body)
            print(f"[ES] Created index {idx}")
    except Exception as e:
        print(f"[ERROR] ensure_index: {e}", file=sys.stderr)

# ===================== Seismic helpers =====================
def rand_coord(base, jitter):
    return base + random.uniform(-jitter, jitter)

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def km_distance_approx(lat1, lon1, lat2, lon2):
    # Rough planar approx near equator (good enough for synthetic data)
    dx = (lon2 - lon1) * 111.32 * abs(math.cos(math.radians((lat1 + lat2)/2.0)))
    dy = (lat2 - lat1) * 110.57
    return (dx*dx + dy*dy) ** 0.5

def sample_magnitude_ml():
    """
    Gutenberg–Richter-like: many small, few larger.
    Use exponential with mean ~0.4 and floor at 0.1, cap 4.5.
    """
    # random.expovariate(lam) has mean 1/lam; want mean ≈ 0.4 → lam ≈ 2.5
    mag = 0.1 + random.expovariate(2.5)
    return round(clamp(mag, 0.1, 4.5), 2)

def sample_depth_km():
    """Shallow depth around volcano; truncated normal-ish via gauss, clamped 0.2–20 km."""
    d = random.gauss(5.0, 3.0)
    return round(clamp(d, 0.2, 20.0), 2)

def choose_event_type(mag, depth):
    """
    Very rough rule:
      - LP more common when shallow & small mag
      - VT at broader depths and moderate mags
      - TR (tremor) occasionally at small-moderate mag
      - EX (explosive-like) rare, needs higher mag
    """
    r = random.random()
    if mag > 3.0 and r < 0.10:
        return "EX"
    if depth < 3.0 and mag < 1.5 and r < 0.50:
        return "LP"
    if r < 0.25:
        return "TR"
    return "VT"

def quality_from_uncertainty(h_err, v_err, gap):
    score = h_err + 0.7*v_err + (gap/360.0)*2.0
    if score < 2.0:  return "A"
    if score < 4.0:  return "B"
    if score < 6.0:  return "C"
    return "D"

# Track recent larger events for simple aftershock flagging
RECENT_MAINSHOCKS = []  # list of (event_id, ts, lat, lon, mag)

def register_mainshock(evt):
    # consider "mainshock" if mag >= 2.8
    if evt["magnitude_ml"] >= 2.8:
        RECENT_MAINSHOCKS.append(
            (evt["event_id"], datetime.now(timezone.utc), evt["latitude"], evt["longitude"], evt["magnitude_ml"])
        )
        # keep last ~50
        if len(RECENT_MAINSHOCKS) > 50:
            del RECENT_MAINSHOCKS[0]

def maybe_aftershock(lat, lon):
    """
    Very simple Omori-style idea: aftershocks more likely shortly after larger events and nearby.
    Returns (is_aftershock, parent_event_id or None)
    """
    now = datetime.now(timezone.utc)
    # prune > 12h
    cutoff = now - timedelta(hours=12)
    candidates = [ms for ms in RECENT_MAINSHOCKS if ms[1] >= cutoff]
    if not candidates:
        return False, None
    # proximity check (rough)
    for (eid, ts, ms_lat, ms_lon, mag) in reversed(candidates):
        dt_hours = (now - ts).total_seconds() / 3600.0
        # probability decays with time and distance, grows with mag
        dist_km = ((lat - ms_lat)**2 + (lon - ms_lon)**2) ** 0.5 * 110.0
        p = max(0.0, (mag - 2.5) * 0.08) * (1.0 / (1.0 + dt_hours)) * (1.0 / (1.0 + dist_km/15.0))
        if random.random() < p:
            return True, eid
    return False, None

import math

# ===================== Event generator =====================
def gen_event():
    mag  = sample_magnitude_ml()
    depth = sample_depth_km()
    lat = round(rand_coord(LAT0, LAT_JITTER), 5)
    lon = round(rand_coord(LON0, LON_JITTER), 5)

    # Basic signal/solution quality proxies
    snr_db = round(clamp(random.gauss(12.0 + mag*3.0, 5.0), 0.5, 50.0), 2)
    az_gap = round(clamp(random.uniform(30, 240), 10, 360), 1)
    h_err  = round(clamp(random.gauss(1.2, 0.8), 0.05, 8.0), 2)
    v_err  = round(clamp(random.gauss(1.8, 1.0), 0.05, 10.0), 2)
    rms    = round(clamp(random.gauss(0.12, 0.08), 0.005, 0.8), 3)
    picks_p = max(1, int(random.gauss(5 + mag*2, 2)))
    picks_s = max(0, int(random.gauss(3 + mag, 2)))

    evt_type = choose_event_type(mag, depth)
    event_id = str(uuid.uuid4())
    waveform_id = str(uuid.uuid4())

    is_after, parent = maybe_aftershock(lat, lon)
    quality = quality_from_uncertainty(h_err, v_err, az_gap)

    doc = {
        "id": str(uuid.uuid4()),
        "event_id": event_id,
        "waveform_id": waveform_id,
        "cluster_id": None,  # could be filled by downstream clustering
        "station_id": random.choice(STATIONS) if STATIONS else "COTO",
        "ts_event": datetime.now(timezone.utc).isoformat(),
        "event_type": evt_type,
        "magnitude_ml": mag,
        "depth_km": depth,
        "intensity_mmi": random.randint(1, 3),  # micro events in NRT feed
        "phase_picks_p": picks_p,
        "phase_picks_s": picks_s,
        "snr_db": snr_db,
        "rms_residual": rms,
        "azimuthal_gap": az_gap,
        "h_error_km": h_err,
        "v_error_km": v_err,
        "quality": quality,
        "latitude": lat,
        "longitude": lon,
        "near_volcano_km": None,  # optionally filled below
        "is_aftershock": is_after,
        "parent_event": parent,
        "source": "python-nrt-seismic",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    if INCLUDE_PROXIMITY_KM:
        # simple approximate great-circle distance (already imported)
        # using planar local approximation above:
        # We'll compute with more robust approx here:
        doc["near_volcano_km"] = round(km_distance_approx(lat, lon, LAT0, LON0), 2)

    # register as potential mainshock for future aftershocks
    register_mainshock(doc)
    return doc

# ===================== File emitter =====================
class FileEmitter:
    """
    Rotates per minute:
      seismic_YYYYmmdd_HHMM.jsonl
    Creates a *_DONE marker per batch (useful for Airflow sensors).
    """
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._current_path = None
        self._fh = None

    def _rotate_if_needed(self):
        name = f"seismic_{datetime.now().strftime('%Y%m%d_%H%M')}.jsonl"
        target = self.out_dir / name
        if self._current_path != target:
            self._close_file()
            self._current_path = target
            self._fh = open(target, "a", encoding="utf-8")
        return self._fh

    def write_batch(self, docs):
        fh = self._rotate_if_needed()
        for d in docs:
            fh.write(json.dumps(d, ensure_ascii=False) + "\n")
        fh.flush()
        marker = str(self._current_path).replace(".jsonl", f"_{int(time.time())}_DONE")
        Path(marker).touch(exist_ok=True)
        return self._current_path, marker

    def _close_file(self):
        if self._fh:
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass
            finally:
                self._fh = None

    def close(self):
        self._close_file()

# ===================== ES bulk =====================
def es_bulk(docs):
    """Send a batch via _bulk to the current daily index."""
    if ES is None or helpers is None:
        return 0
    idx = es_index_name()
    ensure_index()
    actions = ({"_op_type": "index", "_index": idx, "_source": d} for d in docs)
    ok, _ = helpers.bulk(ES, actions, chunk_size=min(len(docs), 500))
    return ok

# ===================== Signals =====================
_STOP = False
def _handle_sig(signum, frame):
    global _STOP
    _STOP = True

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)

# ===================== Main loop =====================
def main():
    if MODE in ("es", "both"):
        _init_es()

    file_emitter = FileEmitter(OUT_DIR) if MODE in ("file", "both") else None
    print(f"[START] MODE={MODE} BATCH_SIZE={BATCH_SIZE} INTERVAL_SEC={INTERVAL_SEC}")
    total = 0
    buffer = []

    try:
        while not _STOP:
            evt = gen_event()
            buffer.append(evt)
            total += 1

            if len(buffer) >= BATCH_SIZE:
                if file_emitter:
                    path, marker = file_emitter.write_batch(buffer)
                    print(f"[FILE] Wrote {len(buffer)} -> {path.name} (+marker)")
                if MODE in ("es", "both"):
                    ok = es_bulk(buffer)
                    print(f"[ES] Indexed {ok} docs into {es_index_name()}")
                buffer = []

            time.sleep(INTERVAL_SEC)

        # final flush
        if buffer:
            if file_emitter:
                path, marker = file_emitter.write_batch(buffer)
                print(f"[FILE] (flush) {len(buffer)} -> {path.name}")
            if MODE in ("es", "both"):
                ok = es_bulk(buffer)
                print(f"[ES] (flush) {ok} docs")

    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)

    finally:
        if file_emitter:
            file_emitter.close()
        print(f"[STOP] total events generated: {total}")

if __name__ == "__main__":
    main()
