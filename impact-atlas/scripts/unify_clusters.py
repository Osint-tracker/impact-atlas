"""
ENHANCED FUSION PROTOCOL V6 - Tiered Thresholds + Multi-Signal Matching
========================================================================
This script merges GDELT and Telegram events using:
1. Tiered similarity thresholds (same-source vs cross-source)
2. Keyword overlap detection (locations, units, dates)
3. Multi-signal matching (lower threshold when 3+ signals align)
"""

import sqlite3
import numpy as np
import json
import time
import re
from datetime import datetime, timedelta, timezone
import logging
import dateutil.parser
import sys
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

DB_PATH = 'war_tracker_v2/data/raw_events.db'

# === TIERED THRESHOLDS ===
THRESHOLD_SAME_SOURCE = 0.85      # GDELT<->GDELT or TG<->TG
THRESHOLD_CROSS_SOURCE = 0.70     # GDELT<->TELEGRAM
THRESHOLD_MULTI_SIGNAL = 0.65     # When 3+ signals match

# === KEYWORD LISTS FOR SIGNAL MATCHING ===
# Ukrainian locations (oblasts, cities, strategic points)
LOCATIONS = [
    'kyiv', 'kharkiv', 'odesa', 'odessa', 'mariupol', 'kherson', 'zaporizhzhia',
    'donetsk', 'luhansk', 'lugansk', 'bakhmut', 'avdiivka', 'kramatorsk', 'sloviansk',
    'pokrovsk', 'toretsk', 'chasiv yar', 'kupiansk', 'vuhledar', 'marinka',
    'melitopol', 'berdyansk', 'enerhodar', 'mykolaiv', 'sumy', 'chernihiv',
    'crimea', 'sevastopol', 'kerch', 'dnipro', 'poltava', 'zhytomyr',
    # Oblasts
    'donetsk oblast', 'luhansk oblast', 'kherson oblast', 'zaporizhzhia oblast',
    'kharkiv oblast', 'kursk oblast', 'belgorod'
]

# Military units and formations
UNITS = [
    'brigade', 'battalion', 'regiment', 'division', 'corps', 'army',
    'azov', 'wagner', 'pmc', 'gru', 'fsb', 'sbu',
    'marines', 'airborne', 'vdv', 'spetsnaz', 'national guard',
    '72nd', '93rd', '47th', '128th', '110th', '59th',  # UA brigades
    '58th', '144th', '76th', '98th', '104th',  # RU formations
]

# Weapon systems
WEAPONS = [
    'himars', 'atacms', 'mlrs', 'grad', 'tornado', 'hurricane',
    'leopard', 'abrams', 'challenger', 'bradley', 't-72', 't-90', 't-80',
    'bmp', 'btr', 'apc', 'ifv', 'mrap',
    's-300', 's-400', 'patriot', 'iris-t', 'nasams',
    'shahed', 'geran', 'lancet', 'orlan', 'fpv', 'drone', 'uav',
    'iskander', 'kinzhal', 'kalibr', 'storm shadow', 'scalp',
    'f-16', 'su-34', 'su-35', 'mig-31', 'ka-52', 'mi-28'
]

# Action keywords
ACTIONS = [
    'strike', 'attack', 'shell', 'bomb', 'hit', 'destroy', 'damage',
    'advance', 'retreat', 'capture', 'liberate', 'assault', 'offensive',
    'intercept', 'shoot down', 'explosion', 'fire', 'casualties'
]


def cosine_similarity(v1, v2):
    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))


def parse_vector(blob_or_str):
    if not blob_or_str:
        return None
    try:
        if isinstance(blob_or_str, bytes):
            return np.frombuffer(blob_or_str, dtype=np.float32)
        return np.array(json.loads(blob_or_str), dtype=np.float32)
    except:
        return None


def robust_parse_date(date_str):
    if not date_str: return None
    dt = None
    try: dt = datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
    except: pass
    if not dt:
        try: dt = datetime.strptime(str(date_str), "%Y%m%d%H%M%S")
        except: pass
    if not dt:
        try: dt = dateutil.parser.parse(str(date_str))
        except: pass
    if dt:
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        else: dt = dt.astimezone(timezone.utc)
    return dt


def extract_keywords(text):
    """Extract matching keywords from text for signal scoring."""
    if not text:
        return set()
    
    text_lower = text.lower()
    found = set()
    
    for loc in LOCATIONS:
        if loc in text_lower:
            found.add(f"LOC:{loc}")
    
    for unit in UNITS:
        if unit in text_lower:
            found.add(f"UNIT:{unit}")
    
    for weapon in WEAPONS:
        if weapon in text_lower:
            found.add(f"WEAP:{weapon}")
    
    for action in ACTIONS:
        if action in text_lower:
            found.add(f"ACT:{action}")
    
    # Extract dates (format: DD/MM, Month DD, etc.)
    date_patterns = [
        r'\b(\d{1,2})[/.-](\d{1,2})\b',  # DD/MM or DD-MM
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})\b',
    ]
    for pattern in date_patterns:
        matches = re.findall(pattern, text_lower)
        for m in matches:
            found.add(f"DATE:{m}")
    
    return found


def calculate_signal_score(keywords1, keywords2, time_delta_hours):
    """
    Calculate multi-signal matching score.
    Returns number of matching signals (0-5 scale).
    """
    signals = 0
    
    # Signal 1: Time proximity (within 24h = 1 signal, within 6h = 2 signals)
    if time_delta_hours <= 6:
        signals += 2
    elif time_delta_hours <= 24:
        signals += 1
    
    # Signal 2: Location overlap
    loc1 = {k for k in keywords1 if k.startswith('LOC:')}
    loc2 = {k for k in keywords2 if k.startswith('LOC:')}
    if loc1 & loc2:
        signals += 1
    
    # Signal 3: Unit/weapon overlap
    unit1 = {k for k in keywords1 if k.startswith('UNIT:') or k.startswith('WEAP:')}
    unit2 = {k for k in keywords2 if k.startswith('UNIT:') or k.startswith('WEAP:')}
    if unit1 & unit2:
        signals += 1
    
    # Signal 4: Action type overlap
    act1 = {k for k in keywords1 if k.startswith('ACT:')}
    act2 = {k for k in keywords2 if k.startswith('ACT:')}
    if act1 & act2:
        signals += 1
    
    return signals


def get_dynamic_threshold(source1, source2, signal_count):
    """
    Determine the appropriate similarity threshold based on:
    - Source types (same vs cross-source)
    - Number of matching signals
    """
    is_cross_source = (source1 == 'GDELT') != (source2 == 'GDELT')
    
    # Multi-signal override: if 3+ signals match, use lower threshold
    if signal_count >= 3:
        return THRESHOLD_MULTI_SIGNAL
    
    # Otherwise use tiered thresholds
    if is_cross_source:
        return THRESHOLD_CROSS_SOURCE
    else:
        return THRESHOLD_SAME_SOURCE


def unify_clusters(incremental=False, recent_days=None):
    logging.info("=" * 60)
    logging.info("FUSION PROTOCOL V6 - TIERED THRESHOLDS + MULTI-SIGNAL")
    logging.info("=" * 60)
    
    if incremental:
        logging.info("*** INCREMENTAL MODE: Only processing new events ***")
    if recent_days:
        logging.info(f"*** RECENT MODE: Only events from last {recent_days} days ***")
    
    logging.info(f"Thresholds: Same={THRESHOLD_SAME_SOURCE}, Cross={THRESHOLD_CROSS_SOURCE}, Multi-Signal={THRESHOLD_MULTI_SIGNAL}")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. Date Range
    logging.info("Checking Timeline...")
    c.execute("SELECT MIN(date_published), MAX(date_published) FROM raw_signals WHERE embedding_vector IS NOT NULL AND length(embedding_vector) > 100")
    min_s, max_s = c.fetchone()
    start_date = robust_parse_date(min_s) or (datetime.now(timezone.utc) - timedelta(days=60))
    end_date = robust_parse_date(max_s) or datetime.now(timezone.utc)
    logging.info(f"Timeline: {start_date.date()} -> {end_date.date()}")

    # 2. Build Index with text content for keyword extraction
    logging.info("Building Date Index with Keywords...")
    
    # Build WHERE clause based on mode
    where_clauses = ["embedding_vector IS NOT NULL", "length(embedding_vector) > 100"]
    
    if incremental:
        # Only process events without a fused cluster_id (new events)
        where_clauses.append("(cluster_id IS NULL OR cluster_id NOT LIKE 'fus_%')")
    
    if recent_days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=recent_days)).strftime('%Y%m%d')
        where_clauses.append(f"date_published >= '{cutoff}'")
    
    where_sql = " AND ".join(where_clauses)
    
    c.execute(f"""
        SELECT rowid, date_published, text_content, source_type 
        FROM raw_signals 
        WHERE {where_sql}
    """)
    
    date_index = []
    text_cache = {}  # rowid -> (keywords, source_type)
    
    for r in c.fetchall():
        dt = robust_parse_date(r[1])
        if dt:
            rowid = int(r[0])
            date_index.append((rowid, dt))
            text_cache[rowid] = (extract_keywords(r[2]), r[3], dt)
    
    date_index.sort(key=lambda x: x[1])
    logging.info(f"Index Built: {len(date_index)} events with keywords extracted.")

    # 3. Clustering with Tiered Thresholds
    CHUNK_SIZE = 5000
    active_clusters = []
    # Structure: { 'id': str, 'centroid': np.array, 'last_seen': datetime, 'count': int, 
    #              'g': bool, 't': bool, 'keywords': set, 'source_type': str }
    
    total_fused = 0
    total_processed = 0
    stats = {'same_match': 0, 'cross_match': 0, 'multi_signal_match': 0, 'new_cluster': 0}

    for i in range(0, len(date_index), CHUNK_SIZE):
        chunk = date_index[i : i+CHUNK_SIZE]
        chunk_ids = [item[0] for item in chunk]
        id_map = {item[0]: item[1] for item in chunk}
        
        placeholders = ','.join(['?']*len(chunk_ids))
        query = f"SELECT rowid, embedding_vector, source_type FROM raw_signals WHERE rowid IN ({placeholders})"
        c.execute(query, chunk_ids)
        rows = c.fetchall()
        
        batch_items = []
        for r in rows:
            rid = r[0]
            vec = parse_vector(r[1])
            if vec is not None:
                keywords, stype, dt = text_cache.get(rid, (set(), r[2], id_map.get(rid)))
                batch_items.append({
                    'id': rid,
                    'date': dt,
                    'vec': vec,
                    'type': stype,
                    'keywords': keywords
                })
        
        # Pruning (48h window)
        if batch_items:
            chunk_start_time = batch_items[0]['date']
            active_clusters = [cl for cl in active_clusters if (chunk_start_time - cl['last_seen']).total_seconds() < 48*3600]

        batch_updates = []
        
        for item in batch_items:
            best_score = -1.0
            best_idx = -1
            best_threshold = 1.0
            best_signals = 0
            
            for idx, cl in enumerate(active_clusters):
                # Calculate cosine similarity
                score = cosine_similarity(item['vec'], cl['centroid'])
                
                # Calculate signal match count
                time_delta = abs((item['date'] - cl['last_seen']).total_seconds() / 3600)
                signals = calculate_signal_score(item['keywords'], cl['keywords'], time_delta)
                
                # Get dynamic threshold
                threshold = get_dynamic_threshold(item['type'], cl['source_type'], signals)
                
                # Check if this is a better match
                if score > threshold and score > best_score:
                    best_score = score
                    best_idx = idx
                    best_threshold = threshold
                    best_signals = signals
            
            if best_idx >= 0:
                # MERGE into existing cluster
                target = active_clusters[best_idx]
                cid = target['id']
                
                # Update centroid (moving average)
                target['centroid'] = (target['centroid'] * target['count'] + item['vec']) / (target['count'] + 1)
                target['count'] += 1
                target['last_seen'] = max(target['last_seen'], item['date'])
                target['keywords'] = target['keywords'] | item['keywords']
                
                # Track fusion
                was_mixed = (target['g'] and target['t'])
                target['g'] = target['g'] or (item['type'] == 'GDELT')
                target['t'] = target['t'] or (item['type'] != 'GDELT')
                
                if (not was_mixed) and (target['g'] and target['t']):
                    total_fused += 1
                
                # Track match type
                is_cross = (item['type'] == 'GDELT') != (target['source_type'] == 'GDELT')
                if best_signals >= 3:
                    stats['multi_signal_match'] += 1
                elif is_cross:
                    stats['cross_match'] += 1
                else:
                    stats['same_match'] += 1
            else:
                # NEW cluster
                cid = f"fus_{item['id']}_{int(time.time())}"
                active_clusters.append({
                    'id': cid,
                    'centroid': item['vec'],
                    'last_seen': item['date'],
                    'count': 1,
                    'g': (item['type'] == 'GDELT'),
                    't': (item['type'] != 'GDELT'),
                    'keywords': item['keywords'],
                    'source_type': item['type']
                })
                stats['new_cluster'] += 1
            
            batch_updates.append((cid, item['id']))
        
        if batch_updates:
            c.executemany("UPDATE raw_signals SET cluster_id=? WHERE rowid=?", batch_updates)
            conn.commit()
            
        total_processed += len(batch_items)
        if total_processed % 10000 == 0:
            pct = int(i / len(date_index) * 100)
            logging.info(f"   [{pct}%] Processed {total_processed:,}. Active: {len(active_clusters):,} | Fused: {total_fused:,}")

    # Final Summary
    logging.info("=" * 60)
    logging.info("FUSION COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Total Processed: {total_processed:,}")
    logging.info(f"Mixed Clusters (GDELT+TG): {total_fused:,}")
    logging.info(f"Match Types:")
    logging.info(f"  - Same-source matches: {stats['same_match']:,}")
    logging.info(f"  - Cross-source matches: {stats['cross_match']:,}")
    logging.info(f"  - Multi-signal matches: {stats['multi_signal_match']:,}")
    logging.info(f"  - New clusters created: {stats['new_cluster']:,}")
    
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fusion Protocol V6 - Cluster Events")
    parser.add_argument("--incremental", action="store_true",
                        help="Only process new events (those without fus_ cluster_id)")
    parser.add_argument("--recent", type=int, nargs='?', const=7,
                        help="Only process events from last N days (default: 7)")
    args = parser.parse_args()
    
    unify_clusters(incremental=args.incremental, recent_days=args.recent)
