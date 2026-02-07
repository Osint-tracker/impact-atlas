"""
Semantic Narrative Generator (v1.0)
Clusters military events by Space + Time + Semantics using DBSCAN,
then generates AI-powered strategic narratives for each cluster.

Author: Impact Atlas Team
Date: February 2026
"""
import sqlite3
import json
import os
import sys
import hashlib
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

# Machine Learning
from sklearn.cluster import DBSCAN

# Geometry
from shapely.geometry import MultiPoint, Polygon
from shapely.ops import unary_union

# LLM
from openai import OpenAI
from dotenv import load_dotenv

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
OUTPUT_PATH = os.path.join(BASE_DIR, '../assets/data/narratives.json')

load_dotenv()

# Clustering Parameters
WINDOW_DAYS = 30                   # Rolling window for event ingestion (increased for more data)
DBSCAN_EPS = 0.15                  # ~15-20km in degrees
DBSCAN_MIN_SAMPLES = 3             # Minimum events per cluster
SEMANTIC_THRESHOLD = 0.75          # Cosine similarity threshold
TEMPORAL_WINDOW_HOURS = 72         # Max time spread within cluster
BUFFER_DEGREES = 0.02              # ~2km polygon buffer

# Tactic Colors (for frontend)
TACTIC_COLORS = {
    'ATTRITION': '#f59e0b',        # Amber
    'MANOEUVRE': '#3b82f6',        # Blue
    'SHAPING': '#8b5cf6',          # Purple
    'SHAPING_OFFENSIVE': '#8b5cf6',
    'SHAPING_COERCIVE': '#ef4444', # Red
    'LOGISTICS': '#22c55e',        # Green
    'INCOHERENT_DISARRAY': '#64748b', # Gray
    'UNKNOWN': '#94a3b8'           # Slate
}


class NarrativeEngine:
    """
    Generates strategic narratives from clustered military events.
    Uses DBSCAN for spatial clustering, cosine similarity for semantic
    refinement, and LLM for narrative synthesis.
    """
    
    def __init__(self):
        """Initialize database connection and LLM client."""
        self.conn = None
        self.llm_client = None
        self._init_db()
        self._init_llm()
        
    def _init_db(self):
        """Connect to SQLite database."""
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Database not found: {DB_PATH}")
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
        print(f"[DB] Connected to {DB_PATH}")
        
    def _init_llm(self):
        """Initialize OpenRouter client for The Strategist agent."""
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            print("[WARN] OPENROUTER_API_KEY not found. LLM narratives disabled.")
            return
            
        self.llm_client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
            default_headers={"X-Title": "Impact Atlas - Narrative Engine"}
        )
        print("[LLM] OpenRouter client initialized")
        
    def _load_events(self, window_days: int = WINDOW_DAYS) -> List[Dict]:
        """
        Load events from the last N days with required columns.
        Uses rolling window to avoid cutting off ongoing battles.
        Embeddings are now optional to support spatial-only clustering.
        """
        cutoff_date = (datetime.now() - timedelta(days=window_days)).isoformat()
        
        cursor = self.conn.cursor()
        # Note: embedding_vector is now optional (removed from WHERE clause)
        cursor.execute("""
            SELECT 
                event_id,
                last_seen_date,
                full_text_dossier,
                embedding_vector,
                ai_report_json
            FROM unique_events 
            WHERE ai_analysis_status = 'COMPLETED'
              AND last_seen_date >= ?
              AND last_seen_date != 'NaT'
            ORDER BY last_seen_date DESC
        """, (cutoff_date,))
        
        rows = cursor.fetchall()
        events = []
        
        for row in rows:
            try:
                # Parse embedding vector (now optional)
                embedding = None
                if row['embedding_vector']:
                    try:
                        embedding = json.loads(row['embedding_vector'])
                        if embedding and len(embedding) >= 100:
                            embedding = np.array(embedding)
                        else:
                            embedding = None
                    except:
                        embedding = None
                    
                # Parse AI report for coordinates and intensity
                ai_data = {}
                lat, lon = None, None
                intensity_score = 5  # Default
                classification = 'UNKNOWN'
                
                if row['ai_report_json']:
                    ai_data = json.loads(row['ai_report_json'])
                    
                    # Extract coordinates
                    tactics = ai_data.get('tactics', {})
                    geo = tactics.get('geo_location', {}).get('explicit', {})
                    lat = geo.get('lat')
                    lon = geo.get('lon')
                    
                    # Extract intensity (kinetic score)
                    intensity_score = ai_data.get('scores', {}).get('kinetic', 5)
                    classification = ai_data.get('classification', 'UNKNOWN')
                
                # Skip if no valid coordinates
                if not lat or not lon or float(lat) == 0 or float(lon) == 0:
                    continue
                    
                # Parse date (normalize to naive datetime for comparisons)
                date_str = row['last_seen_date']
                try:
                    event_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    # Strip timezone to make it naive for consistent comparisons
                    if event_date.tzinfo is not None:
                        event_date = event_date.replace(tzinfo=None)
                except:
                    event_date = datetime.now()
                
                events.append({
                    'event_id': row['event_id'],
                    'lat': float(lat),
                    'lon': float(lon),
                    'date': event_date,
                    'date_str': date_str[:10] if date_str else 'Unknown',
                    'text': row['full_text_dossier'] or '',
                    'embedding': embedding,  # Can be None now
                    'intensity_score': float(intensity_score),
                    'classification': classification
                })
                
            except Exception as e:
                print(f"[WARN] Failed to parse event {row['event_id']}: {e}")
                continue
                
        print(f"[DATA] Loaded {len(events)} events from last {window_days} days")
        emb_count = len([e for e in events if e['embedding'] is not None])
        print(f"[DATA] Events with embeddings: {emb_count}/{len(events)}")
        return events
        
    def _compute_cosine_similarity(self, vec_a: np.ndarray, vec_b: np.ndarray) -> float:
        """Compute cosine similarity between two vectors."""
        norm_a = np.linalg.norm(vec_a)
        norm_b = np.linalg.norm(vec_b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(vec_a, vec_b) / (norm_a * norm_b))
        
    def _cluster_events(self, events: List[Dict]) -> List[List[Dict]]:
        """
        Two-stage clustering: DBSCAN spatial + semantic refinement.
        
        Stage 1: DBSCAN on lat/lon coordinates
        Stage 2: Split clusters with low semantic coherence
        """
        if len(events) < DBSCAN_MIN_SAMPLES:
            print("[CLUSTER] Not enough events for clustering")
            return []
            
        # Stage 1: Spatial clustering with DBSCAN
        coords = np.array([[e['lat'], e['lon']] for e in events])
        
        dbscan = DBSCAN(
            eps=DBSCAN_EPS, 
            min_samples=DBSCAN_MIN_SAMPLES,
            metric='euclidean'
        )
        labels = dbscan.fit_predict(coords)
        
        # Group events by spatial cluster
        spatial_clusters = {}
        for idx, label in enumerate(labels):
            if label == -1:  # Noise points
                continue
            if label not in spatial_clusters:
                spatial_clusters[label] = []
            spatial_clusters[label].append(events[idx])
            
        print(f"[CLUSTER] DBSCAN found {len(spatial_clusters)} spatial clusters")
        
        # Stage 2: Semantic refinement
        final_clusters = []
        
        for cluster_id, cluster_events in spatial_clusters.items():
            # Check temporal coherence (72h window)
            dates = [e['date'] for e in cluster_events]
            time_spread = (max(dates) - min(dates)).total_seconds() / 3600
            
            if time_spread > TEMPORAL_WINDOW_HOURS:
                # Split by time windows
                cluster_events.sort(key=lambda x: x['date'])
                # Simple split: just take events within temporal window of first event
                filtered_events = []
                anchor_date = cluster_events[0]['date']
                for e in cluster_events:
                    if (e['date'] - anchor_date).total_seconds() / 3600 <= TEMPORAL_WINDOW_HOURS:
                        filtered_events.append(e)
                cluster_events = filtered_events
                
            if len(cluster_events) < DBSCAN_MIN_SAMPLES:
                continue
                
            # Calculate mean pairwise semantic similarity (only if embeddings exist)
            embeddings = [e['embedding'] for e in cluster_events if e['embedding'] is not None]
            
            # If no embeddings, skip semantic refinement and accept spatial cluster as-is
            if len(embeddings) < 2:
                final_clusters.append(cluster_events)
                print(f"   Cluster {cluster_id}: {len(cluster_events)} events (spatial-only, no embeddings)")
                continue
                
            similarities = []
            for i in range(len(embeddings)):
                for j in range(i + 1, len(embeddings)):
                    sim = self._compute_cosine_similarity(embeddings[i], embeddings[j])
                    similarities.append(sim)
                    
            mean_similarity = np.mean(similarities) if similarities else 0
            
            # If semantically coherent, keep as single cluster
            if mean_similarity >= SEMANTIC_THRESHOLD:
                final_clusters.append(cluster_events)
                print(f"   Cluster {cluster_id}: {len(cluster_events)} events (sem={mean_similarity:.2f})")
            else:
                # Split into sub-clusters based on semantic similarity
                # Simple approach: keep only events similar to cluster centroid
                centroid_embedding = np.mean(embeddings, axis=0)
                coherent_events = [
                    e for e in cluster_events 
                    if e['embedding'] is not None and self._compute_cosine_similarity(e['embedding'], centroid_embedding) >= SEMANTIC_THRESHOLD
                ]
                
                # If no coherent events after filtering, keep original cluster (spatial-only)
                if len(coherent_events) >= DBSCAN_MIN_SAMPLES:
                    final_clusters.append(coherent_events)
                    print(f"   Cluster {cluster_id}: Split -> {len(coherent_events)} coherent events")
                else:
                    final_clusters.append(cluster_events)
                    print(f"   Cluster {cluster_id}: {len(cluster_events)} events (sem={mean_similarity:.2f}, kept despite low coherence)")
                    
        print(f"[CLUSTER] Final: {len(final_clusters)} narrative clusters")
        return final_clusters
        
    def _generate_geometry(self, cluster_events: List[Dict]) -> Optional[Dict]:
        """
        Generate buffered convex hull polygon for cluster visualization.
        Returns GeoJSON-compatible geometry.
        """
        points = [(e['lon'], e['lat']) for e in cluster_events]
        
        if len(points) < 3:
            # Create circle buffer around centroid for 2 points
            centroid = MultiPoint(points).centroid
            buffered = centroid.buffer(BUFFER_DEGREES)
        else:
            try:
                # Create convex hull
                multi_point = MultiPoint(points)
                hull = multi_point.convex_hull
                
                # Apply buffer to ensure visibility
                buffered = hull.buffer(BUFFER_DEGREES)
            except Exception as e:
                print(f"[WARN] Geometry error: {e}")
                return None
                
        # Convert to GeoJSON coordinates
        if buffered.is_empty:
            return None
            
        if isinstance(buffered, Polygon):
            coords = [list(buffered.exterior.coords)]
        else:
            # Handle MultiPolygon (shouldn't happen often)
            coords = [list(buffered.geoms[0].exterior.coords)]
            
        return {
            "type": "Polygon",
            "coordinates": [[[round(c[0], 5), round(c[1], 5)] for c in coords[0]]]
        }
        
    def _generate_narrative(self, cluster_events: List[Dict]) -> Dict:
        """
        Use LLM to generate strategic narrative from cluster events.
        Returns structured metadata for the cluster.
        """
        # Aggregate text dossiers
        combined_text = "\n---\n".join([
            f"[{e['date_str']}] {e['text'][:500]}" 
            for e in cluster_events[:10]  # Limit to prevent token overflow
        ])
        
        # Calculate cluster metrics
        avg_intensity = np.mean([e['intensity_score'] for e in cluster_events])
        classifications = [e['classification'] for e in cluster_events]
        primary_tactic = max(set(classifications), key=classifications.count)
        
        dates = [e['date_str'] for e in cluster_events]
        date_range = [min(dates), max(dates)]
        
        # Default narrative (fallback if LLM fails)
        default_narrative = {
            "title": f"Tactical Activity Cluster ({len(cluster_events)} events)",
            "summary": "Multiple correlated military events detected in this area.",
            "primary_tactic": primary_tactic,
            "strategic_context": "UNKNOWN"
        }
        
        if not self.llm_client:
            return {**default_narrative, "avg_intensity": round(avg_intensity, 1), "date_range": date_range}
            
        # LLM System Prompt (The Strategist)
        system_prompt = """You are a military intelligence analyst. Analyze these correlated military reports from the Russia-Ukraine conflict.

Output ONLY valid JSON with these fields:
{
  "title": "Name of the operation/front (e.g., 'Winter Offensive in Kupyansk')",
  "summary": "A 2-sentence tactical summary: Strategic Intent + Outcome",
  "primary_tactic": "ATTRITION | MANOEUVRE | SHAPING | LOGISTICS | INCOHERENT_DISARRAY",
  "strategic_context": "OFFENSIVE_PUSH | DEFENSIVE_HOLD | PARTISAN_ACTIVITY | SHAPING_OPERATION"
}

Rules:
- Title should be evocative and location-specific
- Summary should synthesize, not list individual events
- Base tactic classification on the dominant pattern
- Be concise and professional"""

        try:
            response = self.llm_client.chat.completions.create(
                model="deepseek/deepseek-v3.2",  # The Strategist uses DeepSeek V3.2
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Analyze these {len(cluster_events)} correlated reports:\n\n{combined_text}"}
                ],
                temperature=0.1,
                max_tokens=300,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            return {
                "title": result.get("title", default_narrative["title"]),
                "summary": result.get("summary", default_narrative["summary"]),
                "primary_tactic": result.get("primary_tactic", primary_tactic),
                "strategic_context": result.get("strategic_context", "UNKNOWN"),
                "avg_intensity": round(avg_intensity, 1),
                "date_range": date_range
            }
            
        except Exception as e:
            print(f"[WARN] LLM narrative generation failed: {e}")
            return {**default_narrative, "avg_intensity": round(avg_intensity, 1), "date_range": date_range}
            
    def _generate_cluster_id(self, cluster_events: List[Dict]) -> str:
        """Generate unique cluster identifier."""
        event_ids = "_".join(sorted([e['event_id'][:8] for e in cluster_events[:3]]))
        date_prefix = datetime.now().strftime("%Y%m%d")
        hash_suffix = hashlib.md5(event_ids.encode()).hexdigest()[:4]
        return f"nar_{date_prefix}_{hash_suffix}"
        
    def run(self) -> Dict:
        """
        Main orchestrator: Load events, cluster, generate narratives, save output.
        """
        print("\n" + "="*60)
        print("🚀 NARRATIVE ENGINE - Starting Analysis")
        print("="*60)
        
        # Step 1: Load events
        events = self._load_events()
        
        if not events:
            print("[WARN] No events found. Creating empty output.")
            output = {"generated_at": datetime.now().isoformat(), "narratives": []}
            self._save_output(output)
            return output
            
        # Step 2: Cluster events
        clusters = self._cluster_events(events)
        
        if not clusters:
            print("[WARN] No clusters formed. Creating empty output.")
            output = {"generated_at": datetime.now().isoformat(), "narratives": []}
            self._save_output(output)
            return output
            
        # Step 3: Generate narratives for each cluster
        narratives = []
        
        for idx, cluster_events in enumerate(clusters):
            print(f"\n[NARRATIVE {idx+1}/{len(clusters)}] Processing {len(cluster_events)} events...")
            
            # Calculate centroid
            centroid = [
                round(np.mean([e['lat'] for e in cluster_events]), 4),
                round(np.mean([e['lon'] for e in cluster_events]), 4)
            ]
            
            # Generate geometry
            geometry = self._generate_geometry(cluster_events)
            if not geometry:
                print(f"   [SKIP] Could not generate geometry")
                continue
                
            # Generate AI narrative
            narrative_meta = self._generate_narrative(cluster_events)
            
            # Get tactic color
            tactic = narrative_meta.get('primary_tactic', 'UNKNOWN')
            tactic_color = TACTIC_COLORS.get(tactic, TACTIC_COLORS['UNKNOWN'])
            
            narrative = {
                "cluster_id": self._generate_cluster_id(cluster_events),
                "centroid": centroid,
                "geometry": geometry,
                "meta": {
                    "title": narrative_meta["title"],
                    "summary": narrative_meta["summary"],
                    "primary_tactic": tactic,
                    "tactic_color": tactic_color,
                    "intensity": narrative_meta["avg_intensity"],
                    "event_count": len(cluster_events),
                    "date_range": narrative_meta["date_range"],
                    "strategic_context": narrative_meta.get("strategic_context", "UNKNOWN")
                },
                "event_ids": [e['event_id'] for e in cluster_events]
            }
            
            narratives.append(narrative)
            print(f"   ✅ '{narrative_meta['title']}' ({tactic})")
            
        # Step 4: Save output
        output = {
            "generated_at": datetime.now().isoformat(),
            "narratives": narratives
        }
        
        self._save_output(output)
        
        print("\n" + "="*60)
        print(f"✅ COMPLETE: Generated {len(narratives)} strategic narratives")
        print("="*60)
        
        return output
        
    def _save_output(self, output: Dict):
        """Save narratives to JSON file."""
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
            
        print(f"[SAVE] Output written to {OUTPUT_PATH}")
        
    def close(self):
        """Clean up resources."""
        if self.conn:
            self.conn.close()


def main():
    """Entry point for narrative generation."""
    engine = NarrativeEngine()
    try:
        engine.run()
    finally:
        engine.close()


if __name__ == "__main__":
    main()
