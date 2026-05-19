import sqlite3
import os

dbs = [
    'war_tracker_v2/data/raw_events.db',
    'osint_tracker.db',
    'impact_atlas.db'
]

mojibake_indicators = ['Ã¨', 'Ã ', 'Ã²', 'Ã¹', 'Ã¬', 'Ã©', 'Ã‚']

print("=== SCANNING DATABASES FOR MOJIBAKE (SAMPLING LARGE TABLES) ===")
for db_path in dbs:
    if not os.path.exists(db_path):
        print(f"Skipping {db_path} (does not exist)")
        continue
        
    print(f"\nChecking database: {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            # Get text columns
            cursor.execute(f"PRAGMA table_info({table});")
            columns = [col[1] for col in cursor.fetchall() if 'TEXT' in col[2].upper() or 'CHAR' in col[2].upper()]
            
            if not columns:
                continue
                
            # Count total rows to decide if we sample
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()[0]
            
            if total_rows > 5000:
                print(f"  Table '{table}' has {total_rows} rows. Sampling last 5000 rows...")
                # We'll fetch the last 5000 rows and scan them in Python
                # If there's an autoincrement ID or rowid, we can order by rowid desc
                try:
                    cursor.execute(f"SELECT {', '.join(columns)} FROM {table} ORDER BY rowid DESC LIMIT 5000")
                    rows = cursor.fetchall()
                except Exception:
                    cursor.execute(f"SELECT {', '.join(columns)} FROM {table} LIMIT 5000")
                    rows = cursor.fetchall()
            else:
                cursor.execute(f"SELECT {', '.join(columns)} FROM {table}")
                rows = cursor.fetchall()
                
            # Scan the rows in Python
            mojibake_counts = {col: {ind: 0 for ind in mojibake_indicators} for col in columns}
            for row in rows:
                for col_idx, col_name in enumerate(columns):
                    val = row[col_idx]
                    if val and isinstance(val, str):
                        for ind in mojibake_indicators:
                            if ind in val:
                                mojibake_counts[col_name][ind] += 1
                                
            for col_name in columns:
                col_indicators = [ind for ind, count in mojibake_counts[col_name].items() if count > 0]
                if col_indicators:
                    print(f"  [!] Table '{table}', Column '{col_name}': found indicators {col_indicators} in the sample")
                    
        conn.close()
    except Exception as e:
        print(f"Error checking {db_path}: {e}")

print("\n=== SCANNING DATA FILES FOR MOJIBAKE ===")
data_dir = 'assets/data'
if os.path.exists(data_dir):
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(('.json', '.geojson', '.csv')):
                if file == 'events_timeline.json':
                    continue
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    found = []
                    for ind in mojibake_indicators:
                        if ind in content:
                            found.append(ind)
                    if found:
                        print(f"  [!] File '{file_path}': found mojibake indicators {found}")
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
