import sqlite3, json

conn = sqlite3.connect('war_tracker_v2/data/raw_events.db')

# Check what source data looks like for Telegram events
cur = conn.execute("SELECT sources_list, urls_list FROM unique_events WHERE sources_list LIKE '%t.me%' LIMIT 8")
for r in cur.fetchall():
    print("SRC:", (r[0] or '')[:200])
    print("URL:", (r[1] or '')[:200])
    print()

# Count how many have channel-only vs deep links
cur2 = conn.execute("SELECT sources_list FROM unique_events WHERE sources_list LIKE '%t.me%'")
deep_count = 0
channel_only = 0
for r in cur2.fetchall():
    src = r[0] or ''
    # Find all t.me URLs
    import re
    urls_found = re.findall(r'https?://t\.me/([^\s",\]]+)', src)
    for u in urls_found:
        parts = u.split('/')
        if len(parts) >= 2 and parts[1].isdigit():
            deep_count += 1
        else:
            channel_only += 1

print(f"\nDeep links (with message ID): {deep_count}")
print(f"Channel-only (no message ID): {channel_only}")
conn.close()
