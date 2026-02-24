import json
data = json.load(open('assets/data/events.geojson', 'r', encoding='utf-8'))
events = data.get('features', [])
print(f'Total events: {len(events)}')

dates = set()
for e in events[:50]:
    dates.add(e['properties'].get('date', '?'))
print(f'Sample dates: {sorted(list(dates))[:15]}')

print('Timestamp samples:')
for e in events[:5]:
    p = e['properties']
    print(f"  date={p.get('date','?')} timestamp={p.get('timestamp','MISSING')}")

# Check timestamp spread
timestamps = [e['properties'].get('timestamp', 0) for e in events if e['properties'].get('timestamp')]
if timestamps:
    import datetime
    mn = min(timestamps)
    mx = max(timestamps)
    print(f'\nTimestamp range: {mn} - {mx}')
    print(f'  Min date: {datetime.datetime.fromtimestamp(mn/1000)}')
    print(f'  Max date: {datetime.datetime.fromtimestamp(mx/1000)}')
    spread_hours = (mx - mn) / 3600000
    print(f'  Spread: {spread_hours:.1f} hours ({spread_hours/24:.1f} days)')
    
    # How many within 24h, 48h, 72h of max?
    for h in [24, 48, 72]:
        cutoff = mx - h * 3600000
        count = sum(1 for t in timestamps if t >= cutoff)
        print(f'  Within {h}H of max: {count} events')
