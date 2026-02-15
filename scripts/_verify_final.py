"""Final verification of Oryx data quality after fix."""
import json, sys
sys.stdout.reconfigure(encoding='utf-8')

with open('assets/data/external_losses.json', 'r', encoding='utf-8') as f:
    losses = json.load(f)

oryx = [x for x in losses if x.get('source_tag') == 'Oryx']
la = [x for x in losses if x.get('source_tag') == 'LostArmour']

print(f"Total: {len(losses)} | Oryx: {len(oryx)} | LostArmour: {len(la)}")
print()

# Category distribution
cats = {}
for x in oryx:
    cat = x.get('type', '?')
    cats[cat] = cats.get(cat, 0) + 1

print("=== CATEGORY BREAKDOWN ===")
for cat, count in sorted(cats.items(), key=lambda i: -i[1])[:15]:
    print(f"  {count:4d}  {cat[:60]}")

# Status distribution
stats = {}
for x in oryx:
    s = x.get('status', '?')
    stats[s] = stats.get(s, 0) + 1
print()
print("=== STATUS BREAKDOWN ===")
for s, count in sorted(stats.items(), key=lambda i: -i[1]):
    print(f"  {count:4d}  {s}")

# Sample entries
print()
print("=== SAMPLE ENTRIES ===")
for i, entry in enumerate(oryx[:5]):
    print(f"  [{i+1}] model={entry.get('model')!r:.50s}  type={entry.get('type')!r:.40s}  status={entry.get('status')}")

# Check units enrichment
print()
print("=== UNITS WITH CASUALTIES ===")
with open('assets/data/units.json', 'r', encoding='utf-8') as f:
    units = json.load(f)
enriched = [u for u in units if u.get('casualty_count', 0) > 0]
print(f"  {len(enriched)} units enriched out of {len(units)} total")
for u in enriched[:3]:
    cas = u.get('verified_casualties', [])
    print(f"  -> {u.get('display_name')} ({u.get('faction')}): {u.get('casualty_count')} casualties")
    if cas:
        print(f"     First: {cas[0].get('rank')} {cas[0].get('name')}")

print()
print("DONE.")
