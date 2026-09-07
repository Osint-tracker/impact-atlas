import re

with open('assets/js/map.js', 'r', encoding='utf-8') as f:
    content = f.read()

# We only want to remove \" inside the renderStrategicCampaignCards function string literals
# It's safer to just replace \" with " where it's part of an HTML attribute
# e.g., class=\"sc-card -> class="sc-card
pattern = re.compile(r'\\"(.*?)\\"')
content = pattern.sub(r'"\1"', content)

with open('assets/js/map.js', 'w', encoding='utf-8') as f:
    f.write(content)
