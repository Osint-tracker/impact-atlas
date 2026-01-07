import json
import os

GEOJSON_PATH = 'assets/data/events.geojson'

def analyze():
    # Use absolute path to avoid ambiguity
    path = os.path.abspath(GEOJSON_PATH)
    if not os.path.exists(path):
        print(f"File not found at: {path}")
        return

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    features = data.get('features', [])
    print(f"Total Features: {len(features)}")

    empty_desc = 0
    empty_reasoning = 0
    zero_tie_total = 0
    missing_bias = 0
    empty_sources = 0
    bad_url_sources = 0

    for feat in features:
        props = feat.get('properties', {})
        
        # Description
        if not props.get('description'):
            empty_desc += 1
        
        # AI Reasoning
        reasoning = props.get('ai_reasoning', '')
        if not reasoning or len(reasoning) < 10:
            empty_reasoning += 1
            
        # TIE
        if props.get('tie_total') == 0:
            zero_tie_total += 1
            
        # Bias
        if props.get('bias_score') == 0:
             missing_bias += 1

        # Sources
        sources = props.get('sources_list')
        is_empty = False
        if not sources:
            is_empty = True
        else:
            try:
                if isinstance(sources, str):
                    src_list = json.loads(sources)
                else:
                    src_list = sources
                
                if not src_list or len(src_list) == 0:
                    is_empty = True
                else:
                    # Check URL quality
                    has_good_url = False
                    for s in src_list:
                        u = s.get('url')
                        if u and u != "[null]" and u != "None" and "http" in u:
                            has_good_url = True
                    if not has_good_url:
                        bad_url_sources += 1
            except:
                is_empty = True
        
        if is_empty:
            empty_sources += 1

    print(f"Empty Descriptions: {empty_desc}")
    print(f"Empty/Short AI Reasoning: {empty_reasoning}")
    print(f"Zero TIE Total: {zero_tie_total}")
    print(f"Bias Score = 0 (Total Neutral): {missing_bias}")
    print(f"Empty Sources List: {empty_sources}")
    print(f"Sources with only Bad/Missing URLs: {bad_url_sources}")

if __name__ == "__main__":
    analyze()
