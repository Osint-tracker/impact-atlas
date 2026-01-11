import os
import json
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
TITAN_MODEL_ID = "ft:gpt-4o-mini-2024-07-18:personal:osint-analyst-v4-clean:Cv5yHxTJ"

# The Prompt with EXPLICIT DEFINITIONS (The candidate for the fix)
SYSTEM_PROMPT_WITH_DEFS = """You are a military intelligence analyst. Output strict JSON.

CRITICAL CLASSIFICATION RULES:
1. NOISE FILTER: If text is summary/political/static map -> NULL.
2. MANOUVRE: Territorial changes.
3. SHAPING: Deep strikes/logistics.
4. ATTRITION: Static fighting/shelling.

TASK 2: ESTIMATE METRICS (TITAN-10 PROTOCOL)
If classification is NOT NULL, you MUST estimate:
- kinetic_score (1-10): 1=Small Arms, 5=Tank/Grad, 7=Missile, 10=Nuke.
- target_score (1-10): 1=Field, 5=Tank, 8=AirDefense, 10=Command/Capital.
- effect_score (1-10): 1=Fail/Unknown, 5=Moderate Damage, 7=Destroyed.

OUTPUT FORMAT:
{
  "classification": "STRING",
  "kinetic_score": INTEGER,
  "target_score": INTEGER,
  "effect_score": INTEGER
}"""

TEST_CASES = [
    {
        "name": "HIGH INTENSITY (Missile on Airfield)",
        "text": "Russian forces launched an Iskander-M missile strike on the Myrhorod Airfield. Secondary detonations observed. Heavy damage.",
        "expected": "K~7, T~8, E~6+"
    },
    {
        "name": "LOW INTENSITY (Small Arms)",
        "text": "Small arms fire reported near the tree line east of Robotyne. No casualties confirmed.",
        "expected": "K~1, T~3, E~1"
    },
    {
        "name": "MEDIUM INTENSITY (Artillery)",
        "text": "Ukrainian artillery shelled Russian trench positions near Bakhmut with 155mm cluster munitions. suppression achieved.",
        "expected": "K~4-5, T~3, E~3"
    }
]

def run_tests():
    print(f"Testing Model: {TITAN_MODEL_ID}")
    print("-" * 60)
    
    client = OpenAI(api_key=API_KEY)

    for case in TEST_CASES:
        print(f"\nTEST CASE: {case['name']}")
        print(f"Input: {case['text']}")
        print(f"Expected: {case['expected']}")
        
        try:
            response = client.chat.completions.create(
                model=TITAN_MODEL_ID,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_WITH_DEFS},
                    {"role": "user", "content": case['text']}
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            data = json.loads(content)
            
            # Formatting output for readability
            k = data.get('kinetic_score', 'N/A')
            t = data.get('target_score', 'N/A')
            e = data.get('effect_score', 'N/A')
            cls = data.get('classification', 'N/A')
            
            print(f"RESULT: Class={cls} | K={k} | T={t} | E={e}")
            
        except Exception as e:
            print(f"ERROR: {e}")

if __name__ == "__main__":
    run_tests()
