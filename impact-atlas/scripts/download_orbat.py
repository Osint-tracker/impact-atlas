import os
import requests
import json
import csv
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DATA_DIR = os.path.join(BASE_DIR, '../assets/data')
os.makedirs(ASSETS_DATA_DIR, exist_ok=True)

UA_CSV_PATH = os.path.join(ASSETS_DATA_DIR, 'orbat_ua.csv')
RU_JSON_PATH = os.path.join(ASSETS_DATA_DIR, 'orbat_ru.json')

# URL for Ukraine Data (GitHub uawardata)
UA_DATA_URL = "https://raw.githubusercontent.com/uawardata/uawardata/master/units_current.csv"

def download_ua_units():
    print(f"[*] Downloading Ukraine Units from {UA_DATA_URL}...")
    try:
        response = requests.get(UA_DATA_URL)
        response.raise_for_status()
        
        # Save raw CSV
        with open(UA_CSV_PATH, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"   [OK] Saved to {UA_CSV_PATH}")
        return True
    except Exception as e:
        print(f"   [WARN] Failed to download UA data: {e}")
        print("   [*] Switching to Hardcoded Fallback for UA Units...")
        return generate_ua_units_fallback()

def generate_ua_units_fallback():
    # Hardcoded list of MAJOR Ukrainian Brigades (active 2024-2025)
    # This ensures the tracker works even if the external CSV is offline.
    # Format matches uawardata conventions approx.
    ua_data = [
        "unit_id,name,type,subordination",
        "UA_47_MECH_BDE,47th Separate Mechanized Brigade,MECH_INF,Operational Command North",
        "UA_3_ASSAULT_BDE,3rd Separate Assault Brigade,INFANTRY,Ground Forces",
        "UA_82_AIR_ASSAULT_BDE,82nd Air Assault Brigade,AIRBORNE,Air Assault Forces",
        "UA_80_AIR_ASSAULT_BDE,80th Air Assault Brigade,AIRBORNE,Air Assault Forces",
        "UA_93_MECH_BDE,93rd Separate Mechanized Brigade (Kholodnyi Yar),MECH_INF,Operational Command East",
        "UA_72_MECH_BDE,72nd Separate Mechanized Brigade (Black Zaporozhians),MECH_INF,Operational Command North",
        "UA_110_MECH_BDE,110th Separate Mechanized Brigade,MECH_INF,Operational Command North",
        "UA_25_AIRBORNE_BDE,25th Airborne Brigade,AIRBORNE,Air Assault Forces",
        "UA_128_MTN_BDE,128th Mountain Assault Brigade,INFANTRY,Operational Command West",
        "UA_10_MTN_BDE,10th Mountain Assault Brigade (Edelweiss),INFANTRY,Operational Command West",
        "UA_59_MOT_BDE,59th Separate Motorized Infantry Brigade,INFANTRY,Operational Command South",
        "UA_1_TANK_BDE,1st Separate Tank Brigade,ARMORED,Operational Command North",
        "UA_4_TANK_BDE,4th Separate Tank Brigade,ARMORED,Reserve Corps",
        "UA_35_MARINE_BDE,35th Separate Marine Brigade,NAVAL_INFANTRY,Marine Corps",
        "UA_36_MARINE_BDE,36th Separate Marine Brigade,NAVAL_INFANTRY,Marine Corps",
        "UA_37_MARINE_BDE,37th Separate Marine Brigade,NAVAL_INFANTRY,Marine Corps",
        "UA_38_MARINE_BDE,38th Separate Marine Brigade,NAVAL_INFANTRY,Marine Corps",
        "UA_KRAKEN,Kraken Regiment,SPECIAL_FORCES,GUR (Intelligence)",
        "UA_OMEGA,Omega Special Purpose Unit,SPECIAL_FORCES,National Guard",
        "UA_ALPHA,Alpha Group (SBU),SPECIAL_FORCES,SBU",
        "UA_12_AZOV_BDE,12th Special Purpose Brigade Azov,SPECIAL_FORCES,National Guard",
        "UA_24_MECH_BDE,24th Separate Mechanized Brigade,MECH_INF,Operational Command West",
        "UA_28_MECH_BDE,28th Separate Mechanized Brigade,MECH_INF,Operational Command South",
        "UA_53_MECH_BDE,53rd Separate Mechanized Brigade,MECH_INF,Operational Command East",
        "UA_54_MECH_BDE,54th Separate Mechanized Brigade,MECH_INF,Operational Command East",
        "UA_92_ASSAULT_BDE,92nd Separate Assault Brigade,INFANTRY,Operational Command East",
        "UA_79_AIR_ASSAULT_BDE,79th Air Assault Brigade,AIRBORNE,Air Assault Forces",
        "UA_95_AIR_ASSAULT_BDE,95th Air Assault Brigade,AIRBORNE,Air Assault Forces",
        "UA_21_MECH_BDE,21st Separate Mechanized Brigade,MECH_INF,Ground Forces",
        "UA_22_MECH_BDE,22nd Separate Mechanized Brigade,MECH_INF,Ground Forces",
        "UA_32_MECH_BDE,32nd Separate Mechanized Brigade,MECH_INF,Ground Forces",
        "UA_33_MECH_BDE,33rd Separate Mechanized Brigade,MECH_INF,Ground Forces",
        "UA_118_MECH_BDE,118th Separate Mechanized Brigade,MECH_INF,Ground Forces",
        "UA_151_MECH_BDE,151st Separate Mechanized Brigade,MECH_INF,Ground Forces"
    ]
    
    try:
        # Create CSV content
        csv_content = "\n".join(ua_data)
        with open(UA_CSV_PATH, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        print(f"   [OK] Generated {len(ua_data)-1} UA units to {UA_CSV_PATH}")
        return True
    except Exception as e:
        print(f"   [ERR] Failed to save UA Fallback data: {e}")
        return False


def generate_ru_units():
    print(f"[*] Generating Russian Units Dataset (Hardcoded High-Value Target List)...")
    
    # Comprehensive List of Major Russian Formations in Ukraine (2024-2025)
    # Grouped by Military District / Army for structure, flattened for JSON
    ru_units = [
        # --- CENTRAL MILITARY DISTRICT (CMD) ---
        {"unit_id": "RU_2_CAA", "name": "2nd Guards Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_15_MRB", "name": "15th Separate Motorized Rifle Brigade", "type": "INFANTRY"}, # Peacekeepers
        {"unit_id": "RU_21_MRB", "name": "21st Separate Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_30_MRB", "name": "30th Separate Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_41_CAA", "name": "41st Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_90_TD", "name": "90th Guards Tank Division", "type": "ARMORED"},
        {"unit_id": "RU_55_MRB", "name": "55th Mountain Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_74_MRB", "name": "74th Guards Motorized Rifle Brigade", "type": "INFANTRY"},

        # --- SOUTHERN MILITARY DISTRICT (SMD) ---
        {"unit_id": "RU_8_CAA", "name": "8th Guards Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_150_MRD", "name": "150th Motorized Rifle Division", "type": "INFANTRY"},
        {"unit_id": "RU_20_MRD", "name": "20th Guards Motorized Rifle Division", "type": "INFANTRY"},
        {"unit_id": "RU_58_CAA", "name": "58th Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_42_MRD", "name": "42nd Guards Motorized Rifle Division", "type": "INFANTRY"},
        {"unit_id": "RU_19_MRD", "name": "19th Motorized Rifle Division", "type": "INFANTRY"},
        {"unit_id": "RU_136_MRB", "name": "136th Guards Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_49_CAA", "name": "49th Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_34_MRB", "name": "34th Mountain Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_205_MRB", "name": "205th Separate Motorized Rifle Brigade", "type": "INFANTRY"},

        # --- WESTERN MILITARY DISTRICT (WMD/Moscow MD) ---
        {"unit_id": "RU_1_GTA", "name": "1st Guards Tank Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_4_GTD", "name": "4th Guards Tank Division (Kantemirovskaya)", "type": "ARMORED"},
        {"unit_id": "RU_2_GMRD", "name": "2nd Guards Motorized Rifle Division (Tamanskaya)", "type": "INFANTRY"},
        {"unit_id": "RU_27_MRB", "name": "27th Sevastopol Guards Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_20_CAA", "name": "20th Guards Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_3_MRD", "name": "3rd Motorized Rifle Division", "type": "INFANTRY"},
        {"unit_id": "RU_144_MRD", "name": "144th Guards Motorized Rifle Division", "type": "INFANTRY"},

        # --- EASTERN MILITARY DISTRICT (EMD) ---
        {"unit_id": "RU_5_CAA", "name": "5th Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_127_MRD", "name": "127th Motorized Rifle Division", "type": "INFANTRY"},
        {"unit_id": "RU_57_MRB", "name": "57th Separate Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_35_CAA", "name": "35th Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_38_MRB", "name": "38th Guards Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_64_MRB", "name": "64th Guards Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_36_CAA", "name": "36th Combined Arms Army", "type": "HQ_ARMY"},
        {"unit_id": "RU_37_MRB", "name": "37th Guards Motorized Rifle Brigade", "type": "INFANTRY"},
        {"unit_id": "RU_29_CAA", "name": "29th Combined Arms Army", "type": "HQ_ARMY"},

        # --- AIRBORNE FORCES (VDV) ---
        {"unit_id": "RU_76_GAAD", "name": "76th Guards Air Assault Division", "type": "AIRBORNE"},
        {"unit_id": "RU_104_GARD", "name": "104th Guards Air Assault Regiment", "type": "AIRBORNE"},
        {"unit_id": "RU_234_GARD", "name": "234th Guards Air Assault Regiment", "type": "AIRBORNE"},
        {"unit_id": "RU_98_GAD", "name": "98th Guards Airborne Division", "type": "AIRBORNE"},
        {"unit_id": "RU_106_GAD", "name": "106th Guards Airborne Division", "type": "AIRBORNE"},
        {"unit_id": "RU_7_GAAD", "name": "7th Guards Mountain Air Assault Division", "type": "AIRBORNE"},
        {"unit_id": "RU_108_GAAR", "name": "108th Guards Air Assault Regiment", "type": "AIRBORNE"},
        {"unit_id": "RU_11_GAB", "name": "11th Guards Air Assault Brigade", "type": "AIRBORNE"},
        {"unit_id": "RU_31_GAB", "name": "31st Guards Air Assault Brigade", "type": "AIRBORNE"},
        {"unit_id": "RU_83_GAB", "name": "83rd Guards Air Assault Brigade", "type": "AIRBORNE"},
        {"unit_id": "RU_45_SPETSNAZ", "name": "45th Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},

        # --- NAVAL INFANTRY ---
        {"unit_id": "RU_810_NIB", "name": "810th Guards Naval Infantry Brigade", "type": "NAVAL_INFANTRY"}, # Black Sea
        {"unit_id": "RU_155_NIB", "name": "155th Guards Naval Infantry Brigade", "type": "NAVAL_INFANTRY"}, # Pacific
        {"unit_id": "RU_40_NIB", "name": "40th Naval Infantry Brigade", "type": "NAVAL_INFANTRY"}, # Pacific
        {"unit_id": "RU_336_NIB", "name": "336th Guards Naval Infantry Brigade", "type": "NAVAL_INFANTRY"}, # Baltic
        {"unit_id": "RU_61_NIB", "name": "61st Naval Infantry Brigade", "type": "NAVAL_INFANTRY"}, # Northern

        # --- SPECIAL FORCES (GRU/SSO) ---
        {"unit_id": "RU_2_SPETSNAZ", "name": "2nd Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},
        {"unit_id": "RU_3_SPETSNAZ", "name": "3rd Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},
        {"unit_id": "RU_10_SPETSNAZ", "name": "10th Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},
        {"unit_id": "RU_14_SPETSNAZ", "name": "14th Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},
        {"unit_id": "RU_16_SPETSNAZ", "name": "16th Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},
        {"unit_id": "RU_22_SPETSNAZ", "name": "22nd Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},
        {"unit_id": "RU_24_SPETSNAZ", "name": "24th Guards Spetsnaz Brigade", "type": "SPECIAL_FORCES"},

        # --- ARTILLERY & MISSILE ---
        {"unit_id": "RU_232_RAB", "name": "232nd Rocket Artillery Brigade", "type": "ARTILLERY"},
        {"unit_id": "RU_9_AB", "name": "9th Guards Artillery Brigade", "type": "ARTILLERY"},
        {"unit_id": "RU_448_MB", "name": "448th Missile Brigade (Iskander)", "type": "MISSILE"},
    ]

    try:
        with open(RU_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(ru_units, f, indent=2, ensure_ascii=False)
        print(f"   [OK] Generated {len(ru_units)} Russian units to {RU_JSON_PATH}")
        return True
    except Exception as e:
        print(f"   [ERR] Failed to save RU data: {e}")
        return False

def main():
    print("=== ORBAT DOWNLOADER ===")
    ua_ok = download_ua_units()
    ru_ok = generate_ru_units()
    
    if ua_ok and ru_ok:
        print("=== SUCCESS: All data ready for seeding ===")
    else:
        print("=== WARNING: Some downloads failed ===")

if __name__ == "__main__":
    main()
