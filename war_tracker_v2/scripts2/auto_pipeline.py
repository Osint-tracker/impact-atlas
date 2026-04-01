import subprocess
import os
import sys
import logging
from datetime import datetime

import io
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Aggressive path resolution for Impact Atlas
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, "pipeline_auto.log")

# Setup logging with military precision
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Force UTF-8 for subprocesses on Windows
env = os.environ.copy()
env["PYTHONIOENCODING"] = "utf-8"

SCRIPTS = [
    "run_daily.py",
    "refiner_fast.py",
    "event_builder.py"
]

def run_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    logging.info(f"🚀 [PIPELINE] EXE: {script_name}")
    
    try:
        # We use the same interpreter that is running THIS script
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            check=True
        )
        logging.info(f"✅ [PIPELINE] OK: {script_name}")
        # Log a snippet of the output if necessary
        # logging.info(f"STDOUT: {result.stdout[-500:]}") 
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ [PIPELINE] CRITICAL_FAILURE: {script_name} (Code {e.returncode})")
        logging.error(f"STDERR: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"❌ [PIPELINE] UNEXPECTED_ERROR: {str(e)}")
        return False
    return True

def main():
    logging.info("="*60)
    logging.info("IMPACT ATLAS - OPERATIONAL PIPELINE CYCLE START")
    logging.info("="*60)
    
    success = True
    for script in SCRIPTS:
        if not run_script(script):
            logging.error("⛔ [PIPELINE] CYCLE_ABORTED - Sequence broken.")
            success = False
            break
            
    if success:
        logging.info("="*60)
        logging.info("IMPACT ATLAS - OPERATIONAL PIPELINE CYCLE COMPLETE")
        logging.info("="*60)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
