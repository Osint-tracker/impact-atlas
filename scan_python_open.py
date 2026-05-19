import os
import re

print("=== SCANNING PYTHON FILES FOR open() WITHOUT UTF-8 ENCODING ===")
unsafe_open_pattern = re.compile(r'\bopen\s*\([^)]*\)')

for root, dirs, files in os.walk('.'):
    # Skip virtual environments and git
    if any(p in root for p in ['.venv', '.git', '__pycache__', '.vscode', '.gemini']):
        continue
        
    for file in files:
        if file.endswith('.py'):
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                for idx, line in enumerate(lines):
                    if 'open(' in line:
                        # Check if it specifies encoding
                        # For writing/reading text files, encoding should be utf-8
                        # If it contains 'wb' or 'rb' or 'ab' (binary modes), encoding is not required.
                        is_binary = any(m in line for m in ["'rb'", '"rb"', "'wb'", '"wb"', "'ab'", '"ab"'])
                        has_encoding = 'encoding=' in line
                        
                        if not is_binary and not has_encoding:
                            # Print warning
                            print(f"  [!] {file_path}:{idx+1} -> {line.strip()}")
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
