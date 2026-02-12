"""
debug_instrument.py - Parser Crash Recorder for OSINT Tracker
===============================================================
The "Sanfilippo Method" Instrument (Part 3): Code-Level Observability.

This module implements the "Flight Data Recorder" pattern for JSON parsing.
When a parser crash occurs, it captures forensic data including:
- Exact timestamp
- Full raw input that caused the crash
- Error position and surrounding context
- Partial data extracted before failure

Log Format: JSONL (one JSON object per line) for easy grep/tail operations.
"""

import json
import os
import sys
from datetime import datetime
from typing import Optional


class CrashRecorder:
    """
    Forensic logger for parser crashes - 'Flight Data Recorder' pattern.
    
    This class is responsible for capturing detailed crash dumps when
    JSON parsing fails. The dumps are designed to be consumed by AI agents
    for self-diagnosis and bug fixing.
    
    Usage:
        CrashRecorder.dump_state(
            context_name="_clean_and_parse_json",
            raw_input=response_text,
            error=json_decode_error,
            partial_data=None
        )
    """
    
    # Log file path (relative to scripts directory)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LOG_DIR = os.path.join(BASE_DIR, '..', 'logs')
    LOG_PATH = os.path.join(LOG_DIR, 'parser_crashes.log')
    
    # Context window for error snippets (characters before/after error position)
    CONTEXT_WINDOW = 50
    
    # Max raw input size to store (prevent huge logs)
    MAX_RAW_INPUT_SIZE = 50000
    
    @classmethod
    def _ensure_log_dir(cls) -> None:
        """Ensure the logs directory exists."""
        if not os.path.exists(cls.LOG_DIR):
            os.makedirs(cls.LOG_DIR, exist_ok=True)
    
    @classmethod
    def _extract_token_context(cls, raw_input: str, error: Exception) -> str:
        """
        Extract the context around the error position.
        
        For JSONDecodeError, uses the 'pos' attribute.
        For other errors, returns the first 100 characters.
        """
        try:
            # JSONDecodeError has 'pos' attribute
            if hasattr(error, 'pos') and error.pos is not None:
                pos = error.pos
                start = max(0, pos - cls.CONTEXT_WINDOW)
                end = min(len(raw_input), pos + cls.CONTEXT_WINDOW)
                
                context = raw_input[start:end]
                # Mark the error position
                relative_pos = pos - start
                if relative_pos < len(context):
                    marked = context[:relative_pos] + "<<<HERE>>>" + context[relative_pos:]
                    return f"...{marked}..."
                return f"...{context}..."
            
            # Fallback for other errors: return first 100 chars
            return raw_input[:100] + "..." if len(raw_input) > 100 else raw_input
            
        except Exception:
            return "[Context extraction failed]"
    
    @classmethod
    def _get_error_position(cls, error: Exception) -> Optional[int]:
        """Extract error position from JSONDecodeError."""
        if hasattr(error, 'pos'):
            return error.pos
        return None
    
    @classmethod
    def dump_state(cls, 
                   context_name: str, 
                   raw_input: str, 
                   error: Exception, 
                   partial_data: Optional[dict] = None) -> None:
        """
        Dump the full crash state to the log file and stderr.
        
        This is the primary interface for crash recording. It writes a
        complete forensic record in JSONL format.
        
        Args:
            context_name: The function/method where the crash occurred
                         (e.g., "_clean_and_parse_json", "_repair_json_with_ai")
            raw_input: The exact string that caused the parsing failure
            error: The exception object (usually JSONDecodeError)
            partial_data: Any data successfully extracted before failure (optional)
        
        Note:
            Uses synchronous file I/O to ensure data is captured even on
            abrupt termination. Flushes immediately after write.
        """
        try:
            cls._ensure_log_dir()
            
            # Build the crash dump record
            timestamp = datetime.now().astimezone().isoformat()
            error_pos = cls._get_error_position(error)
            token_context = cls._extract_token_context(raw_input, error)
            
            # Truncate raw input if too large
            raw_to_store = raw_input[:cls.MAX_RAW_INPUT_SIZE]
            if len(raw_input) > cls.MAX_RAW_INPUT_SIZE:
                raw_to_store += f"\n... [TRUNCATED: {len(raw_input)} total chars]"
            
            crash_record = {
                "timestamp": timestamp,
                "context_name": context_name,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "error_position": error_pos,
                "token_context": token_context,
                "raw_input_length": len(raw_input),
                "raw_input": raw_to_store,
                "partial_data": partial_data
            }
            
            # Write to JSONL file (synchronous, with immediate flush)
            with open(cls.LOG_PATH, 'a', encoding='utf-8') as f:
                f.write(json.dumps(crash_record, ensure_ascii=False) + '\n')
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            
            # Print condensed error to stderr for immediate visibility
            cls._print_stderr_snippet(context_name, error, token_context, timestamp)
            
        except Exception as log_error:
            # Even if logging fails, print to stderr
            print(f"[CrashRecorder] CRITICAL: Failed to write crash log: {log_error}", 
                  file=sys.stderr)
    
    @classmethod
    def _print_stderr_snippet(cls, context_name: str, error: Exception, 
                               token_context: str, timestamp: str) -> None:
        """Print a condensed crash snippet to stderr."""
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"🔴 PARSER CRASH RECORDED [{timestamp}]", file=sys.stderr)
        print(f"   Context: {context_name}", file=sys.stderr)
        print(f"   Error: {type(error).__name__}: {str(error)[:100]}", file=sys.stderr)
        print(f"   Token Context: {token_context[:80]}...", file=sys.stderr)
        print(f"   📂 Full dump: {cls.LOG_PATH}", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)


# ==============================================================================
# 🔧 SELF-HEALING HOOK: FUTURE AGENT INSTRUCTION
# ==============================================================================
# When modifying this parser, ALWAYS read `logs/parser_crashes.log` first.
# Create a test case from the last 3 crash dumps to ensure your new regex
# fixes the specific edge cases that caused these failures.
#
# The dump contains:
#   1. The EXACT raw string that broke the parser
#   2. The character position where parsing failed
#   3. Contextual snippet around the failure point
#
# Use this data to write regression tests before adding new cleaning rules.
#
# Example analysis workflow:
#   1. tail -n 10 logs/parser_crashes.log | jq .
#   2. Extract the "raw_input" field from the last crash
#   3. Create a unit test that reproduces the failure
#   4. Fix the cleaning logic to handle this edge case
#   5. Verify the test passes before committing
# ==============================================================================


if __name__ == "__main__":
    # Test the crash recorder
    print("Testing CrashRecorder...")
    
    test_json = '{"event": "test", invalid syntax here'
    try:
        json.loads(test_json)
    except json.JSONDecodeError as e:
        CrashRecorder.dump_state(
            context_name="__main__.test",
            raw_input=test_json,
            error=e,
            partial_data={"test": True}
        )
    
    print(f"✅ Test crash logged to: {CrashRecorder.LOG_PATH}")
