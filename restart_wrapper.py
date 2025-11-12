#!/usr/bin/env python3
"""
Wrapper script that runs final.py and automatically restarts every 20 minutes.
This ensures Railway restarts the service even if final.py silently stops.
"""

import subprocess
import sys
import signal
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RESTART-WRAPPER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

RESTART_INTERVAL_SECONDS = 20 * 60  # 20 minutes
PROCESS = None

def signal_handler(sig, frame):
    """Handle termination signals gracefully."""
    logging.info("Received termination signal, shutting down...")
    if PROCESS and PROCESS.poll() is None:
        logging.info("Terminating final.py process...")
        PROCESS.terminate()
        try:
            PROCESS.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logging.warning("Process didn't terminate, killing...")
            PROCESS.kill()
    sys.exit(0)

def main():
    """Main wrapper function."""
    global PROCESS
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Get command to run (default to final.py with --db flag)
    cmd = sys.argv[1:] if len(sys.argv) > 1 else ["python", "final.py", "--db", "--batch-size", "200"]
    
    cycle = 1
    
    while True:
        start_time = time.time()
        logging.info("=" * 80)
        logging.info(f"Starting cycle #{cycle} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Command: {' '.join(cmd)}")
        logging.info(f"Will restart after {RESTART_INTERVAL_SECONDS / 60:.0f} minutes")
        logging.info("=" * 80)
        
        try:
            # Start the process
            PROCESS = subprocess.Popen(
                cmd,
                stdout=sys.stdout,
                stderr=sys.stderr,
                bufsize=1,
                universal_newlines=True
            )
            
            # Monitor the process
            elapsed = 0
            while elapsed < RESTART_INTERVAL_SECONDS:
                # Check if process is still running
                if PROCESS.poll() is not None:
                    # Process exited before timeout
                    exit_code = PROCESS.returncode
                    logging.warning(f"Process exited early with code {exit_code} after {elapsed:.0f} seconds")
                    logging.info("Restarting immediately...")
                    break
                
                # Sleep in small increments to be responsive
                time.sleep(5)
                elapsed = time.time() - start_time
                
                # Log progress every 5 minutes
                if int(elapsed) % 300 == 0 and elapsed > 0:
                    remaining = RESTART_INTERVAL_SECONDS - elapsed
                    logging.info(f"Running... {elapsed/60:.1f} min elapsed, {remaining/60:.1f} min until restart")
            
            # If we reached the timeout, terminate the process
            if PROCESS.poll() is None:
                elapsed = time.time() - start_time
                logging.info(f"Reached {RESTART_INTERVAL_SECONDS / 60:.0f} minute timeout (elapsed: {elapsed/60:.1f} min)")
                logging.info("Terminating process for scheduled restart...")
                
                # Try graceful termination first
                PROCESS.terminate()
                try:
                    PROCESS.wait(timeout=30)
                    logging.info("Process terminated gracefully")
                except subprocess.TimeoutExpired:
                    logging.warning("Process didn't terminate gracefully, forcing kill...")
                    PROCESS.kill()
                    PROCESS.wait()
                    logging.info("Process killed")
            
            # Log cycle completion
            logging.info(f"Cycle #{cycle} completed. Restarting in 5 seconds...")
            time.sleep(5)
            cycle += 1
            
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down...")
            if PROCESS and PROCESS.poll() is None:
                PROCESS.terminate()
                PROCESS.wait()
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error in wrapper: {e}", exc_info=True)
            logging.info("Restarting in 10 seconds...")
            time.sleep(10)
            cycle += 1

if __name__ == "__main__":
    main()

