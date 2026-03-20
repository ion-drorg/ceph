import rados
import json
import subprocess
import sys
import os
import argparse
import signal
import time
from datetime import datetime

# Helper to get the current timestamp string
def now():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]

# --- Global for Cleanup ---
primary_osd_id = None
config_path_global = './ceph.conf'

def run_cmd(cmd):
    """Executes a command and returns only stdout to avoid breaking JSON parsing."""
    try:
        # Separate stdout from stderr to filter out 'DEVELOPER MODE' warnings
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"\n[ERROR] Command failed: {' '.join(cmd)}")
            print(f"[DETAILS] {stderr.decode()}")
            sys.exit(1)
            
        return stdout.decode('utf-8')
    except FileNotFoundError:
        print(f"\n[ERROR] Binary not found: {cmd}")
        print("[ADVICE] Ensure you are running this from the 'build' directory.")
        sys.exit(1)
        
def set_hook(osd_id, enabled):
    """Sets the OSD hook using config set and verifies with retries."""
    val_str = "true" if enabled else "false"
    print(f"Requesting hook={val_str} for osd.{osd_id}...")
    
    # 1. Use 'config set' instead of 'injectargs' for better consistency on 'main'
    # This targets the specific OSD via the central config store
    cmd = ["./bin/ceph", "-c", config_path_global, "config", "set", f"osd.{osd_id}", 
           "osd_reproduce_bug_75403_hook", val_str]
    run_cmd(cmd)
    
    # 2. Verification Loop (Retry up to 5 times)
    verify_cmd = ["./bin/ceph", "-c", config_path_global, "config", "get", 
                  f"osd.{osd_id}", "osd_reproduce_bug_75403_hook"]
    
    for attempt in range(5):
        current_status = run_cmd(verify_cmd).strip().lower()
        if current_status == val_str:
            print(f"[CONFIRMED] Hook is now {val_str} on osd.{osd_id}")
            return
        print(f"  ...waiting for OSD to acknowledge (Attempt {attempt+1}/5)")
        time.sleep(1)
    
    # Final check failure
    print(f"\n[ERROR] Hook state mismatch! Expected {val_str}, still got {current_status}")
    if enabled:
        sys.exit(1)

def signal_handler(sig, frame):
    global primary_osd_id
    print("\n\n[INTERRUPT] Cleaning up...")
    if primary_osd_id is not None:
        try:
            set_hook(primary_osd_id, False)
        except Exception:
            pass
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# --- Argument Parsing ---
parser = argparse.ArgumentParser(
    description="Replicate Ceph bug 75403 by triggering a race condition in PrimaryLogPG.",
    formatter_class=argparse.RawTextHelpFormatter
)
parser.add_argument('-p', '--pool', default='rbd', help='Target RADOS pool (default: rbd)')
parser.add_argument('-c', '--config', default='./ceph.conf', help='Path to ceph.conf (default: ./ceph.conf)')
args = parser.parse_args()

config_path_global = args.config
POOL_NAME = args.pool

# --- Pre-flight Checks (Enforcing execution from build/) ---
if os.path.basename(os.getcwd()) != "build":
    print("\n[ERROR] This script must be run from the Ceph 'build' directory.")
    print("[ADVICE] cd build/")
    print(f"[ADVICE] python3 ../{os.path.basename(__file__)}")
    sys.exit(1)

if not os.path.exists(config_path_global):
    print(f"\n[ERROR] Ceph configuration file not found at: {config_path_global}")
    print("[ADVICE] Activate cluster with: env MON=1 MGR=1 OSD=3 ../src/vstart.sh -n -d --localhost")
    sys.exit(1)

# --- Connection ---
try:
    print(f"Connecting to Ceph cluster using {config_path_global}...")
    cluster = rados.Rados(conffile=config_path_global)
    cluster.connect()
    
    if not cluster.pool_exists(POOL_NAME):
        print(f"\n[ERROR] Pool '{POOL_NAME}' does not exist.")
        print("[ADVICE] Check existing pools with: ./bin/ceph osd lspools")
        print(f"[ADVICE] Use an existing pool: python3 {sys.argv} -p <pool_name>")
        print("\n[ADVICE] OR create and initialize a new Replicated pool (Standard):")
        print(f"         ./bin/ceph osd pool create rbd 8 8 [replicated]")
        print(f"         # Initialize OMAP structures (within the same pool):")
        print(f"         ./bin/rbd pool init rbd")
        print("\n[ADVICE] OR create and initialize a new EC Pool Pair (Metadata + Data):")
        print(f"         ./bin/ceph osd erasure-code-profile set myprofile k=2 m=1")
        print(f"         ./bin/ceph osd pool create ec-pool-data 8 8 erasure myprofile")
        print(f"         ./bin/ceph osd pool set ec-pool-data allow_ec_overwrites true")
        print(f"         # Create and initialize OMAP Metadata pool (Replicated):")
        print(f"         ./bin/ceph osd pool create ec-pool-metadata 8 8 [replicated]")
        print(f"         # Initialize OMAP structures (in the metadata pool):")
        print(f"         ./bin/rbd pool init ec-pool-metadata")
        print(f"         # Note: Linkage happens at image creation: ./bin/rbd create --size 1G --data-pool ec-pool-data ec-pool-metadata/img")
        sys.exit(1)
        
    ioctx = cluster.open_ioctx(POOL_NAME)
except rados.ObjectNotFound:
    print(f"\n[ERROR] Could not find the cluster or config at {config_path_global}")
    sys.exit(1)
except rados.Error as e:
    print(f"\n[ERROR] RADOS connection failed: {e}")
    sys.exit(1)

obj_name = "my_test_obj"
base_content = b"base value"
modified_content = b"modified value"

# --- Test Execution ---
print(f"Using pool: {POOL_NAME}")
print(f"Writing initial object: '{obj_name}' with value: '{base_content.decode()}' ({len(base_content)} chars)")
ioctx.write_full(obj_name, base_content)

# Get OSD and PG info
cmd = ["./bin/ceph", "-c", config_path_global, "osd", "map", POOL_NAME, obj_name, "--format=json"]
map_data = json.loads(run_cmd(cmd))
pg_id = map_data['pgid']
acting_set = map_data['acting']
primary_osd_id = acting_set[0] # Target the first OSD in the acting set
print(f"Acting Set: {acting_set} (Primary: osd.{primary_osd_id})")

# Verify Initial Write
read_val = ioctx.read(obj_name)
if read_val == base_content:
    print(f'Confirm: "{base_content.decode()}" written successfully to osd.{primary_osd_id} (PG {pg_id})')
else:
    print(f'\n[ERROR] Verification failed!')
    print(f'[DETAILS] Expected: "{base_content.decode()}", but read: "{read_val.decode() if read_val else "None"}"')
    sys.exit(1)

# Inject Hook
print(f"Injecting hook config for reproducing bug 75403 to osd.{primary_osd_id}...")
set_hook(primary_osd_id, True)

# Dispatch Ops - Exactly matching your rados.pyx signatures
print("Dispatching racing reads and write...")

# Suggest user check the specific OSD log
log_path = f"out/osd.{primary_osd_id}.log"
print(f'Check OSD log: grep -aE "do_op before hook check:|: do_op entry:|trigger_laggy|Hook for bug 75429|not readable|before out of order check:" {log_path} | less -S -N')

# --- Async Result Tracking ---
results = {}

def read_cb(completion, data_read):
    """Matches: oncomplete(completion, data_read) from your rados.pyx"""
    ts = time.time()
    ret = completion.get_return_value()
    # Store: (Timestamp, CompID, ReturnCode, Data)
    results[id(completion)] = (ts, id(completion), ret, data_read)

def write_cb(completion):
    """Matches: oncomplete(completion) for aio_write_full"""
    ts = time.time()
    ret = completion.get_return_value()
    # Store: (Timestamp, CompID, ReturnCode)
    results[id(completion)] = (ts, id(completion), ret)

print(f"[{now()}] Starting IO Sends (Primary: osd.{primary_osd_id}, Acting: {acting_set})")

comp1 = ioctx.aio_read(obj_name, 4096, 0, read_cb)
print(f"[{now()}] Read 1 sent.")
comp2 = ioctx.aio_read(obj_name, 4096, 0, read_cb)
print(f"[{now()}] Read 2 sent.")

# sleep so that write doesn't preceed the reads.
time.sleep(0.5) 

print(f"Writing object: '{obj_name}' with value: '{modified_content.decode()}' ({len(modified_content)} chars)")
comp_write = ioctx.aio_write_full(obj_name, modified_content, write_cb)
print(f"[{now()}] Write sent.")

# --- Wait and Print Results ---
print(f"\n[TEST STATE] Waiting for completions at {time.strftime('%H:%M:%S')}...")
print("[EXPECTED BEHAVIOR] Script should hang here if bug 75403 is successfully replicated.")

comp1.wait_for_complete()
comp2.wait_for_complete()
comp_write.wait_for_complete()

# CRITICAL: Give callbacks a millisecond to populate the 'results' dict
time.sleep(0.2)

print("\n--- RESULTS (Sorted by Arrival Order) ---")

# Sort results by the timestamp (the first element in the tuple)
sorted_items = sorted(results.values(), key=lambda x: x[0])

for info in sorted_items:
    ts_val, comp_id, ret = info[0], info[1], info[2]
    ts_str = time.strftime('%H:%M:%S', time.localtime(ts_val)) + f".{int((ts_val%1)*1000):03d}"
    
    # Identify which handle this ID belongs to
    if comp_id == id(comp1):
        label = "Read 1 "
    elif comp_id == id(comp2):
        label = "Read 2 "
    elif comp_id == id(comp_write):
        label = "Write  "
    else:
        label = "Unknown"

    if "Read" in label:
        data = info[3] # The 4th element is the data_read bytes
        val = data.decode(errors='replace').rstrip('\x00') if data else "None"
        print(f"[{ts_str}] [ID:{comp_id}] {label} finished. Ret: {ret} | Data: '{val}'")
    else:
        print(f"[{ts_str}] [ID:{comp_id}] {label} finished. Ret: {ret}")

# --- FINAL VERIFICATION READ ---
# Ensure OSD has finalized the write
time.sleep(0.5)
final_val = ioctx.read(obj_name).decode().rstrip('\x00')
print(f"\nFinal object content in OSD: '{final_val}'")

# --- Cleanup at the end of successful run ---
set_hook(primary_osd_id, False)
ioctx.close()
cluster.shutdown()

