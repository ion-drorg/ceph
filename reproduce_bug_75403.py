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

def set_hook(osd_id, enabled, no_assert=False, after_fix=False):
    """Sets the OSD hooks and manages debug_op_order and after_fix path."""
    hook_val = "true" if enabled else "false"

    # Logic:
    # 1. If we are cleaning up (enabled=False), everything goes to default.
    # 2. If no_assert is requested, we disable the order check to see data corruption instead of a crash.
    debug_order_val = "false" if (enabled and no_assert) else "true"

    # Logic:
    # Set after_fix to true only if the hook is being enabled AND the flag was passed.
    after_fix_val = "true" if (enabled and after_fix) else "false"

    print(f"\n[CONFIG] Target: osd.{osd_id}")
    print(f"         osd_reproduce_bug_75403_hook={hook_val}")
    print(f"         osd_debug_op_order={debug_order_val}")
    print(f"         osd_bug_75403_after_fix={after_fix_val}")

    configs = {
        "osd_reproduce_bug_75403_hook": hook_val,
        "osd_debug_op_order": debug_order_val,
        "osd_bug_75403_after_fix": after_fix_val
    }

    # Apply all configs to the specific OSD
    for opt, val in configs.items():
        cmd = ["./bin/ceph", "-c", config_path_global, "config", "set", f"osd.{osd_id}", opt, val]
        run_cmd(cmd)

    # Verification Loop (Focusing on the main hook)
    verify_cmd = ["./bin/ceph", "-c", config_path_global, "config", "get",
                  f"osd.{osd_id}", "osd_reproduce_bug_75403_hook"]

    for attempt in range(5):
        current_state = run_cmd(verify_cmd).strip().lower()
        if current_state == hook_val:
            print(f"[CONFIRMED] Hook state is successfully {hook_val} on osd.{osd_id}")
            return
        time.sleep(1)

    # If we get here during 'enable', something is wrong with the cluster communication
    if enabled:
        print("[ERROR] Failed to confirm hook application. Is the OSD running?")
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
parser.add_argument('-n', '--no-assert', action='store_true',
                    help='Set osd_debug_op_order=false to avoid OSD crash/assert')
parser.add_argument('-a', '--after-fix', action='store_true',
                    help='Set osd_bug_75403_after_fix=true to test the fix path')
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

print(f"*** {os.path.basename(__file__)} ***")
print(f'Meant to be run once on a new vstart cluster.')
print("If gets stuck:")
print(f"pkill -9 -f {os.path.basename(__file__)}\n")

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
        print(f"         ./bin/ceph osd pool create rbd 8 8")
        print(f"         # Initialize OMAP structures (optional):")
        print(f"         ./bin/rbd pool init rbd")
        print("\n[ADVICE] OR create and initialize a new EC Pool Pair (Metadata + Data):")
        print(f"         ./bin/ceph osd erasure-code-profile set myprofile k=2 m=1")
        print(f"         ./bin/ceph osd pool create ec-pool-data 8 8 erasure myprofile")
        print(f"         ./bin/ceph osd pool set ec-pool-data allow_ec_overwrites true")
        print(f"         # Create and initialize OMAP Metadata pool (Replicated):")
        print(f"         ./bin/ceph osd pool create ec-pool-metadata 8 8")
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
modified_content_1 = b"mod value 1"
modified_content_2 = b"modified value 2"

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
set_hook(primary_osd_id, True, args.no_assert, args.after_fix)

# Dispatch Ops - Exactly matching your rados.pyx signatures
print("Dispatching racing reads and write...")

# Suggest user check the specific OSD log
log_path = f"out/osd.{primary_osd_id}.log"

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

print(f"Writing object: '{obj_name}' with value: '{modified_content_1.decode()}' ({len(modified_content_1)} chars)")
comp1_write = ioctx.aio_write_full(obj_name, modified_content_1, write_cb)
print(f"[{now()}] Write {modified_content_1.decode()} sent.")

time.sleep(0.2)

comp2_read = ioctx.aio_read(obj_name, 4096, 0, read_cb)
print(f"[{now()}] Read sent.")

time.sleep(0.2)

print(f"Writing object: '{obj_name}' with value: '{modified_content_2.decode()}' ({len(modified_content_2)} chars)")
comp3_write = ioctx.aio_write_full(obj_name, modified_content_2, write_cb)
print(f"[{now()}] Write {modified_content_2.decode()} sent.")

time.sleep(0.2)

# --- Wait and Print Results ---
print(f"\n[TEST STATE] Waiting for completions at {time.strftime('%H:%M:%S')}...")

if args.after_fix:
    print(f"[EXPECTED BEHAVIOR] FIX ACTIVE: The OSD should correctly serialize the operations.")
    print(f"                    The final value should be '{modified_content_2.decode()}' and no crash should occur.")
elif args.no_assert:
    print(f"[EXPECTED BEHAVIOR] BUG REPRO (No Assert): The OSD will not crash, but the race condition will allow")
    print(f"                    '{modified_content_1.decode()}' to override the newer value due to out-of-order execution.")
else:
    print(f"[EXPECTED BEHAVIOR] BUG REPRO (Assert): OSD is expected to crash due to osd_debug_op_order=true.")
    print(f"                    The client will likely receive the value from a replica, but order is still violated.")

# --- Wait for all ---
comp1_write.wait_for_complete()
comp2_read.wait_for_complete()
comp3_write.wait_for_complete()

# CRITICAL: Give callbacks a millisecond to populate the 'results' dict
time.sleep(0.2)

print("\n--- RESULTS (Sorted by Arrival Order) ---")

# Sort results by the timestamp (the first element in the tuple)
sorted_items = sorted(results.values(), key=lambda x: x[0])

for info in sorted_items:
    ts_val, comp_id, ret = info[0], info[1], info[2]
    ts_str = time.strftime('%H:%M:%S', time.localtime(ts_val)) + f".{int((ts_val%1)*1000):03d}"

    # Identify which handle this ID belongs to
    if comp_id == id(comp1_write):
        label = f"Write {modified_content_1.decode()} "
    elif comp_id == id(comp2_read):
        label = "Read      "
    elif comp_id == id(comp3_write):
        label = f"Write {modified_content_2.decode()} "
    else:
        label = "Unknown   "

    if "Read" in label:
        data = info[3] # The 4th element is the data_read bytes
        val = data.decode(errors='replace').rstrip('\x00') if data else "None"
        print(f"[{ts_str}] [ID:{comp_id}] {label} finished. Ret: {ret} | Data: '{val}'")
    else:
        print(f"[{ts_str}] [ID:{comp_id}] {label} finished. Ret: {ret}")

# --- Results Analysis ---
print(f"\n[{now()}] --- Final Verification ---")
final_val = ioctx.read(obj_name)
print(f"Final Object Content: '{final_val.decode()}'")

# --- ANSI Color Codes ---
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# --- Final Check ---
if final_val == modified_content_1:
    print(f"\n{RED}[!!] BUG REPRODUCED{RESET}: {modified_content_1.decode()} overrode the newer {modified_content_2.decode()}.")
elif final_val == modified_content_2:
    print(f"\n{GREEN}[OK]{RESET} Final state is consistent with the last write.")
else:
    print(f"\n{YELLOW}[UNKNOWN]{RESET} Object contains unexpected data: {final_val.decode()}")

# --- Cleanup ---
set_hook(primary_osd_id, False)
ioctx.close()
cluster.shutdown()
print(f"[{now()}] Test complete.")

print(f'\nCheck OSD log:')
print(f'grep -aE "do_op before hook check:|: do_op entry:|trigger_laggy|Hook for bug 75429|not readable|out of order op" {log_path} | less -S -N\n')


