#!/bin/bash
set -e

# --- Usage ---
# ./buildAndRun                 build and run bug repro
# ./buildAndRun fix             build and run fix
# ./buildAndRun b               build only
# ./buildAndRun r               run only the bug repro
# ./buildAndRun r fix           run only the fix

# --- CCACHE PREP ---
# Reset stats so we only see the data for THIS specific build session.
# We redirect to /dev/null to keep the start of the script clean.
if command -v ccache &> /dev/null; then
    ccache -z > /dev/null
else
    echo "Warning: ccache not found. Statistics will be skipped."
fi

# Start the timer
START_TIME=$SECONDS

# Check if run from a 'build' directory
if [[ "${PWD##*/}" != "build" ]]; then
    echo "Error: This script must be run from a 'build' directory."
    exit 1
fi

ACTION=${1:-all}

do_cleanup() {
    echo "Stopping and wiping cluster state..."
    ../src/stop.sh || true
    rm -rf dev/* out/*
}

do_build() {
    echo "Starting build with Ninja..."
    ninja -j 3 vstart-base
}

reproduce_bug() {
    echo "Running bug reproduction script..."
    python3 ../reproduce_bug_75403.py
}

run_fix() {
    echo "Running bug fix verification..."
    python3 ../reproduce_bug_75403.py --after-fix
}

do_io() {
    echo "Performing baseline I/O..."
    echo "baseline" | ./bin/rados -p rbd put my_test_obj -
    ./bin/rados -p rbd get my_test_obj -
}

do_run() {
    echo "Starting vstart cluster..."
    env MON=1 MGR=1 OSD=3 MDS=0 ../src/vstart.sh -n -d -x
    ./bin/ceph -s
    ./bin/ceph osd pool create rbd 8 8
    ./bin/ceph osd pool ls
    do_io
    # Logic: Check if the word "fix" was passed anywhere in the command line
    if [[ "$*" == *"fix"* ]]; then
        run_fix
    else
        reproduce_bug
    fi
}

case "$ACTION" in
    all|fix)  # Handles: ./buildAndRun all OR ./buildAndRun fix
        do_cleanup
        do_build
        do_run "$@"
        ;;
    b|bu|bui|buil|build)
        do_cleanup
        do_build
        ;;
    r|ru|run) # Handles: ./buildAndRun r fix
        do_cleanup
        do_run "$@"
        ;;
    *)
        echo "Error: Invalid parameter '$ACTION'"
        echo "Usage: $0 [all | build | run | fix]"
        exit 1
        ;;
esac

# Calculate and display total time
ELAPSED_TIME=$(($SECONDS - $START_TIME))
echo ""
echo "-------------------------------------------"
echo "DONE!"
echo "Total time: $(($ELAPSED_TIME / 60))m $(($ELAPSED_TIME % 60))s"

# --- CCACHE ANALYSIS ---
# Only print if we actually attempted a build (ACTION is not run)
if [[ "$ACTION" != "r" && "$ACTION" != "ru" && "$ACTION" != "run" ]]; then
    if command -v ccache &> /dev/null; then
        echo "-------------------------------------------"
        echo "ccache Stats for this Session:"
        ccache -s | grep -m 3 -E 'Hits:|Misses:|Cacheable calls'
        echo "-------------------------------------------"
    fi
else
    echo "-------------------------------------------"
    echo "Skipping ccache analysis (run-only mode)"
    echo "-------------------------------------------"
fi

