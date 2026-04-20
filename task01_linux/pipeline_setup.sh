#!/usr/bin/env bash
# =============================================================================
# Task 01 – Linux + File System
# Digital Wallet Data Pipeline Directory Setup
# Sets up directory structure, file permissions, automates file movement,
# and logs every operation with timestamps.
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="$PROJECT_ROOT/logs/pipeline_operations.log"
PIPELINE="$PROJECT_ROOT/pipeline_dirs"
TIMESTAMP() { date '+%Y-%m-%d %H:%M:%S'; }

log() {
    local msg="[$(TIMESTAMP)] $1"
    echo "$msg"
    echo "$msg" >> "$LOG_FILE"
}

# ── 1. Create structured pipeline directories ─────────────────────────────────
log "=== Task 01: Pipeline Setup Started ==="
log "Creating directory structure..."

for dir in raw processed archive failed temp config logs/daily logs/weekly; do
    mkdir -p "$PIPELINE/$dir"
done
log "Directories created under: $PIPELINE"

# ── 2. Set permissions ────────────────────────────────────────────────────────
chmod 755 "$PIPELINE/raw"
chmod 755 "$PIPELINE/processed"
chmod 700 "$PIPELINE/failed"      # restricted – only owner
chmod 755 "$PIPELINE/archive"
chmod 755 "$PIPELINE/config"
log "Permissions set: raw=755 | processed=755 | failed=700 | archive=755"

# ── 3. Copy source CSV files into raw/ ───────────────────────────────────────
for f in "$PROJECT_ROOT/data/"transactions_2023_*.csv; do
    cp "$f" "$PIPELINE/raw/"
    log "Copied: $(basename $f) → raw/"
done

# ── 4. Process files: raw/ → processed/ ──────────────────────────────────────
PROCESSED_COUNT=0
for f in "$PIPELINE/raw/"*.csv; do
    base=$(basename "$f" .csv)
    dest="$PIPELINE/processed/${base}_processed.csv"
    cp "$f" "$dest"
    log "Processed: $base → processed/"
    PROCESSED_COUNT=$((PROCESSED_COUNT + 1))
done
log "Total files processed: $PROCESSED_COUNT"

# ── 5. Archive processed files ────────────────────────────────────────────────
ARCHIVE_DIR="$PIPELINE/archive/$(date '+%Y%m%d')"
mkdir -p "$ARCHIVE_DIR"
cp "$PIPELINE/processed/"*.csv "$ARCHIVE_DIR/"
ARCHIVED=$(ls "$ARCHIVE_DIR" | wc -l)
log "Archived $ARCHIVED file(s) → archive/$(date '+%Y%m%d')/"

# ── 6. Generate file summary report ──────────────────────────────────────────
REPORT="$PIPELINE/config/pipeline_report.txt"
{
    echo "=== Pipeline File Report ==="
    echo "Generated : $(TIMESTAMP)"
    echo ""
    for d in raw processed "archive/$(date '+%Y%m%d')"; do
        count=$(ls "$PIPELINE/$d" 2>/dev/null | wc -l)
        size=$(du -sh "$PIPELINE/$d" 2>/dev/null | cut -f1)
        echo "[$d]  Files: $count  |  Size: $size"
    done
} > "$REPORT"
log "Report written → $REPORT"

# ── 7. Show directory tree ────────────────────────────────────────────────────
log "Directory tree:"
find "$PIPELINE" -maxdepth 3 -type d | sort | \
    awk -F/ '{printf "%s%s\n", substr("                    ", 1, (NF-1)*2), $NF}' \
    >> "$LOG_FILE"

log "=== Task 01: Pipeline Setup Complete ==="
echo ""
echo "Log  : $LOG_FILE"
echo "Report: $REPORT"
