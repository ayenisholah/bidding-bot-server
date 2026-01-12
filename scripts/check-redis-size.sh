#!/bin/bash
# Redis Size Analysis Script for NFT Bidding Bot
# Shows BullMQ queue sizes and Order Cache memory usage

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Try to find Redis container
REDIS_CONTAINER=$(docker ps --format '{{.Names}}' | grep -E 'redis' | head -1 2>/dev/null || echo "")

if [ -z "$REDIS_CONTAINER" ]; then
    echo -e "${RED}Error: No Redis container found${NC}"
    echo "Trying localhost connection..."
    REDIS_CMD="redis-cli"
else
    REDIS_CMD="docker exec $REDIS_CONTAINER redis-cli"
fi

# Test connection
if ! $REDIS_CMD PING > /dev/null 2>&1; then
    echo -e "${RED}Error: Cannot connect to Redis${NC}"
    exit 1
fi

echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${CYAN}                    REDIS SIZE ANALYSIS                          ${NC}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════════${NC}"
echo ""

# ============== MEMORY OVERVIEW ==============
echo -e "${BOLD}${BLUE}📊 MEMORY OVERVIEW${NC}"
echo -e "${BLUE}────────────────────────────────────────${NC}"
USED_MEM=$($REDIS_CMD INFO memory | grep "used_memory_human" | cut -d: -f2 | tr -d '\r')
PEAK_MEM=$($REDIS_CMD INFO memory | grep "used_memory_peak_human" | cut -d: -f2 | tr -d '\r')
TOTAL_KEYS=$($REDIS_CMD DBSIZE | awk '{print $2}')
echo -e "Used Memory:      ${GREEN}$USED_MEM${NC}"
echo -e "Peak Memory:      ${YELLOW}$PEAK_MEM${NC}"
echo -e "Total Keys:       ${GREEN}$TOTAL_KEYS${NC}"
echo ""

# ============== BULLMQ MAIN QUEUE ==============
echo -e "${BOLD}${BLUE}📦 BULLMQ: BIDDING_BOT (Main Queue)${NC}"
echo -e "${BLUE}────────────────────────────────────────${NC}"

MAIN_WAIT=$($REDIS_CMD LLEN bull:BIDDING_BOT:wait 2>/dev/null || echo "0")
MAIN_ACTIVE=$($REDIS_CMD LLEN bull:BIDDING_BOT:active 2>/dev/null || echo "0")
MAIN_DELAYED=$($REDIS_CMD ZCARD bull:BIDDING_BOT:delayed 2>/dev/null || echo "0")
MAIN_COMPLETED=$($REDIS_CMD ZCARD bull:BIDDING_BOT:completed 2>/dev/null || echo "0")
MAIN_FAILED=$($REDIS_CMD ZCARD bull:BIDDING_BOT:failed 2>/dev/null || echo "0")
MAIN_PRIORITIZED=$($REDIS_CMD ZCARD bull:BIDDING_BOT:prioritized 2>/dev/null || echo "0")

echo -e "  Waiting:             ${GREEN}$MAIN_WAIT${NC}"
echo -e "  Active:              ${YELLOW}$MAIN_ACTIVE${NC}"
echo -e "  Delayed:             ${CYAN}$MAIN_DELAYED${NC}"
echo -e "  Prioritized:         ${CYAN}$MAIN_PRIORITIZED${NC}"
echo -e "  Completed:           ${GREEN}$MAIN_COMPLETED${NC}"
echo -e "  Failed:              ${RED}$MAIN_FAILED${NC}"

# Count job data keys
MAIN_JOB_KEYS=$($REDIS_CMD --scan --pattern 'bull:BIDDING_BOT:*' 2>/dev/null | wc -l)
echo -e "  Total Keys:          ${BLUE}$MAIN_JOB_KEYS${NC}"

# Sample job size
SAMPLE_JOB=$($REDIS_CMD --scan --pattern 'bull:BIDDING_BOT:*' --count 1 2>/dev/null | head -1)
if [ -n "$SAMPLE_JOB" ]; then
    SAMPLE_SIZE=$($REDIS_CMD MEMORY USAGE "$SAMPLE_JOB" 2>/dev/null || echo "N/A")
    if [ "$SAMPLE_SIZE" != "N/A" ]; then
        echo -e "  Sample Key Size:     ${CYAN}${SAMPLE_SIZE} bytes${NC}"
    fi
fi
echo ""

# ============== BULLMQ BLUR QUEUE ==============
echo -e "${BOLD}${BLUE}📦 BULLMQ: BLUR_BIDDING_QUEUE${NC}"
echo -e "${BLUE}────────────────────────────────────────${NC}"

BLUR_WAIT=$($REDIS_CMD LLEN bull:BLUR_BIDDING_QUEUE:wait 2>/dev/null || echo "0")
BLUR_ACTIVE=$($REDIS_CMD LLEN bull:BLUR_BIDDING_QUEUE:active 2>/dev/null || echo "0")
BLUR_DELAYED=$($REDIS_CMD ZCARD bull:BLUR_BIDDING_QUEUE:delayed 2>/dev/null || echo "0")
BLUR_COMPLETED=$($REDIS_CMD ZCARD bull:BLUR_BIDDING_QUEUE:completed 2>/dev/null || echo "0")
BLUR_FAILED=$($REDIS_CMD ZCARD bull:BLUR_BIDDING_QUEUE:failed 2>/dev/null || echo "0")
BLUR_PRIORITIZED=$($REDIS_CMD ZCARD bull:BLUR_BIDDING_QUEUE:prioritized 2>/dev/null || echo "0")

echo -e "  Waiting:             ${GREEN}$BLUR_WAIT${NC}"
echo -e "  Active:              ${YELLOW}$BLUR_ACTIVE${NC}"
echo -e "  Delayed:             ${CYAN}$BLUR_DELAYED${NC}"
echo -e "  Prioritized:         ${CYAN}$BLUR_PRIORITIZED${NC}"
echo -e "  Completed:           ${GREEN}$BLUR_COMPLETED${NC}"
echo -e "  Failed:              ${RED}$BLUR_FAILED${NC}"

BLUR_JOB_KEYS=$($REDIS_CMD --scan --pattern 'bull:BLUR_BIDDING_QUEUE:*' 2>/dev/null | wc -l)
echo -e "  Total Keys:          ${BLUE}$BLUR_JOB_KEYS${NC}"
echo ""

# ============== ORDER CACHE ==============
echo -e "${BOLD}${BLUE}🗄️  ORDER CACHE${NC}"
echo -e "${BLUE}────────────────────────────────────────${NC}"

# Count different order key patterns
ORDER_DATA_KEYS=$($REDIS_CMD --scan --pattern 'order:*' 2>/dev/null | grep -v 'order:id:' | wc -l)
ORDER_ID_KEYS=$($REDIS_CMD --scan --pattern 'order:id:*' 2>/dev/null | wc -l)
TASK_ORDER_SETS=$($REDIS_CMD --scan --pattern 'task:orders:*' 2>/dev/null | wc -l)

echo -e "  Order Data Keys:     ${GREEN}$ORDER_DATA_KEYS${NC}"
echo -e "  Order ID Mappings:   ${CYAN}$ORDER_ID_KEYS${NC}"
echo -e "  Task Order Sets:     ${YELLOW}$TASK_ORDER_SETS${NC}"

# Sample order size
SAMPLE_ORDER=$($REDIS_CMD --scan --pattern 'order:*' --count 100 2>/dev/null | grep -v 'order:id:' | head -1)
if [ -n "$SAMPLE_ORDER" ]; then
    SAMPLE_ORDER_SIZE=$($REDIS_CMD MEMORY USAGE "$SAMPLE_ORDER" 2>/dev/null || echo "N/A")
    SAMPLE_ORDER_TTL=$($REDIS_CMD TTL "$SAMPLE_ORDER" 2>/dev/null || echo "N/A")
    if [ "$SAMPLE_ORDER_SIZE" != "N/A" ]; then
        echo -e "  Sample Order Size:   ${CYAN}${SAMPLE_ORDER_SIZE} bytes${NC}"
    fi
    if [ "$SAMPLE_ORDER_TTL" != "N/A" ] && [ "$SAMPLE_ORDER_TTL" != "-1" ]; then
        TTL_MIN=$((SAMPLE_ORDER_TTL / 60))
        echo -e "  Sample TTL:          ${CYAN}${TTL_MIN} minutes${NC}"
    fi
fi

# Estimate total order cache size
if [ "$ORDER_DATA_KEYS" -gt 0 ] && [ -n "$SAMPLE_ORDER_SIZE" ] && [ "$SAMPLE_ORDER_SIZE" != "N/A" ]; then
    EST_ORDER_SIZE=$((ORDER_DATA_KEYS * SAMPLE_ORDER_SIZE))
    if [ $EST_ORDER_SIZE -gt 1048576 ]; then
        EST_ORDER_MB=$((EST_ORDER_SIZE / 1048576))
        echo -e "  Est. Total Size:     ${YELLOW}~${EST_ORDER_MB} MB${NC}"
    elif [ $EST_ORDER_SIZE -gt 1024 ]; then
        EST_ORDER_KB=$((EST_ORDER_SIZE / 1024))
        echo -e "  Est. Total Size:     ${YELLOW}~${EST_ORDER_KB} KB${NC}"
    else
        echo -e "  Est. Total Size:     ${YELLOW}~${EST_ORDER_SIZE} bytes${NC}"
    fi
fi
echo ""

# ============== MEMORY BY KEY PATTERN ==============
echo -e "${BOLD}${BLUE}📈 ESTIMATED MEMORY BY PATTERN${NC}"
echo -e "${BLUE}────────────────────────────────────────${NC}"

# Function to estimate memory for a pattern
estimate_pattern_memory() {
    local pattern=$1
    local name=$2
    local count=$($REDIS_CMD --scan --pattern "$pattern" 2>/dev/null | wc -l)

    if [ "$count" -gt 0 ]; then
        # Sample up to 10 keys and average their sizes
        local total_size=0
        local sampled=0
        while IFS= read -r key; do
            if [ -n "$key" ]; then
                local size=$($REDIS_CMD MEMORY USAGE "$key" 2>/dev/null || echo "0")
                if [ "$size" != "0" ] && [ -n "$size" ]; then
                    total_size=$((total_size + size))
                    sampled=$((sampled + 1))
                fi
            fi
            [ $sampled -ge 10 ] && break
        done < <($REDIS_CMD --scan --pattern "$pattern" --count 100 2>/dev/null | head -10)

        if [ $sampled -gt 0 ]; then
            local avg_size=$((total_size / sampled))
            local est_total=$((count * avg_size))

            if [ $est_total -gt 1073741824 ]; then
                local est_gb=$((est_total / 1073741824))
                echo -e "  $name: ${count} keys × ${avg_size} bytes ≈ ${YELLOW}${est_gb} GB${NC}"
            elif [ $est_total -gt 1048576 ]; then
                local est_mb=$((est_total / 1048576))
                echo -e "  $name: ${count} keys × ${avg_size} bytes ≈ ${YELLOW}${est_mb} MB${NC}"
            elif [ $est_total -gt 1024 ]; then
                local est_kb=$((est_total / 1024))
                echo -e "  $name: ${count} keys × ${avg_size} bytes ≈ ${CYAN}${est_kb} KB${NC}"
            else
                echo -e "  $name: ${count} keys × ${avg_size} bytes ≈ ${GREEN}${est_total} B${NC}"
            fi
        else
            echo "  $name: ${count} keys"
        fi
    fi
}

estimate_pattern_memory "bull:BIDDING_BOT:*" "Main Queue (bull:BIDDING_BOT)"
estimate_pattern_memory "bull:BLUR_BIDDING_QUEUE:*" "Blur Queue (bull:BLUR_*)"
estimate_pattern_memory "order:*" "Order Cache (order:*)"
estimate_pattern_memory "task:orders:*" "Task Order Sets (task:orders:*)"

echo ""

# ============== OTHER KEYS ==============
echo -e "${BOLD}${BLUE}🔑 OTHER KEY PATTERNS${NC}"
echo -e "${BLUE}────────────────────────────────────────${NC}"

# Find other key patterns
OTHER_PATTERNS=$($REDIS_CMD --scan --count 1000 2>/dev/null | sed 's/:.*/:*/' | sort -u | grep -v -E '^(bull:|order:|task:orders:)' | head -20)
if [ -n "$OTHER_PATTERNS" ]; then
    while IFS= read -r pattern; do
        if [ -n "$pattern" ]; then
            count=$($REDIS_CMD --scan --pattern "$pattern" 2>/dev/null | wc -l)
            if [ "$count" -gt 0 ]; then
                printf "  %-40s %s keys\n" "$pattern" "$count"
            fi
        fi
    done <<< "$OTHER_PATTERNS"
else
    echo "  No other key patterns found"
fi

echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}Analysis complete at $(date '+%Y-%m-%d %H:%M:%S')${NC}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════════${NC}"
