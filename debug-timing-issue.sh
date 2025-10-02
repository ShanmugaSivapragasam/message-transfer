#!/bin/bash

echo "=== DEBUGGING MESSAGE TIMING ISSUE ==="
echo "This script will help debug why one message goes active and another stays scheduled"
echo

# Function to make API calls with error handling
call_api() {
    local url="$1"
    local method="${2:-GET}"
    local description="$3"
    
    echo "🔍 $description"
    echo "   → $method $url"
    
    if [ "$method" = "POST" ]; then
        response=$(curl -s -X POST "$url" 2>/dev/null)
    else
        response=$(curl -s "$url" 2>/dev/null)
    fi
    
    if [ $? -eq 0 ] && [ -n "$response" ]; then
        echo "$response" | jq . 2>/dev/null || echo "$response"
    else
        echo "   ❌ API call failed or empty response"
    fi
    echo
}

# Check if server is running
echo "🚀 Checking if application is running..."
if ! curl -s http://localhost:8080/api/health > /dev/null 2>&1; then
    echo "❌ Application not running on http://localhost:8080"
    echo "Please start the application first with: ./gradlew bootRun"
    exit 1
fi
echo "✅ Application is running"
echo

# Step 1: Clean up any existing state
echo "🧹 Cleaning up existing state..."
call_api "http://localhost:8080/api/cleanup" "POST" "Clean all queues"

# Step 2: Check initial state
call_api "http://localhost:8080/api/transfer/status" "GET" "Check initial transfer status"

# Step 3: Schedule 2 messages with same timing
echo "📅 Scheduling 2 messages with 300 second delay (5 minutes)..."
call_api "http://localhost:8080/api/schedule?count=2&delaySeconds=300" "POST" "Schedule 2 messages with longer delay"

# Step 4: Check queue states before transfer
call_api "http://localhost:8080/api/debug/queue-analysis?peek=10" "GET" "Analyze queue states BEFORE transfer"

# Step 5: Perform transfer
echo "🔄 Performing transfer..."
call_api "http://localhost:8080/api/transfer?printMetadata=true&cleanupSource=false" "POST" "Transfer messages with metadata logging"

# Step 6: Analyze queue states after transfer
call_api "http://localhost:8080/api/debug/queue-analysis?peek=10" "GET" "Analyze queue states AFTER transfer"

# Step 7: Check timing analysis
call_api "http://localhost:8080/api/debug/timing-analysis" "GET" "Analyze transfer timing history"

# Step 8: Validate final state
call_api "http://localhost:8080/api/validate?peek=10&includeTimings=true" "GET" "Final validation with timings"

echo "=== DEBUG COMPLETE ==="
echo "Look for:"
echo "1. 'shouldBeScheduled=NO_WILL_BE_ACTIVE' in transfer logs"
echo "2. Messages with 'status=ACTIVE' vs 'status=SCHEDULED' in queue analysis"
echo "3. 'minutesUntilScheduled' values in timing analysis"
echo "4. Any timing mismatches in the final validation"