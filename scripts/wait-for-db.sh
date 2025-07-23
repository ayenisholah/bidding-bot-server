#!/bin/bash

echo "Waiting for PostgreSQL to be ready..."

# Maximum number of attempts
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker exec postgres_container pg_isready -U bidding_bot > /dev/null 2>&1; then
        echo "PostgreSQL is ready!"
        
        # Additional check to ensure database exists
        if docker exec postgres_container psql -U bidding_bot -d bidding_bot_db -c "SELECT 1;" > /dev/null 2>&1; then
            echo "Database bidding_bot_db is accessible!"
            exit 0
        else
            echo "Database not yet accessible, waiting..."
        fi
    else
        echo "PostgreSQL is not ready yet, waiting... (attempt $((ATTEMPT+1))/$MAX_ATTEMPTS)"
    fi
    
    ATTEMPT=$((ATTEMPT+1))
    sleep 2
done

echo "Failed to connect to PostgreSQL after $MAX_ATTEMPTS attempts"
exit 1