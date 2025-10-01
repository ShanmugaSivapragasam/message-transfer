#!/bin/bash

echo "🚀 Starting Message Transfer Application..."

# Load environment variables from .env
export $(cat .env | grep -v '^#' | xargs)

echo "✅ Environment variables loaded"
echo "📦 Building application..."

# Build the application
./gradlew build -q

echo "🏃 Starting Spring Boot application..."

# Run with explicit environment variables
java \
  -Dazure.servicebus.source.connection-string="$AZURE_SERVICEBUS_SOURCE_CONNECTION_STRING" \
  -Dazure.servicebus.dest.connection-string="$AZURE_SERVICEBUS_DEST_CONNECTION_STRING" \
  -Dspring.data.redis.host="$SPRING_DATA_REDIS_HOST" \
  -Dspring.data.redis.port="$SPRING_DATA_REDIS_PORT" \
  -Dspring.data.redis.password="$SPRING_DATA_REDIS_PASSWORD" \
  -Dspring.data.redis.ssl.enabled="$SPRING_DATA_REDIS_SSL_ENABLED" \
  -jar build/libs/message-transfer-0.0.1-SNAPSHOT.jar