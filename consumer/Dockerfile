FROM node:18-alpine

WORKDIR /app

# Install wait-for-it and other utilities
RUN apk add --no-cache bash curl netcat-openbsd

# Copy package.json and package-lock.json first for better caching
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the application code
COPY . .

# Create a startup script that properly reads the .env file
RUN echo '#!/bin/bash\n\
echo "Waiting for Kafka to be fully ready..."\n\
# Try to connect to Kafka\n\
MAX_RETRY=30\n\
RETRY=0\n\
echo "Checking Kafka connectivity..."\n\
\n\
# Parse environment variables from .env file\n\
if [ -f "/app/.env" ]; then\n\
  echo "Found .env file, loading environment variables..."\n\
  export $(grep -v "^#" /app/.env | xargs)\n\
  echo "Using KAFKA_BROKER: $KAFKA_BROKER"\n\
  echo "Using API_ENDPOINT: $API_ENDPOINT"\n\
else\n\
  echo "Warning: No .env file found!"\n\
fi\n\
\n\
# Get Kafka host and port from KAFKA_BROKER\n\
KAFKA_HOST=$(echo $KAFKA_BROKER | cut -d: -f1)\n\
KAFKA_PORT=$(echo $KAFKA_BROKER | cut -d: -f2)\n\
\n\
# If KAFKA_BROKER is not set or is set to localhost/127.0.0.1, use kafka:9092\n\
if [ -z "$KAFKA_HOST" ] || [ "$KAFKA_HOST" = "localhost" ] || [ "$KAFKA_HOST" = "127.0.0.1" ]; then\n\
  echo "KAFKA_BROKER references localhost, using kafka:9092 instead..."\n\
  KAFKA_HOST="kafka"\n\
  KAFKA_PORT="9092"\n\
  export KAFKA_BROKER="kafka:9092"\n\
fi\n\
\n\
# Check if API_ENDPOINT references localhost/127.0.0.1 and warn\n\
if [[ "$API_ENDPOINT" == *"localhost"* ]] || [[ "$API_ENDPOINT" == *"127.0.0.1"* ]]; then\n\
  echo "Warning: API_ENDPOINT references localhost, this might not work from inside the container."\n\
  echo "Consider using host.docker.internal:4001 instead."\n\
fi\n\
\n\
echo "Checking connection to Kafka at $KAFKA_HOST:$KAFKA_PORT..."\n\
until nc -z $KAFKA_HOST $KAFKA_PORT || [ $RETRY -eq $MAX_RETRY ]; do\n\
  echo "Kafka is not available yet - sleeping (retry $RETRY/$MAX_RETRY)"\n\
  RETRY=$((RETRY+1))\n\
  sleep 2\n\
done\n\
\n\
if [ $RETRY -eq $MAX_RETRY ]; then\n\
  echo "Failed to connect to Kafka after multiple retries - continuing anyway"\n\
else\n\
  echo "Kafka is up!"\n\
fi\n\
\n\
echo "Waiting additional time for topic initialization (30 seconds)..."\n\
sleep 30\n\
\n\
echo "Starting consumer application..."\n\
exec node consumer.js\n\
' > /app/start.sh && chmod +x /app/start.sh

# Set the startup script as the entry point
ENTRYPOINT ["/app/start.sh"]