#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Determine which docker-compose command to use
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif command -v docker compose &> /dev/null; then
    DOCKER_COMPOSE="docker compose" 
else
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${YELLOW}Starting Kafka infrastructure and ERP-Blockchain consumer in ordered sequence${NC}"

# Check if we are in the kafka-debezium directory
if [ ! -f "./docker-compose.yml" ]; then
  echo -e "${RED}Error: docker-compose.yml not found.${NC}"
  echo -e "${YELLOW}Please run this script from the kafka-debezium directory.${NC}"
  exit 1
fi

# Stop any existing services
echo -e "${YELLOW}Stopping any existing services...${NC}"
$DOCKER_COMPOSE down

# Start services in order
echo -e "${YELLOW}Starting Zookeeper...${NC}"
$DOCKER_COMPOSE up -d zookeeper
echo -e "${GREEN}Zookeeper started. Waiting 10 seconds...${NC}"
sleep 10

echo -e "${YELLOW}Starting Kafka...${NC}"
$DOCKER_COMPOSE up -d kafka
echo -e "${GREEN}Kafka started. Waiting 30 seconds...${NC}"
sleep 30

echo -e "${YELLOW}Starting Kafka Connect...${NC}"
$DOCKER_COMPOSE up -d connect
echo -e "${GREEN}Kafka Connect started. Waiting 20 seconds...${NC}"
sleep 20

echo -e "${YELLOW}Starting Kafka UI...${NC}"
$DOCKER_COMPOSE up -d kafka-ui
echo -e "${GREEN}Kafka UI started.${NC}"

echo -e "${YELLOW}Starting Consumer...${NC}"
$DOCKER_COMPOSE up -d consumer
echo -e "${GREEN}Consumer started.${NC}"

echo -e "${GREEN}All services have been started in the correct order.${NC}"
echo -e "${YELLOW}View logs with: $DOCKER_COMPOSE logs -f consumer${NC}"
echo -e "${YELLOW}Kafka UI is available at: http://localhost:8085${NC}"
echo -e "${YELLOW}Kafka Connect REST API is available at: http://localhost:8083${NC}"