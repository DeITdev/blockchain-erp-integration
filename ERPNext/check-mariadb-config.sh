#!/bin/bash
# Script to check MariaDB configuration for Debezium compatibility

echo "Checking MariaDB configuration for Debezium compatibility..."

# Find the MariaDB container name
CONTAINER_NAME=$(docker ps --format "{{.Names}}" | grep -E 'db|mariadb|mysql')

if [ -z "$CONTAINER_NAME" ]; then
  echo "Error: Could not find a running MariaDB container."
  echo "Please make sure the ERPNext database container is running."
  exit 1
fi

echo "Using container: $CONTAINER_NAME"

# Connect to MariaDB and check binary logging status
echo -e "\nChecking binary logging status:"
docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "SHOW VARIABLES LIKE 'log_bin';"

echo -e "\nChecking binary log format:"
docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "SHOW VARIABLES LIKE 'binlog_format';"

echo -e "\nChecking server ID:"
docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "SHOW VARIABLES LIKE 'server_id';"

echo -e "\nChecking binary log file name:"
docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "SHOW VARIABLES LIKE 'log_bin_basename';"

echo -e "\nChecking binlog row image:"
docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "SHOW VARIABLES LIKE 'binlog_row_image';"

echo -e "\nVerifying if MariaDB is configured correctly for CDC..."

# Get values without headers
log_bin=$(docker exec $CONTAINER_NAME mysql -u root -padmin -e "SELECT variable_value FROM information_schema.global_variables WHERE variable_name='log_bin';" --skip-column-names 2>/dev/null)
binlog_format=$(docker exec $CONTAINER_NAME mysql -u root -padmin -e "SELECT variable_value FROM information_schema.global_variables WHERE variable_name='binlog_format';" --skip-column-names 2>/dev/null)
server_id=$(docker exec $CONTAINER_NAME mysql -u root -padmin -e "SELECT variable_value FROM information_schema.global_variables WHERE variable_name='server_id';" --skip-column-names 2>/dev/null)

echo -e "\nValues detected:"
echo "log_bin = $log_bin"
echo "binlog_format = $binlog_format"
echo "server_id = $server_id"

if [[ "$log_bin" == *"ON"* ]] && [[ "$binlog_format" == *"ROW"* ]] && [[ "$server_id" != "0" ]]; then
  echo -e "\n✅ MariaDB is correctly configured for Debezium CDC!"
else
  echo -e "\n❌ MariaDB is NOT correctly configured for Debezium CDC."
  echo "Please update the MariaDB configuration."
  
  echo -e "\nTo fix this, you need to add these settings to your MariaDB configuration:"
  echo "log_bin = mysql-bin"
  echo "binlog_format = ROW"
  echo "server_id = 1"
  echo "binlog_row_image = FULL"
  
  echo -e "\nFor Docker deployment, you may need to modify the docker-compose.yml"
  echo "file for the MariaDB service by adding these command line arguments."
  
  echo -e "\nAlso ensure that your container is properly restarted after making changes."
fi

# Wait for user input before exiting
echo -e "\nPress any key to continue..."
read -n 1 -s -r