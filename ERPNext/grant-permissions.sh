#!/bin/bash
# Script to grant necessary permissions for Debezium CDC

echo "Granting necessary permissions for Debezium CDC..."

# Find the MariaDB container name
CONTAINER_NAME=$(docker ps --format "{{.Names}}" | grep -E 'db|mariadb|mysql')

if [ -z "$CONTAINER_NAME" ]; then
  echo "Error: Could not find a running MariaDB container."
  echo "Please make sure the ERPNext database container is running."
  exit 1
fi

echo "Using container: $CONTAINER_NAME"

# Execute MySQL commands to grant permissions
echo "Granting REPLICATION permissions to root user..."

docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;
"

# Verify permissions were granted
echo "Verifying permissions..."
docker exec -it $CONTAINER_NAME mysql -u root -padmin -e "SHOW GRANTS FOR 'root'@'%';"

echo "Done! The user 'root' should now have the necessary permissions for Debezium CDC."
echo "If you're still having issues, please check the troubleshooting guide."

# Wait for user input before exiting
echo -e "\nPress any key to continue..."
read -n 1 -s -r