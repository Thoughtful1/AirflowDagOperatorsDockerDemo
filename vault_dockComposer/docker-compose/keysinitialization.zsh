#!/bin/zsh
echo "provide container id"
read -r DOCK_CONTAINER_ID
echo "$DOCK_CONTAINER_ID"
docker exec "-i" "$DOCK_CONTAINER_ID" "sh" < "providesecrets.bash"

