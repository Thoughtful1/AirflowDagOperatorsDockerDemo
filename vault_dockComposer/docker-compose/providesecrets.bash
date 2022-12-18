#!/bin/bash
#Echo Hello, please provide container id for hashicorp vault
#read -r VAULT_DOCKER_ID
#echo $VAULT_DOCKER_ID
#docker exec "-it" "$VAULT_DOCKER_ID" "sh"
vault login "ZyrP7NtNw0hbLUqu7N3IlTdO"
vault secrets enable "-path=airflow" "-version=2" "kv"
vault kv put "airflow/variables/slack_token" "value='TOKEN_PLACEHOLDER"

