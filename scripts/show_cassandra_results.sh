#!/usr/bin/env bash
set -euo pipefail

docker exec -it hw10-cassandra cqlsh -e "SELECT user_id, domain, created_at, page_title FROM wikimedia.page_creations LIMIT 20;"
