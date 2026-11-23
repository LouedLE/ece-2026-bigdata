#!/bin/bash
# Commit the Iceberg table metadata (lab 07, section "Commit the Iceberg table")
set -u
cd /home/onyxia/work/ece-2026-bigdata
B="$KUBERNETES_NAMESPACE"
aws s3 --profile default sync "s3://$B/warehouse/" ./warehouse/ --only-show-errors
grep -qxF "warehouse/**/data/" .gitignore || echo "warehouse/**/data/" >> .gitignore
git add .gitignore ./warehouse/silver/orders/metadata ./warehouse/silver/users/metadata
git commit -q -m "feat(spark-lab): add Iceberg table metadata"
git push -q && git log --oneline -1
git status --short
