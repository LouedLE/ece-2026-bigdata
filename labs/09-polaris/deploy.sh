#!/bin/bash
# Lab 09.1: deploy Polaris with Helm. Run from anywhere.
# The principal credentials go to /home/onyxia/work/.polaris-credentials.env, outside the Git repository.
set -u
cd /home/onyxia/work/ece-2026-bigdata
OUT=labs/09-polaris
CRED_FILE=/home/onyxia/work/.polaris-credentials.env
s(){ echo; echo "===== $1 ====="; }

export AWS_CONFIG_FILE=/home/onyxia/work/.aws/config AWS_SHARED_CREDENTIALS_FILE=/home/onyxia/work/.aws/credentials
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"
echo "bucket and catalog: $LAB_BUCKET_NAME"

s "bronze layer"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"

s "chart files"
bash "$OUT/files.sh"
find polaris/deployment -type f | sort

s "configmap and secrets"
kubectl create configmap polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --from-literal=POLARIS_URI="$POLARIS_URI" \
  --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" \
  --from-literal=POLARIS_REALM="POLARIS" \
  --from-literal=POLARIS_CLIENT_ID="root" \
  --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" \
  --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-credential \
  --from-literal=POLARIS_ROOT_SECRET="s3cr3t" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

s "helm"
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
helm dependency update polaris/deployment
helm upgrade --install polaris polaris/deployment --wait --timeout 10m
echo "helm exit code: $?"
kubectl get pods,jobs

s "health"
curl -s "$POLARIS_MGMT_URI/q/health" | jq '{status, checks: [.checks[]? | {name, status}]}'

s "bootstrap job (without credentials)"
kubectl logs job/polaris-bootstrap | grep -v '"credentials"'

s "principal credentials"
kubectl logs job/polaris-bootstrap \
  | grep '"credentials"' \
  | jq -r 'select(.principal == "dev" or .principal == "analyst")
      | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
  > "$CRED_FILE.new"
if [ -s "$CRED_FILE.new" ]; then mv "$CRED_FILE.new" "$CRED_FILE"; else rm -f "$CRED_FILE.new"; fi
chmod 600 "$CRED_FILE"
cut -d= -f1 "$CRED_FILE"

s "advanced: entities created by the bootstrap job"
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
echo "catalogs:"
curl -s "$API/catalogs" -H "Authorization: Bearer $TOKEN" \
  | jq -c '.catalogs[] | {name, type, location: .properties["default-base-location"]}'
echo "principals:"
curl -s "$API/principals" -H "Authorization: Bearer $TOKEN" | jq -c '[.principals[].name]'
echo "principal roles:"
curl -s "$API/principal-roles" -H "Authorization: Bearer $TOKEN" | jq -c '[.roles[].name]'
echo "catalog roles:"
curl -s "$API/catalogs/$POLARIS_CATALOG/catalog-roles" -H "Authorization: Bearer $TOKEN" | jq -c '[.roles[].name]'
for r in catalog_contributor catalog_reader; do
  echo "grants of $r:"
  curl -s "$API/catalogs/$POLARIS_CATALOG/catalog-roles/$r/grants" -H "Authorization: Bearer $TOKEN" \
    | jq -c '[.grants[].privilege]'
done
