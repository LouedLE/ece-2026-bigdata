#!/bin/bash
# Lab 09.2, exercise 2: catalog role silver_reader, limited to the silver namespace.
# silver_analyst (principal) -> silver_analyst_role (principal role) -> silver_reader (catalog role)
set -u
POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
CATALOG="${POLARIS_CATALOG:-$KUBERNETES_NAMESPACE}"
API="$POLARIS_URI/api/management/v1"
CRED_FILE=/home/onyxia/work/.polaris-credentials.env

TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
call(){ # method url [json]
  curl -s -o /tmp/polaris_resp.json -w '%{http_code}' -X "$1" "$2" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" ${3:+-d "$3"}
}

echo "catalog role silver_reader: $(call POST "$API/catalogs/$CATALOG/catalog-roles" \
  '{"catalogRole":{"name":"silver_reader","properties":{}}}')"
for p in TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA NAMESPACE_READ_PROPERTIES; do
  echo "grant $p on namespace silver: $(call PUT "$API/catalogs/$CATALOG/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$p\"}}")"
done

echo "principal role silver_analyst_role: $(call POST "$API/principal-roles" \
  '{"principalRole":{"name":"silver_analyst_role","properties":{}}}')"
echo "catalog role -> principal role: $(call PUT "$API/principal-roles/silver_analyst_role/catalog-roles/$CATALOG" \
  '{"catalogRole":{"name":"silver_reader"}}')"

status=$(call POST "$API/principals" \
  '{"principal":{"name":"silver_analyst","properties":{}},"credentialRotationRequired":false}')
echo "principal silver_analyst: $status"
if [ "$status" != "201" ]; then
  status=$(call POST "$API/principals/silver_analyst/reset")
  echo "credentials reset: $status"
fi
cred=$(jq -r '"\(.credentials.clientId):\(.credentials.clientSecret)"' /tmp/polaris_resp.json)
rm -f /tmp/polaris_resp.json
grep -v '^POLARIS_SILVER_ANALYST_CREDENTIAL=' "$CRED_FILE" > "$CRED_FILE.new"
echo "POLARIS_SILVER_ANALYST_CREDENTIAL=\"$cred\"" >> "$CRED_FILE.new"
mv "$CRED_FILE.new" "$CRED_FILE"; chmod 600 "$CRED_FILE"
echo "principal role -> principal: $(call PUT "$API/principals/silver_analyst/principal-roles" \
  '{"principalRole":{"name":"silver_analyst_role"}}')"

echo "grants of silver_reader:"
curl -s "$API/catalogs/$CATALOG/catalog-roles/silver_reader/grants" -H "Authorization: Bearer $TOKEN" \
  | jq -c '.grants[] | {type, namespace, privilege}'
echo "roles of silver_analyst:"
curl -s "$API/principals/silver_analyst/principal-roles" -H "Authorization: Bearer $TOKEN" | jq -c '[.roles[].name]'
