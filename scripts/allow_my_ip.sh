#!/usr/bin/env bash
# Re-open Redshift to whatever public IP this machine currently has.
#
# The workgroup is publicly accessible with the security group allowing a single /32,
# so a change of network (or an ISP lease renewal) makes dbt hang on connect with no
# useful error. Run this when that happens.

set -euo pipefail

SG_ID="sg-0b9a3d447705d2593"
PORT=5439

MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "current IP: $MY_IP"

# Drop every rule on this port, then add the current one, so stale entries do not pile up.
aws ec2 describe-security-groups --group-ids "$SG_ID" \
  --query "SecurityGroups[0].IpPermissions[?FromPort==\`$PORT\`].IpRanges[].CidrIp" \
  --output text | tr '\t' '\n' | while read -r cidr; do
    [ -z "$cidr" ] && continue
    echo "  revoking $cidr"
    aws ec2 revoke-security-group-ingress \
      --group-id "$SG_ID" --protocol tcp --port "$PORT" --cidr "$cidr"
  done

aws ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" --protocol tcp --port "$PORT" --cidr "${MY_IP}/32"

echo "allowed ${MY_IP}/32 on port $PORT"
