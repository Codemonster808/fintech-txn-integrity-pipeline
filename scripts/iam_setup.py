#!/usr/bin/env python3
"""Create least-privilege IAM roles for P1, idempotently.

    source env.sh
    python3 scripts/iam_setup.py

Creates 3 roles from iam/*.json:
  - txn-preflight-role       (s3:ListBucket on txn-raw only)
  - txn-record-status-role   (dynamodb:PutItem on txn-curation-jobs only)
  - txn-sfn-execution-role   (lambda:InvokeFunction on the 2 gate functions only)

MiniStack's IAM API accepts these calls and stores the policy documents
correctly, but does NOT enforce them on live S3/DynamoDB/Lambda calls
(verified: an assumed role with an explicit Deny * could still call
`aws s3 ls`). What MiniStack *does* evaluate correctly is
`iam simulate-principal-policy` — see docs/RUNBOOK.md for the exercise
that uses it. Print statements here are deliberate: this script is meant
to be read while it runs, not just executed.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from common import aws  # noqa: E402

IAM_DIR = ROOT / "iam"

ROLES = {
    "txn-preflight-role": ("trust-lambda.json", "policy-preflight.json"),
    "txn-record-status-role": ("trust-lambda.json", "policy-record-status.json"),
    "txn-sfn-execution-role": ("trust-states.json", "policy-sfn-execution.json"),
}


def _role_arn(role_name: str) -> str:
    return f"arn:aws:iam::000000000000:role/{role_name}"


def ensure_role(iam, role_name: str, trust_file: str, policy_file: str) -> str:
    trust_doc = (IAM_DIR / trust_file).read_text()
    policy_doc = (IAM_DIR / policy_file).read_text()

    existing = {r["RoleName"] for r in iam.list_roles().get("Roles", [])}
    if role_name in existing:
        print(f"  {role_name}: already exists")
    else:
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust_doc)
        print(f"  {role_name}: created")

    iam.put_role_policy(RoleName=role_name, PolicyName="least-privilege", PolicyDocument=policy_doc)
    print(f"    attached policy: {policy_file}")
    return _role_arn(role_name)


def main() -> None:
    iam = aws.client("iam")
    print("IAM roles (least privilege — see iam/*.json for the exact grants):")
    arns = {}
    for role_name, (trust_file, policy_file) in ROLES.items():
        arns[role_name] = ensure_role(iam, role_name, trust_file, policy_file)
    print()
    print("Role ARNs (used by src/statemachine.py):")
    print(json.dumps(arns, indent=2))
    print()
    print("Reminder: MiniStack does not enforce these on live calls. Try the")
    print("simulate-principal-policy exercise in docs/RUNBOOK.md to see real")
    print("policy evaluation (explicitDeny / implicitDeny / allowed).")


if __name__ == "__main__":
    main()
