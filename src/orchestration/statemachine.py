#!/usr/bin/env python3
"""
Deploys the Lambdas + Step Functions state machines from asl/, and
drives the daily job: pre-flight execution -> PySpark curate.py -> post-
flight execution. This is the Python "driver" pattern real Step
Functions pipelines use around Spark compute — SF orchestrates and the
Lambda payload never contains the actual Spark job (see asl/postflight.json).
"""

import argparse
import ast
import json
import subprocess
import sys
import time
import uuid
import zipfile
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from iam_setup import ROLES as IAM_ROLES
from iam_setup import ensure_role  # noqa: E402

from utils import aws  # noqa: E402

LAMBDAS_DIR = Path(__file__).resolve().parent / "lambdas"
ASL_DIR = Path(__file__).resolve().parents[2] / "asl"

# Role per function/machine, least-privilege (see iam/*.json + scripts/iam_setup.py).
# MiniStack doesn't enforce these on live calls, but they're real roles with
# real policies attached — not a placeholder ARN.
FUNCTION_ROLES = {
    "txn-preflight": "arn:aws:iam::000000000000:role/txn-preflight-role",
    "txn-record-status": "arn:aws:iam::000000000000:role/txn-record-status-role",
}
SFN_ROLE_ARN = "arn:aws:iam::000000000000:role/txn-sfn-execution-role"

FUNCTIONS = {
    "txn-preflight": "preflight.py",
    "txn-record-status": "record_status.py",
}
STATE_MACHINES = {
    "txn-daily-preflight": "preflight.json",
    "txn-daily-postflight": "postflight.json",
}


def _zip_handler(file_name: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.write(LAMBDAS_DIR / file_name, arcname=file_name)
    return buf.getvalue()


def deploy_iam_roles(iam) -> None:
    for role_name, (trust_file, policy_file) in IAM_ROLES.items():
        ensure_role(iam, role_name, trust_file, policy_file)


def deploy_lambdas(lam) -> None:
    for fn_name, file_name in FUNCTIONS.items():
        zip_bytes = _zip_handler(file_name)
        handler = f"{file_name[:-3]}.handler"
        existing = lam.list_functions().get("Functions", [])
        if any(f["FunctionName"] == fn_name for f in existing):
            lam.update_function_code(FunctionName=fn_name, ZipFile=zip_bytes)
            print(f"  updated Lambda: {fn_name}")
        else:
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=FUNCTION_ROLES[fn_name],
                Handler=handler,
                Code={"ZipFile": zip_bytes},
            )
            print(f"  created Lambda: {fn_name}")
        _wait_active(lam, fn_name)


def _wait_active(lam, fn_name: str, timeout_s: float = 20) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        cfg = lam.get_function(FunctionName=fn_name)["Configuration"]
        if cfg["State"] == "Active":
            return
        time.sleep(0.5)
    raise TimeoutError(f"Lambda {fn_name} did not become Active in time")


def deploy_state_machines(sfn) -> dict:
    arns = {}
    existing = {
        sm["name"]: sm["stateMachineArn"] for sm in sfn.list_state_machines()["stateMachines"]
    }
    for sm_name, asl_file in STATE_MACHINES.items():
        definition = (ASL_DIR / asl_file).read_text()
        if sm_name in existing:
            sfn.update_state_machine(stateMachineArn=existing[sm_name], definition=definition)
            arns[sm_name] = existing[sm_name]
            print(f"  updated state machine: {sm_name}")
        else:
            resp = sfn.create_state_machine(
                name=sm_name, definition=definition, roleArn=SFN_ROLE_ARN
            )
            arns[sm_name] = resp["stateMachineArn"]
            print(f"  created state machine: {sm_name}")
    return arns


def run_execution(sfn, state_machine_arn: str, input_dict: dict, timeout_s: float = 30) -> dict:
    exec_resp = sfn.start_execution(stateMachineArn=state_machine_arn, input=json.dumps(input_dict))
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
        if desc["status"] != "RUNNING":
            return desc
        time.sleep(0.5)
    raise TimeoutError(f"execution {exec_resp['executionArn']} did not finish in time")


def run_daily_job() -> dict:
    iam = aws.client("iam")
    lam = aws.client("lambda")
    sfn = aws.client("stepfunctions")

    print("Deploying IAM roles:")
    deploy_iam_roles(iam)
    print("Deploying Lambdas:")
    deploy_lambdas(lam)
    print("Deploying state machines:")
    arns = deploy_state_machines(sfn)

    job_id = str(uuid.uuid4())

    print(f"Running pre-flight (job_id={job_id})...")
    preflight_result = run_execution(sfn, arns["txn-daily-preflight"], {"job_id": job_id})
    if preflight_result["status"] != "SUCCEEDED":
        return {"job_id": job_id, "status": "preflight_failed", "detail": preflight_result}

    output = json.loads(preflight_result.get("output", "{}"))
    # RecordStarted's Lambda output has "recorded": True — if the Choice
    # state instead routed to NoDataToCurate (a bare Succeed), the
    # execution output is just the untouched input, with no "recorded" key.
    if not output.get("recorded"):
        return {"job_id": job_id, "status": "no_data_to_curate"}

    print("Running PySpark curate step (outside Lambda — see module docstring)...")
    curate_proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve().parents[1] / "transformation" / "curate.py")],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if curate_proc.returncode != 0:
        return {"job_id": job_id, "status": "curate_failed", "stderr": curate_proc.stderr[-2000:]}

    # curate.py prints "curated: {...}" — parse the stats dict back out.
    stats_line = next(
        (l for l in curate_proc.stdout.splitlines() if l.startswith("curated: ")), None
    )
    stats = ast.literal_eval(stats_line[len("curated: ") :]) if stats_line else {}

    print("Running post-flight...")
    postflight_result = run_execution(
        sfn,
        arns["txn-daily-postflight"],
        {
            "job_id": job_id,
            "curated_row_count": stats.get("rows_out", 0),
            "duplicates_dropped": stats.get("duplicates_dropped", 0),
        },
    )

    return {
        "job_id": job_id,
        "status": "completed"
        if postflight_result["status"] == "SUCCEEDED"
        else "postflight_failed",
        "curate_stats": stats,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deploy-only", action="store_true")
    args = parser.parse_args()

    if args.deploy_only:
        iam = aws.client("iam")
        lam = aws.client("lambda")
        sfn = aws.client("stepfunctions")
        deploy_iam_roles(iam)
        deploy_lambdas(lam)
        deploy_state_machines(sfn)
        return

    result = run_daily_job()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
