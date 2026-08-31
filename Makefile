SHELL := /bin/bash
.PHONY: demo demo-full test e2e bench query check-env build-gate curate inspect outbox scale-bench scale-bench-logical

ENV := set -a && source ./env.sh --quiet && set +a

DEMO_COUNT ?= 200
DEMO_FULL_COUNT ?= 100000

check-env:
	$(ENV) && python3 scripts/check_env.py

build-gate:
	cd src/gate && go build ./...

inspect:
	$(ENV) && python3 scripts/aws_inspect.py all

# Not run automatically by `demo` — the RUNBOOK exercise deliberately
# lets PENDING outbox rows accumulate first so the pattern is visible.
outbox:
	$(ENV) && python3 src/outbox_publisher.py

# Small scale — for learning. Gate is always cleaned up (Problem 2).
demo: build-gate
	$(ENV) && docker compose up -d
	$(ENV) && python3 scripts/bootstrap.py
	$(ENV) && python3 src/data_gen.py --out data/events.jsonl --count $(DEMO_COUNT) --retry-rate 0.08
	$(ENV) && bash scripts/run_with_bg.sh 8080 'GIN_MODE=release src/gate/gate' -- \
		bash -c 'python3 src/publisher.py --in data/events.jsonl && python3 src/consumer.py --idle-timeout 10 && python3 src/statemachine.py'

# README-scale. Regenerates published metrics. ~1h at 100k events.
demo-full: build-gate
	$(MAKE) demo DEMO_COUNT=$(DEMO_FULL_COUNT)

test: build-gate
	$(ENV) && pytest tests/ -v --ignore=tests/test_e2e.py

e2e: build-gate
	$(ENV) && pytest tests/test_e2e.py -v -s

bench:
	$(ENV) && python3 src/bench.py --out benchmarks/results.json

query:
	$(ENV) && python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://txn-curated/txn_events/**/*.parquet', 'txn_curated'); \
	print(con.execute(open('sql/daily_settlement.sql').read()).fetchall())"

curate:
	$(ENV) && python3 src/curate.py

# Not part of demo/e2e — opt-in, ~10-15 min. Measures the real dedup/curate
# path at increasing scale and extrapolates to 1 TB with explicit
# assumptions (see docs/scale-report.md). A literal 1 TB run does not fit
# on this machine (50 GB free disk) — see src/scale_bench.py's module
# docstring for the numbers behind that.
scale-bench:
	$(ENV) && python3 src/scale_bench.py --mode materialized --scales 10000,100000,1000000,10000000

# Billions of rows generated and consumed in-flight, nothing written to
# disk — a different, honest kind of "large" than scale-bench's real I/O.
scale-bench-logical:
	$(ENV) && python3 src/scale_bench.py --mode logical --rows 2000000000
