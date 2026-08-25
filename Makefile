.PHONY: demo test bench query check-env build-gate

check-env:
	python3 scripts/check_env.py

build-gate:
	cd src/gate && go build ./...

demo: build-gate
	docker compose up -d
	python3 scripts/bootstrap.py
	python3 src/data_gen.py --out data/events.jsonl --count 100000 --retry-rate 0.08
	@echo "Start the gate in another terminal: GIN_MODE=release src/gate/gate"
	@echo "Then run: python3 src/replay.py --in data/events.jsonl (see BUILD_GUIDE step 3)"

test: build-gate
	pytest tests/ -v

bench:
	python3 src/bench.py --out benchmarks/results.json

query:
	python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://txn-curated/txn_events/**/*.parquet', 'txn_curated'); \
	print(con.execute(open('sql/daily_settlement.sql').read()).fetchall())"

curate:
	python3 src/curate.py
