#!/usr/bin/env bash
# Хвост прогонов одной цепочкой (после основного exp_b.py): повтор прерванного прогона, PromQL, опыт C, проверки.
set -u
cd "$(dirname "$0")/.."
export PYTHONUTF8=1
PY=.venv/Scripts/python.exe
until [ -f results/exp-b/router-skewed-run3/meta.json ]; do sleep 20; done
sleep 5
$PY experiments/exp_b.py --policies router --profiles skewed --runs 1 >> results/exp-b.log 2>&1
$PY experiments/promql_compare.py > results/promql-compare.log 2>&1
for s in hpa hot alerts; do $PY experiments/exp_c.py $s >> results/exp-c.log 2>&1; done
$PY experiments/run_checks.py > results/checks.log 2>&1
echo ALL-DONE >> results/exp-c.log
