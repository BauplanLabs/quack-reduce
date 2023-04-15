include .env

nodejs-init:
	npm install
.PHONY: nodejs-init

serverless-deploy:
	npx serverless deploy
.PHONY: serverless-deploy

python-init:
	python3 -m venv ./.venv && source ./.venv/bin/activate && pip install -r requirements.txt
.PHONY: python-init

run_me_first:
	source ./.venv/bin/activate && python3 run_me_first.py
.PHONY: run_me_first

test:
	source ./.venv/bin/activate && python quack.py
.PHONY: test

test-distinct:
	source ./.venv/bin/activate && python quack.py -q "SELECT pickup_location_id AS location_id, COUNT(*) AS counts FROM read_parquet(['s3://${S3_BUCKET_NAME}/dataset/taxi_2019_04.parquet']) WHERE pickup_at >= '2019-04-01' AND pickup_at < '2019-04-03' GROUP BY 1 ORDER BY 2 DESC"
.PHONY: test-distinct

benchmark:
	source ./.venv/bin/activate && python benchmark.py
.PHONY: benchmark

dbt-run:
	source ./.venv/bin/activate && cd dashboard/dbt && S3_BUCKET_NAME=${S3_BUCKET_NAME} dbt run
.PHONY: dbt-run

dbt-docs:
	source ./.venv/bin/activate && cd dashboard/dbt && S3_BUCKET_NAME=${S3_BUCKET_NAME} dbt docs generate && dbt docs serve
.PHONY: dbt-docs

dashboard:
	source ./.venv/bin/activate && cd dashboard && streamlit run dashboard.py
.PHONY: dashboard