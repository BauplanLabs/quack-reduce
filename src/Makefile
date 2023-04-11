include .env

init:
	npm install
	python3 -m venv ./.venv && source ./.venv/bin/activate && pip install -r requirements.txt
.PHONY: init

sls-deploy:
	npx serverless deploy
.PHONY: sls-deploy

run_me_first:
	source ./.venv/bin/activate && python3 run_me_first.py
.PHONY: run_me_first

test:
	source ./.venv/bin/activate && python quack.py -q "SELECT pickup_location_id AS location_id, COUNT(*) AS counts FROM read_parquet(['s3://${S3_BUCKET_NAME}/dataset/taxi_2019_04.parquet']) WHERE pickup_at >= '2019-04-01' AND pickup_at < '2019-04-03' GROUP BY 1 ORDER BY 2 DESC"
.PHONY: test