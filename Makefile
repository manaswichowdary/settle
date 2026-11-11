.PHONY: up down fresh test crash check logs

# Build the image and start Postgres, Kafka, the API and 3 workers
up:
	docker compose up -d --build

down:
	docker compose down -v

# Start again from an empty database
fresh: down up

# Unit tests plus the full flow against real Postgres and Kafka (Testcontainers, needs Docker)
test:
	mvn -B test

# Send payments while killing workers and the API, then check nothing was lost or applied twice
crash:
	python3 scripts/crash_test.py --payments $(or $(N),20000)

check:
	curl -s localhost:8080/admin/check | python3 -m json.tool

logs:
	docker compose logs -f --tail=50 api worker
