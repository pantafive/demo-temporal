.PHONY: up down logs reset test lint

## Bring all services up in the background
up:
	docker compose up --build -d

## Stop all services
down:
	docker compose down

## Tail logs for all services (Ctrl-C to stop)
logs:
	docker compose logs -f

## Destroy containers, networks, and volumes (full reset)
reset:
	docker compose down -v --remove-orphans

## Run the test suite
test:
	uv run pytest

## Run all pre-commit hooks against every file
lint:
	uv run pre-commit run --all-files
