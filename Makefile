COMPOSE=docker compose

.PHONY: init up down logs ps

init:
	$(COMPOSE) up airflow-init

up:
	$(COMPOSE) up -d postgres redis airflow-webserver airflow-scheduler airflow-worker

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=200
	
test:
	.\venv\Scripts\pytest -q

ps:
	$(COMPOSE) ps
