.PHONY: sim test replay chaos dashboard clean

sim:
	docker compose up --build

test:
	python -m pip install -r requirements.txt
	python -m pytest

replay:
	python -m pip install -r requirements.txt
	python -m pytest tests/test_scenarios.py -q

chaos:
	python -m pip install -r requirements.txt
	python -m pytest tests/test_scenarios.py -k "t4 or t7 or t8 or t11 or t12" -q

dashboard:
	docker compose up --build control-plane redis

clean:
	docker compose down -v
