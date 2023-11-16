install:
	python3.11 -m venv .venv && \
	source .venv/bin/activate && \
	pip install -e .[dev];  \
	echo Install Finished; \
	echo; \
	echo 'Run "source .venv/bin/activate" to activate the virtual enviornment'; \
	echo;

clean:
	rm -rf .venv

lint: 
	black --check flows tests && \
  mypy --non-interactive --install-types flows  

format:
	black flows tests

# Run all tests
test:
	pytest --cov=flows tests

# Run unit tests only
test-unit:
	pytest --cov=flows tests/unit

# Run unit tests in watch mode for development
test-watch:
	ptw --runner "pytest --testmon" tests/unit
