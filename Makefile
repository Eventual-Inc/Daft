init:
	poetry install
	git config core.hooksPath .githooks

test:
	poetry run pytest
