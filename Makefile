.PHONY: help pycco

SOURCES:=$(shell find . -name '*.py')
DOCFILES:=$(addprefix docs/packages/, $(addsuffix .html, $(basename ${SOURCES})))

default: tests

docs/packages/%.html: %.py
	mkdir -p $(dir $@)
	pycco -d $(dir $@) $^

pycco: ${DOCFILES}

tests: unit_tests

unit_tests: ## Run unit tests
	pytest -v -p no:cacheprovider

coverage: ## Run unit tests, display code coverage on terminal
	pytest -v -p no:cacheprovider --cov schemas/

coverage-report: ## Run unit tests, generate code coverage as a HTML report
	pytest -v -p no:cacheprovider --cov schemas/ --cov-report=html

documentation: ## Generate documentation for all sources
	pydoc3 *.py > docs/sources.txt

shellcheck: ## Run shellcheck
	shellcheck *.sh

pycodestyle: ## Check code style of all Python sources
	echo "Checking code style of all Python sources"
	find . -type f -name "*.py" | xargs pycodestyle

pydocstyle: ## Check docstrings style of all Python sources
	echo "Checking docstrings style of all Python sources"
	find . -type f -name "*.py" | xargs pydocstyle

pyformat-check: ## Check formatting of all Python sources
	echo "Checking formatting of all Python sources"
	find . -type f -name "*.py" | xargs black --check --diff

pyformat: ## Reformat all Python sources
	echo "Reformat all Python sources"
	find . -type f -name "*.py" | xargs black

help: ## Show this help screen
	@echo 'Usage: make <OPTIONS> ... <TARGETS>'
	@echo ''
	@echo 'Available targets are:'
	@echo ''
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ''
