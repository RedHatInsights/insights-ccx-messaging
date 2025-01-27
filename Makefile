.PHONY: default pycco tests unit_tests coverage coverate-report documentation shellcheck lint pyformat help

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
	pytest -v -p no:cacheprovider --cov ccx_messaging/

coverage-report: ## Run unit tests, generate code coverage as a HTML report
	pytest -v -p no:cacheprovider --cov ccx_messaging/ --cov-report=html

documentation: ## Generate documentation for all sources
	pydoc3 *.py > docs/sources.txt

shellcheck: ## Run shellcheck
	shellcheck *.sh

lint:
	echo "Checking code style of all Python sources"
	ruff check

pyformat: ## Reformat all Python sources
	echo "Reformat all Python sources"
	ruff format

help: ## Show this help screen
	@echo 'Usage: make <OPTIONS> ... <TARGETS>'
	@echo ''
	@echo 'Available targets are:'
	@echo ''
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ''
