# Execute the "targets" in this file with `make <target>` e.g. `make help`.
#
# You can also run multiple in sequence, e.g. `make clean lint test serve-coverage-report`
#
# See run.sh for more in-depth comments on what each target does.
#
# Note: pyproject.toml is in the root directory (../pyproject.toml)
#       Run make commands from this directory (api_layer/)

.PHONY: build run-dev clean help install lint lint-ci publish-prod publish-test release-prod release-test serve-coverage-report test-ci test-quick test test-wheel-locally generate-openapi generate-openapi-dev generate-openapi-uat generate-openapi-prod generate-openapi-all

build:
	bash run.sh build

run-dev:
	bash run.sh run

clean:
	bash run.sh clean

help:
	bash run.sh help

install:
	bash run.sh install

lint:
	bash run.sh lint

lint-ci:
	bash run.sh lint:ci

publish-prod:
	bash run.sh publish:prod

publish-test:
	bash run.sh publish:test

release-prod:
	bash run.sh release:prod

release-test:
	bash run.sh release:test

serve-coverage-report:
	bash run.sh serve-coverage-report

test-ci:
	bash run.sh test:ci

test-quick:
	bash run.sh test:quick

test:
	bash run.sh run-tests

test-wheel-locally:
	bash run.sh test:wheel-locally

generate-openapi:
	bash run.sh generate-openapi

generate-openapi-dev:
	bash run.sh generate-openapi:dev

generate-openapi-uat:
	bash run.sh generate-openapi:uat

generate-openapi-prod:
	bash run.sh generate-openapi:prod

generate-openapi-all:
	bash run.sh generate-openapi:all
