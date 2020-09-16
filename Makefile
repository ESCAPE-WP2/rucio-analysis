.ONESHELL:

build:
	@docker build . -f Dockerfile --tag rucio-analysis:latest

build-dev:
	@docker build . -f Dockerfile.dev --tag rucio-analysis:dev 
