.ONESHELL:

build:
	@docker build . -f Dockerfile --tag rucio-analysis:latest
