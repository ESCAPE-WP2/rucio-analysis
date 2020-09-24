.ONESHELL:

latest:
	@docker build . -f Dockerfile --no-cache --tag rucio-analysis:latest
