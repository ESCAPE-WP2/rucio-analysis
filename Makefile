.ONESHELL:

escape:
	@docker build . -f Dockerfile --no-cache --tag rucio-analysis:latest

ska:
	@docker build . -f Dockerfile.ska --no-cache --tag rucio-analysis:ska
