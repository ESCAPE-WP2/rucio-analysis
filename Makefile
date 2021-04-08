.ONESHELL:

escape:
	@docker build . -f Dockerfile.escape --pull --no-cache --tag rucio-analysis:escape

ska:
	@docker build . -f Dockerfile.ska --pull --no-cache --tag rucio-analysis:skao
