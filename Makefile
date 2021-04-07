.ONESHELL:

escape:
	@docker build . -f Dockerfile.escape --no-cache --tag rucio-analysis:escape

ska:
	@docker build . -f Dockerfile.ska --no-cache --tag rucio-analysis:skao
