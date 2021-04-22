.ONESHELL:

escape:
	@docker build . -f Dockerfile.escape --pull --no-cache --tag rucio-analysis:escape

skao:
	@docker build . -f Dockerfile.skao --pull --no-cache --tag rucio-analysis:skao
