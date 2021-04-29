.ONESHELL:

escape:
	@docker build . -f Dockerfile --build-arg BASEIMAGE=projectescape/rucio-client --build-arg BASETAG=latest --pull --no-cache --tag rucio-analysis:escape

skao:
	@docker build . -f Dockerfile --build-arg BASEIMAGE=registry.gitlab.com/ska-telescope/src/ska-rucio-client --build-arg BASETAG=latest --pull --no-cache --tag rucio-analysis:skao
