.ONESHELL:

escape:
	@docker build . -f Dockerfile --build-arg BASEIMAGE=projectescape/rucio-client --build-arg BASETAG=latest --pull --no-cache --tag rucio-analysis:escape

rucio:
	@docker build . -f Dockerfile --build-arg BASEIMAGE=rucio/rucio-clients --build-arg BASETAG=latest --pull --no-cache --tag rucio-analysis:rucio

skao:
	@docker build . -f Dockerfile --build-arg BASEIMAGE=registry.gitlab.com/ska-telescope/src/ska-rucio-client --build-arg BASETAG=release-1.28.0 --no-cache --tag rucio-analysis:skao
