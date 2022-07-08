# Makefile for extending rucio client images to include rucio-analysis.
.ONESHELL:

rucio:
	@docker build . -f Dockerfile --build-arg BASE_RUCIO_CLIENT_IMAGE=rucio/rucio-clients \
	--build-arg BASE_RUCIO_CLIENT_TAG=latest --tag rucio-analysis:rucio

skao:
	@docker build . -f Dockerfile \
	--build-arg BASE_RUCIO_CLIENT_IMAGE=registry.gitlab.com/ska-telescope/src/ska-rucio-client \
	--build-arg BASE_RUCIO_CLIENT_TAG=release-1.28.0 --tag rucio-analysis:skao
