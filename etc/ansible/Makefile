.ONESHELL:

deploy-to-skao-rucio-analysis-prod:
	ansible-playbook -i hosts/inventory.yml deploy-to-remote.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao -e rucio_voms=skatelescope.eu -e rucio_analysis_image_make_target=skao

deploy-to-skao-rucio-analysis-dev:
	ansible-playbook -i hosts/inventory.yml deploy-to-remote.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao-dev -e rucio_voms=skatelescope.eu -e rucio_analysis_image_make_target=skao

deploy-to-escape-rucio-analysis:
	ansible-playbook -i hosts/inventory.yml deploy-to-remote.yml -e HOSTS=escape-rucio-analysis -e ansible_role=escape -e rucio_voms=escape -e rucio_analysis_image_make_target=escape

update-local-skao-rucio-analysis-prod:
	ansible-playbook -i hosts/inventory.yml update-local.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao -e rucio_voms=skatelescope.eu -e rucio_analysis_image_make_target=skao

update-local-skao-rucio-analysis-dev:
	ansible-playbook -i hosts/inventory.yml update-local.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao-dev -e rucio_voms=skatelescope.eu -e rucio_analysis_image_make_target=skao

