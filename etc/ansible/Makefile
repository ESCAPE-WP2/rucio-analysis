.ONESHELL:

deploy-to-skao-rucio-analysis-prod:
	ansible-playbook -i hosts/inventory.yml deploy-to-remote.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao 

deploy-to-skao-rucio-analysis-dev:
	ansible-playbook -i hosts/inventory.yml deploy-to-remote.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao-dev

deploy-to-escape-rucio-analysis:
	ansible-playbook -i hosts/inventory.yml deploy-to-remote.yml -e HOSTS=escape-rucio-analysis -e ansible_role=escape

update-local-skao-rucio-analysis-prod:
	ansible-playbook -i hosts/inventory.yml update-local.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao

update-local-skao-rucio-analysis-dev:
	ansible-playbook -i hosts/inventory.yml update-local.yml -e HOSTS=skao-rucio-analysis -e ansible_role=skao-dev

update-local-skao-rucio-analysis-bulk-dev:
	ansible-playbook -i hosts/inventory.yml update-local.yml -e HOSTS=escape-stress-tests -e ansible_role=escape-stress-tests
