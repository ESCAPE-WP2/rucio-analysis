.ONESHELL:

build-escape-on-escape-rucio-analysis:
	ansible-playbook -i hosts/inventory.ini -e \
	'{"HOSTS": "escape", "RUCIO_VOMS": "escape", "RUCIO_ANALYSIS_MAKE_TARGET": "escape", "RUCIO_ANALYSIS_IMAGE_TAG": "escape", "RUCIO_ANALYSIS_CRONTAB": "vars/crontabs/escape/escape-rucio-analysis.yml"}' \
	install.yml 

build-escape-on-local:
	ansible-playbook -i hosts/inventory.ini -e \
	'{"HOSTS": "localhost", "RUCIO_VOMS": "escape", "RUCIO_ANALYSIS_MAKE_TARGET": "image-escape", "RUCIO_ANALYSIS_IMAGE_TAG": "escape", "RUCIO_ANALYSIS_CRONTAB": "vars/crontabs/escape/escape-rucio-analysis.yml"}' \
	install.yml 