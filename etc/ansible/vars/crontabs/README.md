# Crontabs

## Directory Structure and Naming

The structure of `etc/ansible/vars/crontabs` takes the following format: `<org>/<remote_machine_hostname>.yml`

For consistency, `<org>` should be a section in the hosts inventory (`etc/ansible/hosts/inventory.ini`), and the `<remote_machine_hostname>` should be an entry under that section.



