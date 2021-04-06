# rucio-analysis

A modular toolkit to perform structured tasks on a Rucio datalake.

# Usage

To use the toolkit, it is first required to set a few necessary environment variables. A brief description of each is given below:

- RUCIO_ANALYSIS_ROOT: path to the root of this package
- RUCIO_CFG_ACCOUNT: the rucio account under which the tasks are to be performed
- RUCIO_CFG_X509_CERT_FILE_PATH: a valid X.509 certificate file with the necessary permissions on the datalake
- RUCIO_CFG_X509_KEY_FILE_PATH: a valid X.509 key file with the necessary permissions on the datalake
- RUCIO_VOMS: the VOMS that the user will authenticate against (default: 'escape')
- RUCIO_ANALYSIS_IMAGE_TAG: tag of the dockerised image. This must correspond to a build instruction in the Makefile (default: 'escape').
- RUCIO_ANALYSIS_LOG_PATH: path to cron outputted logs (default: '/var/log/cron/rucio-analysis')

Next, make the rucio-analysis image:

```bash
eng@ubuntu:~/rucio-analysis$ make escape
```

Commands can then be ran directly inside a dockerised environment, e.g.:

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -it --name=rucio-analysis --entrypoint /bin/bash rucio-analysis:escape
```

Or, mounting the package directly into the container (for development purposes):

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -v $RUCIO_ANALYSIS_ROOT:/opt/rucio-analysis -it --name=rucio-analysis --entrypoint /bin/bash rucio-analysis:escape
```

Note that upload tasks require a valid X509 certificate to be bound inside the container (as shown above) and will require initialising a `voms-proxy`:

```bash
[user@b802f5113379 rucio-analysis]$:~$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
```

Tasks can then be executed manually via, e.g.:

```bash
[root@b802f5113379 rucio-analysis]$ python3 src/run-analysis.py -t etc/tasks/stubs.yml 
2020-10-23 08:16:17,039 [root] INFO     9697    Parsing tasks file
2020-10-23 08:16:17,253 [TestStubHelloWorld] INFO       9697    Executing TestStubHelloWorld.run()
2020-10-23 08:16:17,253 [TestStubHelloWorld] INFO       9697    Hello World!
2020-10-23 08:16:17,253 [TestStubHelloWorld] INFO       9697    Finished in 0s
2020-10-23 08:16:17,254 [TestStubRucioAPI] INFO 9697    Executing TestStubRucioAPI.run()
2020-10-23 08:16:17,591 [TestStubRucioAPI] INFO 9697    {'status': 'ACTIVE', 'account': 'robbarnsley', 'account_type': 'SERVICE', 'created_at': '2020-07-16T08:08:13', 'suspended_at': None, 'updated_at': '2020-07-16T08:08:13', 'deleted_at': None, 'email': 'r.barnsley@skatelescope.org'}
2020-10-23 08:16:17,870 [TestStubRucioAPI] INFO 9697    Preparing upload for file 1KB_231020T08.16.17
2020-10-23 08:16:19,312 [TestStubRucioAPI] INFO 9697    Trying upload with gsiftp to EULAKE-1
2020-10-23 08:16:21,779 [TestStubRucioAPI] INFO 9697    Successful upload of temporary file. gsiftp://eulakeftp.cern.ch:2811/eos/eulake/tests/rucio_test/eulake_1/SKA_SKAO_BARNSLEY-testing/4f/d3/1KB_231020T08.16.17.rucio.upload
2020-10-23 08:16:22,016 [TestStubRucioAPI] INFO 9697    Successfully uploaded file 1KB_231020T08.16.17
2020-10-23 08:16:22,602 [TestStubRucioAPI] INFO 9697    Successfully added replica in Rucio catalogue at EULAKE-1
2020-10-23 08:16:22,761 [TestStubRucioAPI] INFO 9697    Successfully added replication rule at EULAKE-1
2020-10-23 08:16:23,267 [TestStubRucioAPI] INFO 9697    Finished in 6s
```

## Automating tasks

To keep containers single purpose, task automation is achieved via cron on the host. To automate a task, the corresponding docker run command must be added to the host's crontab, passing in both the necessary credentials to authenticate with the Rucio server (taken from the environment variables) and the task file path from the perspective of the container.

For simplicity, this crontab can be managed by ansible. To add or amend a job, configure the "loop" parameter in the playbook `etc/install.yml` and run the play:

```bash
eng@ubuntu:~/rucio-analysis$ ansible-playbook etc/install.yml
```

# Tasks

## Creating a new task

The procedure for creating a new tests is as follows:

1. Take a copy of the `TestStubHelloWorld` class stub in `src/tasks/stubs.py` and rename it. 
2. Amend the `run()` function as desired.
3. Create a new task definition file e.g. `etc/tasks/test.yml` copying the format in `etc/tasks/stubs.yml` with `module_name` (including `tasks.` prefix) and `class_name` set accordingly to match that redefined in the previous steps. To inject parameters into the task's entry point, `run()`, assign them in the `args` and `kwargs` keys. Note that the `description`, `module_name`, `class_name`, `enabled`, `args` and `kwargs` keys **must** all be set. 

The stub function, `src/tasks/stubs.py` and corresponding definitions in `etc/tests.stubs.yml` illustrate usage.



