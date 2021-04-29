# rucio-analysis

A modular and extensible framework for performing tasks on a Rucio datalake.

# Structure

```
  ├── Dockerfile
  ├── etc
  │   ├── ansible
  │   ├── init
  │   └── tasks
  ├── LICENSE
  ├── Makefile
  ├── README.md
  ├── requirements.txt
  └── src
  │   ├── es
  │   ├── rucio
  │   ├── slack
  │   └── tasks
 
```

This framework interacts with a Rucio datalake by extending a preexisting containerised Rucio client image, adding in the necessary build instructions for this package. This preexisting image should contain the prerequisite certificate bundles, VOMS setup and Rucio template configs for the datalake. Builds for other datalake instances can be enabled by adding a new `docker build` routine to a new target in the `Makefile` with the corresponding build arguments for the base image and tag. This is a necessary step to make it accessible to the Ansible install script.

Both local and remote install is managed by Ansible. This is discussed further in **Automation**.

Task definitions are written as yaml and stored in `etc/tasks`.

The source containing the logic used by the tasks is kept in `src/tasks`.

## Creating a new task

The procedure for creating a new tests is as follows:

1. Take a copy of the `TestStubHelloWorld` class stub in `src/tasks/stubs.py` and rename both the file and class name as desired.
2. Amend the entrypoint `run()` function as desired. Functionality for communicating with Rucio either by the CLI or API is provided via the wrapper and helper functions in `rucio/wrappers.py` and `rucio/helpers.py` respectively. Example usage can be found in the `StubRucioAPI` class stub in `src/tasks/stubs.py`.
3. Create a new task definition file e.g. `etc/tasks/test.yml` copying the format of the `test-hello-world-stub` definition in `etc/tasks/stubs.yml`. A task is defined by the fields:
    - `module_name` (starting from and including the `tasks.` prefix) and `class_name`, set accordingly to match the modules/classes redefined in step 1,
    - `args` and `kwargs` keys corresponding to the parameters injected into the task's entry point `run()`,
    - `description`, and 
    - `enabled`.

# Usage

To use the framework, it is first necessary to set a few environment variables. A brief description of each is given below:

- **RUCIO_CFG_ACCOUNT**: the rucio account under which the tasks are to be performed
- **RUCIO_CFG_CLIENT_CERT**: a valid X.509 certificate file with the necessary permissions on the datalake
- **RUCIO_CFG_CLIENT_KEY**: a valid X.509 key file with the necessary permissions on the datalake

Next, make the rucio-analysis image, e.g.:

```bash
eng@ubuntu:~/rucio-analysis$ make escape
```

An interactive dockerised environment can then be instantiated by overriding the default image entrypoint:

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v $RUCIO_CFG_CLIENT_CERT:/opt/rucio/etc/client.crt -v $RUCIO_CFG_CLIENT_KEY:/opt/rucio/etc/client.key -it --name=rucio-analysis --entrypoint /bin/bash rucio-analysis:escape
```

Or, mounting the source from the host directly into the container (for development purposes):

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -v $RUCIO_ANALYSIS_ROOT:/opt/rucio-analysis -it --name=rucio-analysis --entrypoint /bin/bash rucio-analysis:escape
```

Note that upload tasks require a valid X.509 certificate to be bound inside the container (as shown above) and will require initialising a `voms-proxy`:

```bash
[user@b802f5113379 rucio-analysis]$:~$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
```

Tasks can then be executed manually inside the container via, e.g.:

```bash
[root@b802f5113379 rucio-analysis]$ python3 src/run.py -t etc/tasks/stubs.yml 
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

# Automation

To keep containers single purpose, task automation is achieved via cron on the host. To automate a task, the corresponding docker run command must be added to the host's crontab, passing in both the necessary credentials to authenticate with the Rucio server (taken from the environment variables) and the task file path from the perspective of the container.

To keep deployment as simple as possible, this crontab can (and should) be managed by Ansible. 

## Crontabs

To add or amend cronned tasks, create/edit the corresponding `jobs` entry in `crontab.yml` in `etc/ansible/vars/<org>`. The fields in this yaml file follow standard crontab nomenclature, and look something like:

```yaml
jobs:
- name: tests-transfers
    minute: "0"
    hour: "*"
    day: "*"
    month: "*"
    weekday: "*"
    task_subpath: "escape/tests/upload-and-replication.yml"
    disabled: yes
```

where `<task_subpath>` is relative to `etc/tasks/`. Logs for each task can be effectively turned off by setting `override_log_path` to "/dev/null". Tasks can be disabled by setting the `disabled` parameter to "no".

## Defining remote hosts

Deployment via Ansible requires that a host target has been defined in `etc/hosts/inventory.ini`.

## Running the play

In addition to the `RUCIO_CFG_*` environment variables discussed in **Usage**, the call to the Ansible install script should include the additional parameters (if different from default):

- **RUCIO_ANALYSIS_HOSTS**: a host target, as defined in `etc/ansible/hosts/inventory.ini` (default: 'localhost')
- **RUCIO_VOMS**: the VOMS that the user will authenticate against (default: 'escape')
- **RUCIO_ANALYSIS_IMAGE_MAKE_TARGET**: the Make target for the rucio-analysis image, as defined in `Makefile` (default: 'escape')
- **RUCIO_ANALYSIS_IMAGE_TAG**: tag of the dockerised image. This must correspond to a build instruction in the Makefile (default: 'escape').

This call can be added as a Make target in `etc/ansible/Makefile` for convenience, allowing for one-line deployment, e.g.:

```bash
eng@ubuntu:~/rucio-analysis/etc/ansible$ make build-escape-on-local
```

for local, or:

```bash
eng@ubuntu:~/rucio-analysis/etc/ansible$ make build-escape-on-escape-rucio-analysis
```

for remote.
