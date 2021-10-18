# rucio-analysis

A modular and extensible framework for performing tasks on a Rucio datalake.

# Architecture

```
  ├── Dockerfile
  ├── etc
  │   ├── ansible
  │   │   ├── deploy.yml
  │   │   ├── hosts
  |   |   └── roles
  │   ├── init
  │   └── tasks
  ├── LICENSE
  ├── Makefile
  ├── README.md
  ├── requirements.txt
  └── src
  |   |── common
  │   ├── daemons
  │   └── tasks

```

Fundamentally, this framework is a task scheduler for Rucio. A "task" is any operation or sequence of operations that can be performed on the datalake.

Within this framework, a task is defined by two parts: the logic and the definition. Task logic should be sufficiently abstracted & parameterised so as to clearly demarcate these two parts, allowing for easy re-use and chaining of tasks.

The source for the task logic is kept in `src/tasks`. The structure of `src/tasks` takes the following format: `<task_type>/<task_name>.yml` where, for consistency, `<task_type>` should be one of:

- misc (miscellaneous)
- sync (syncing functionality, e.g. databases)
- reports (reporting)
- tests (testing)

Other categories may be added as needed.

Task definitions are written in yaml and stored in `etc/tasks`. Each definition contains fields to specify the task logic module to be used and any necessary corresponding arguments. The structure of `etc/tasks` takes the following format: `<project_name>/<task_type>/<task_name>.yml`.

Deployment is managed through Ansible (`etc/ansible`). Hosts eligible for remote deployment are defined in `etc/ansible/hosts/inventory.yml`. 

Roles must be added to `etc/ansible/roles`. Within each role there is a `vars` subdirectory. For remote deployment, a crontab, `crontab.yml`, must exist in this subdirectory.

The recipe for adding a new remote host is described in detail in **Setting up a new remote host**.

## Prerequisites

This framework is designed to be built off a preexisting dockerised Rucio client image. This image could be the de facto standard provided by the Rucio maintainers (https://github.com/rucio/containers/tree/master/clients), included in the root `Makefile` as target "rucio", or an extended image. Client images can be extended to contain the prerequisite certificate bundles, VOMS setup and Rucio template configs for a specific datalake.

Extended images currently exist for the _escape_ and _prototype skao_ datalakes. Builds for other datalake instances can be enabled by adding a new `docker build` routine as a new target in the root `Makefile` with the corresponding build arguments for the base client image and tag. This is a necessary step to make it accessible for deployment via Ansible.

# Usage

## Rucio configuration environment variables

To use the framework, it is first necessary to set a few environment variables. A brief description of each is given below:

- **RUCIO_CFG_ACCOUNT**: the Rucio account under which the tasks are to be performed
- **RUCIO_CFG_AUTH_TYPE**: the authentication type (x509||userpass)

where, if the authentication type is defined as "x509", the following are also required:

- **RUCIO_CFG_CLIENT_CERT**: a valid X.509 certificate file with the necessary permissions on the datalake
- **RUCIO_CFG_CLIENT_KEY**: a valid X.509 key file with the necessary permissions on the datalake

or if the authentication type is defined as "userpass":

- **RUCIO_CFG_USERNAME**: a user with the necessary permissions on the datalake
- **RUCIO_CFG_PASSWORD**: the corresponding password for the user

Note that without X.509 authentication, some operations e.g. upload/delete on Grid managed storage sites will be restricted.

## Using the framework interactively in a dockerised environment

Make the rucio-analysis image for the desired project, e.g. for escape:

```bash
eng@ubuntu:~/rucio-analysis$ make escape
```

An interactive dockerised environment can then be instantiated by overriding the default image entrypoint.

For X.509 authentication with Rucio, you must bind the certificate credentials to a volume inside the container:

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=x509 \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_CLIENT_CERT=/opt/rucio/etc/client.crt \
-e RUCIO_CFG_CLIENT_KEY=/opt/rucio/etc/client.key \
-v $RUCIO_CFG_CLIENT_CERT:/opt/rucio/etc/client.crt \
-v $RUCIO_CFG_CLIENT_KEY:/opt/rucio/etc/client.key \
--name=rucio-analysis --entrypoint /bin/bash \
rucio-analysis:escape
```

For userpass authentication with Rucio, you must specify the corresponding credentials as environment variables:

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=userpass \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_USERNAME=$RUCIO_CFG_USERNAME \
-e RUCIO_CFG_PASSWORD=$RUCIO_CFG_PASSWORD \
--name=rucio-analysis --entrypoint /bin/bash \
rucio-analysis:escape
```

Additionally, for development purposes, it is possible to mount the source from the host directly into the container provided you have exported the project's root directory path as **RUCIO_ANALYSIS_ROOT**:

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=x509 \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_CLIENT_CERT=/opt/rucio/etc/client.crt \
-e RUCIO_CFG_CLIENT_KEY=/opt/rucio/etc/client.key \
-v $RUCIO_CFG_CLIENT_CERT:/opt/rucio/etc/client.crt \
-v $RUCIO_CFG_CLIENT_KEY:/opt/rucio/etc/client.key \
-v $RUCIO_ANALYSIS_ROOT:/opt/rucio-analysis \
--name=rucio-analysis --entrypoint /bin/bash \
rucio-analysis:escape
```

Tasks can then be executed manually inside the container. First, create an X.509 proxy (required for upload to Grid managed storage):

```bash
[user@b802f5113379 rucio-analysis]$:~$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
```

then execute the task:

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

## Running a task in a dockerised environment

To run a task directly in the dockerised environment, do not override the default entrypoint. You must include the the VOMS name (**RUCIO_VOMS**) and task file path from the perspective of the container's filesystem (**TASK_FILE_PATH** ) as additional environment variables in the `docker run` command, e.g.

```bash
eng@ubuntu:~/rucio-analysis$ docker run --rm \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_CLIENT_CERT=/opt/rucio/etc/client.crt \
-e RUCIO_CFG_CLIENT_KEY=/opt/rucio/etc/client.key \
-e RUCIO_VOMS=escape \
-e TASK_FILE_PATH="/opt/rucio-analysis/etc/tasks/escape/tests/upload-and-replication.yml" \
-v $RUCIO_CFG_CLIENT_CERT:/opt/rucio/etc/client.crt \
-v $RUCIO_CFG_CLIENT_KEY:/opt/rucio/etc/client.key \
--name=rucio-analysis \
rucio-analysis:escape
```

# Scheduling tasks

To keep containers single purpose, task scheduling is achieved using a crontab on the remote host machine.

## Setting up a new remote host

This framework contains a means to configure a remote host's crontab using Ansible. The recipe to add a new remote host is as follows:

1.  Add the extended dockerised Rucio client as a new target of the root `Makefile`. This can be done by copying an existing entry and modifying the `$BASEIMAGE` & `$BASETAG` build arguments to point to the desired Rucio client base image, and modifying the `--tag` attribute with some unique project name identifier.

2.  Make a new directory in `etc/ansible/roles` with the remote host name, create a `crontab.yml` in the `vars` subdirectory and populate it like:

        ```yaml
        jobs:
        - name: <task_name>
            minute: "0"
            hour: "*"
            day: "*"
            month: "*"
            weekday: "*"
            task_subpath: "path/to/test"
            disabled: no
        ```

    where `<task_subpath>` is relative to `etc/tasks/`. Logs for a task can be effectively turned off by setting `override_log_path` to "/dev/null". Tasks can be disabled by setting the `disabled` parameter to "no".

3.  Add the remote host to `etc/ansible/hosts/inventory.yml`. This file contains all the necessary variable definitions to build rucio-analysis for the corresponding remote host; this includes the make target (as defined in step 1) to build the extended Rucio client image, the Ansible role (or rather, the name of the remote host as defined in step 2) containing project specific variables such as the crontab to be deployed, and the VOMS used to authenticate with Grid services.

## Deploying to the remote host machine

If the remote host has been specified as above then running the deployment is a case of setting the **RUCIO*CFG*\*** attributes, as described in **usage**, and running the `deploy` playbook, e.g.:

```bash
eng@ubuntu:~/rucio-analysis/etc/ansible$ ansible-playbook -i hosts/inventory.yml deploy.yml -e HOSTS=<remote_host>
```

where `<remote_host>` is set to the remote host name specified in `etc/ansible/hosts/inventory.yml`.

For convenience, this command can be added as a Make target in `etc/ansible/Makefile`, allowing for one-line deployment, e.g.:

```bash
eng@ubuntu:~/rucio-analysis/etc/ansible$ make deploy-to-escape-rucio-analysis
```

# Development

## Creating a new task

The procedure for creating a new tests is as follows:

1. Take a copy of the `TestStubHelloWorld` class stub in `src/tasks/stubs.py` and rename both the file and class name.
2. Amend the entrypoint `run()` function as desired. Functionality for communicating with Rucio either by the CLI or API is provided via the wrapper and helper functions in `rucio/wrappers.py` and `rucio/helpers.py` respectively. Example usage can be found in the `StubRucioAPI` class stub in `src/tasks/stubs.py`.
3. Create a new task definition file e.g. `etc/tasks/test.yml` copying the format of the `test-hello-world-stub` definition in `etc/tasks/stubs.yml`. A task has the following mandatory fields:
   - `module_name` (starting from and including the `tasks.` prefix) and `class_name`, set accordingly to match the modules/classes redefined in step 1,
   - `args` and `kwargs` keys corresponding to the parameters injected into the task's entry point `run()`,
   - `description`, and
   - `enabled`.
