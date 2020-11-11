# rucio-analysis

Modular toolkit to perform tasks on a Rucio datalake.

# Production deployment

First, export the root directory path: 

```bash
eng@ubuntu:~$ export RUCIO_ANALYSIS_ROOT=/home/eng/ESCAP/167/rucio-analysis
```

and build the image:

```bash
eng@ubuntu:~/ESCAP/167/rucio-analysis$ make latest
```

or pull the latest from the DockerHub projectescape org:

```bash
eng@ubuntu:~$ docker pull projectescape/rucio-analysis:latest
eng@ubuntu:~$ docker tag projectescape/rucio-analysis:latest rucio-analysis:latest
```

Commands can be ran directly inside a dockerised environment, e.g.:

```bash
eng@ubuntu:~$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -it --name=rucio-analysis rucio-analysis:latest
```

Note that upload tasks require a valid X509 certificate to be bound inside the container (as shown above) and will require initialising a `voms-proxy` inside the container:

```bash
[user@b802f5113379 src]$:~$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
```

## Automating tasks

To keep the container single purpose & minimal, automation should be invoked via cron on the host. An example production crontab is `etc/cron/crontab`. It can be installed by:

```bash
eng@ubuntu:~$ etc/install-crontab.sh
``` 

:warning: this will overwrite the existing crontab!

Jobs in the crontab should call `docker run` on the `rucio-analysis` image, passing in scripts from `etc/cron/jobs`. These scripts are designed to be ran inside the dockerised environment.

# Development environment

As with the production environment, export the root directory path: 

```bash
eng@ubuntu:~$ export RUCIO_ANALYSIS_ROOT=/home/eng/ESCAP/167/rucio-analysis
```

and build the image:

```bash
eng@ubuntu:~/ESCAP/167/rucio-analysis$ make latest
```

Development can then be done dynamically by mounting the source inside a dockerised environment, e.g.:

```bash
eng@ubuntu:~$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -v $RUCIO_ANALYSIS_ROOT:/opt/rucio-analysis -it --name=rucio-analysis rucio-analysis:latest
```

##  Example invocation

### Running the script with stub test tasks file, `etc/tests.stub.yml`

```bash
[user@b802f5113379 /]$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
[user@b802f5113379 /]$ cd ~/rucio-analysis
[root@b802f5113379 rucio-analysis]$ python3 src/run-analysis.py -t etc/tests.stubs.yml 
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

## Tasks

### Creating a new task

The procedure for creating a new tests is as follows:

1. Take a copy of the `TestStubHelloWorld` class stub in `src/tasks/stubs.py` and rename it. 
2. Create a new task definition file e.g. `etc/tests.yml` copying the format in `etc/tests.stubs.yml` with `module_name` (including `tasks.` prefix) and `class_name` set accordingly. To inject parameters into the task's entry point, `run()`, assign them in the `args` and `kwargs` keys. Note that the `description`, `module_name`, `class_name`, `enabled`, `args` and `kwargs` keys **must** all be set. 
3. Amend the `run()` function as desired.

The stub function, `src/tasks/stubs.py` and corresponding definitions in `etc/tests.stubs.yml` illustrate usage.



