# rucio-analysis

Modular toolkit to perform Rucio stress tests.

Currently supported tests include:
  - test-replication: Iteratively upload files to each RSE add add replication rules to the other RSEs. 

# Production

First, export the root directory path: 

```bash
eng@ubuntu:~$ export RUCIO_ANALYSIS_ROOT=/home/eng/ESCAP/167/rucio-analysis
```

and build the image:

```bash
eng@ubuntu:~/ESCAP/167/rucio-analysis$ make build
```

Commands can be ran directly inside a dockerised environment, e.g.:

```bash
eng@ubuntu:~$ docker run -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -it --name=rucio-analysis rucio-analysis:latest
```

Note that upload tests require a valid X509 certificate to be bound inside the container (as shown above) and will require initialising a `voms-proxy` inside the container:

```bash
[user@b802f5113379 src]$:~$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
```

## Automating tests

Automated testing can be done via cron. It is **not** built in to the image, and must be added.

Cron scripts can be found in `etc/cron`.

After the image is built, start a detached container:

```bash
eng@ubuntu:~$ docker run -d -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -it --name=rucio-analysis rucio-analysis:latest
```

edit the crontab:

```bash
eng@ubuntu:~$ crontab -e
```

and add, e.g.:

```bash
@hourly /opt/rucio-analysis/etc/cron/run-analysis.sh
```

# Development environment

As with the production environment, export the root directory path: 

```bash
eng@ubuntu:~$ export RUCIO_ANALYSIS_ROOT=/home/eng/ESCAP/167/rucio-analysis
```

and build the image:

```bash
eng@ubuntu:~/ESCAP/167/rucio-analysis$ make build
```

Development can then be done dynamically by mounting the source inside a dockerised environment, e.g.:

```bash
eng@ubuntu:~$ docker run --rm -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -v $RUCIO_ANALYSIS_ROOT:/opt/rucio-analysis -it --name=rucio-analysis rucio-analysis:latest
```

##  Example invocation

### Running the script with default tests file, `etc/tests.yml`

```bash
[user@b802f5113379 /]$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
[user@b802f5113379 /]$ cd ~/rucio-analysis/src/
[user@b802f5113379 src]$ python3 run-analysis.py -v -t ../etc/tests.yml
2020-09-16 15:35:34,835 INFO    Parsing tests file
2020-09-16 15:35:34,865 DEBUG   Constructing instance of TestReplication()
2020-09-16 15:35:34,873 INFO    Executing TestReplication.run()
2020-09-16 15:35:34,873 INFO    Checking for dataset (rucio-testing:16-09-2020)
2020-09-16 15:35:35,474 DEBUG   Dataset already exists
2020-09-16 15:35:35,474 INFO    RSE (src): DESY-DCACHE
2020-09-16 15:35:35,474 DEBUG     File size: 15000 bytes
2020-09-16 15:35:35,475 DEBUG       Uploading file 1 of 1
2020-09-16 15:35:36,027 INFO    Preparing upload for file 15KB_160920T15.35.35
2020-09-16 15:35:36,471 INFO    Successfully added replica in Rucio catalogue at DESY-DCACHE
2020-09-16 15:35:36,648 INFO    Successfully added replication rule at DESY-DCACHE
2020-09-16 15:35:37,796 INFO    Trying upload with davs to DESY-DCACHE
2020-09-16 15:35:39,532 INFO    Successfully uploaded file 15KB_160920T15.35.35
2020-09-16 15:35:39,871 DEBUG       Upload complete
2020-09-16 15:35:39,871 DEBUG       Attaching file rucio-testing:15KB_160920T15.35.35 to rucio-testing:16-09-2020
2020-09-16 15:35:40,496 DEBUG       Attached file to dataset
2020-09-16 15:35:40,496 DEBUG       Adding replication rules...
2020-09-16 15:35:40,497 DEBUG       RSE (dst): SARA-DCACHE
2020-09-16 15:35:41,184 DEBUG         Rule ID: 03adf285d91b4bebbdd9c6a351ba3178
2020-09-16 15:35:41,184 DEBUG       Replication rules added
2020-09-16 15:35:41,184 INFO    RSE (src): SARA-DCACHE
2020-09-16 15:35:41,184 DEBUG     File size: 15000 bytes
2020-09-16 15:35:41,184 DEBUG       Uploading file 1 of 1
2020-09-16 15:35:41,720 INFO    Preparing upload for file 15KB_160920T15.35.41
2020-09-16 15:35:42,114 INFO    Successfully added replica in Rucio catalogue at SARA-DCACHE
2020-09-16 15:35:42,312 INFO    Successfully added replication rule at SARA-DCACHE
2020-09-16 15:35:43,144 INFO    Trying upload with root to SARA-DCACHE
2020-09-16 15:35:44,093 INFO    Successfully uploaded file 15KB_160920T15.35.41
2020-09-16 15:35:44,490 DEBUG       Upload complete
2020-09-16 15:35:44,491 DEBUG       Attaching file rucio-testing:15KB_160920T15.35.41 to rucio-testing:16-09-2020
2020-09-16 15:35:45,090 DEBUG       Attached file to dataset
2020-09-16 15:35:45,091 DEBUG       Adding replication rules...
2020-09-16 15:35:45,091 DEBUG       RSE (dst): DESY-DCACHE
2020-09-16 15:35:45,737 DEBUG         Rule ID: 16901d2f4fcc4c1984cd3ed014f94633
2020-09-16 15:35:45,737 DEBUG       Replication rules added
2020-09-16 15:35:45,737 INFO    Finished in 11s
2020-09-16 15:35:45,740 DEBUG   Deconstructing instance of TestReplication()
```

## Tests

### Creating a new test

The procedure for creating a new tests is as follows:

1. Take a copy of the test class stub in `src/tests/stub.py` and rename it. 
2. Add an entry to `etc/tests.yml` with `module_name` (including `tests.` prefix) and `class_name` set accordingly. To inject parameters into the test's entry point, `run()`, assign them in the `args` and `kwargs` keys. Note that the `description`, `module_name`, `class_name`, `enabled`, `args` and `kwargs` keys **must** all be set. 
3. Amend the `run()` function as desired.

The stub function, `src/stub.py` and corresponding entry (test-stub) in `etc/tests.yml` illustrate this usage.


