# rucio-analysis

Toolkit to perform Rucio tests.

# Development

First, export the root directory path: 

```bash
eng@ubuntu:~$ export RUCIO_ANALYSIS_ROOT=/home/eng/ESCAP/167/rucio-analysis
```

and build the image:

```bash
eng@ubuntu:~/ESCAP/167/rucio-analysis$ make build
```

Development can then be done with the mounted source running inside a dockerised environment, e.g.:

```bash
eng@ubuntu:~$ docker run -e RUCIO_CFG_ACCOUNT=robbarnsley -v /home/eng/.globus/client.crt:/opt/rucio/etc/client.crt -v /home/eng/.globus/client.key:/opt/rucio/etc/client.key -v $RUCIO_ANALYSIS_ROOT:/home/user/rucio-analysis -it --name=rucio-analysis rucio-analysis
```

Uploads require initialising a voms-proxy inside the container first:

```bash
eng@ubuntu:~$ voms-proxy-init --cert /opt/rucio/etc/client.crt --key /opt/rucio/etc/client.key --voms escape
```

e.g. 

```bash
[user@b802f5113379 src]$ python3 run-analysis.py -v
2020-09-10 15:08:26,244 INFO    Parsing configuration file
2020-09-10 15:08:26,277 DEBUG   Constructing instance of TestReplication()
2020-09-10 15:08:26,284 INFO    Executing TestReplication.run()
2020-09-10 15:08:26,284 INFO    Checking for dataset (rucio-testing:09-10-2020)
2020-09-10 15:08:26,957 DEBUG   Dataset already exists
2020-09-10 15:08:26,957 INFO    Starting upload...
2020-09-10 15:08:26,957 INFO    RSE: DESY-DCACHE
2020-09-10 15:08:26,957 DEBUG     File size: 10000 bytes
2020-09-10 15:08:26,958 DEBUG       Uploading file 1 of 1
2020-09-10 15:08:32,733 DEBUG         Attaching file rucio-testing:10KB_90fdaee7dc814548b9319e08095ef509 to rucio-testing:09-10-2020
2020-09-10 15:08:33,361 INFO    Upload complete
2020-09-10 15:08:33,361 INFO    Adding replication rules...
2020-09-10 15:08:33,361 INFO    RSE: DESY-DCACHE
2020-09-10 15:08:34,071 DEBUG     Duplicate rule for rucio-testing:10KB_90fdaee7dc814548b9319e08095ef509 found; Skipping.
2020-09-10 15:08:34,071 INFO    RSE: LAPP-DCACHE
2020-09-10 15:08:34,790 DEBUG     0eca59be861448c0a16cde5a12ba355d
2020-09-10 15:08:34,790 INFO    Replication rules added
2020-09-10 15:08:34,790 INFO    Finished in 9s
2020-09-10 15:08:34,793 DEBUG   Deconstructing instance of TestReplication()
```

## Architecture

To create a new test:

1. Take a copy of the test class stub in `src/tests/stub.py` and rename it. 
2. Add an entry to `etc/tests.yml` with `module_name` (including `tests.` prefix) and `class_name` set accordingly. To inject parameters into the test's entry point, `run()`, assign them in the `args` and `kwargs` keys. Note that the `description`, `module_name`, `class_name`, `enabled`, `args` and `kwargs` keys **must** all be set.
3. Amend the `run()` function as desired.
