import datetime
import dateparser
from kubernetes import client, config
import uuid

from common.es.wrappers import Wrappers as ESWrappers
from tasks.task import Task


class ProbesDaemons(Task):
    """ Get information for daemons. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()

        try:
            kubeConfigPath = kwargs["kube_config_path"]
            daemonLikeNames = kwargs["daemon_like_names"]
            namespace = kwargs['namespace']
            databases = kwargs["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        config.load_kube_config(config_file=kubeConfigPath)
        v1 = client.CoreV1Api()

        pods = v1.list_namespaced_pod(namespace=namespace)
        for pod in pods.items:
            if any(likeName in pod.metadata.name for likeName in daemonLikeNames):
                likeName = next((likeName for likeName in daemonLikeNames if likeName in pod.metadata.name), None)

                status = v1.read_namespaced_pod_status(namespace=namespace, name=pod.metadata.name)

                log = v1.read_namespaced_pod_log(namespace=namespace, name=pod.metadata.name, tail_lines=1)  
                logDate = dateparser.parse(log.split('\t')[0].strip().replace(',', '.'), settings={'TIMEZONE': 'UTC'})
                logMessage = log.split('\t')[4].strip() 

                # Push corresponding logs to database
                if databases is not None:
                    for database in databases:
                        if database["type"] == "es":
                            self.logger.debug("Injecting information into ES database...")
                            es = ESWrappers(database["uri"], self.logger)
                            es._index(
                                index=database["index"],
                                documentID=likeName,
                                body={
                                    '@timestamp': int(datetime.datetime.now().strftime("%s"))*1000,
                                    'pod_name': pod.metadata.name,
                                    'daemon_like_name': likeName,
                                    'pod_phase': status.status.phase,
                                    'pod_phase_bool': 1 if status.status.phase == 'Running' else 0,
                                    'pod_start_time': status.status.start_time,
                                    'pod_uptime': (datetime.datetime.utcnow()-status.status.start_time.replace(tzinfo=None)).total_seconds(),
                                    'last_log_time_UTC': logDate,
                                    'last_log_message': logMessage,
                                    'seconds_since_last_message': (datetime.datetime.utcnow()-logDate).total_seconds()
                                }
                            )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
