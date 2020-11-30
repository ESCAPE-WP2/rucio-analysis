import argparse
from datetime import datetime
import os
from subprocess import Popen, PIPE

from crontab import CronTab
from slack import RTMClient

#from db import ES
from logger import Logger
#from rucio_wrappers import RucioWrappersAPI


if __name__ == "__main__":
    parser = argparse.ArgumentParser()  
    parser.add_argument('-dt', help="Database type (ES)", default='es', action='store')
    parser.add_argument('-du', help="Database URI", default='http://130.246.214.144:80/monit/metadata/', action='store')
    parser.add_argument('-di', help="Database index", default='[replication]', action='store')
    parser.add_argument('-dslte', help="Database search less than", default='now', action='store')
    parser.add_argument('-dsgte', help="Database search greater than", default='now-24h', action='store')
    parser.add_argument('-dsmr', help="Database max rows", default=1000, action='store')
    parser.add_argument('-rses', help="Comma separated list of RSEs", 
        default='ALPAMED-DPM, CNAF-STORM, DESY-DCACHE, EULAKE-1, GSI-ROOT, IN2P3-CC-DCACHE, INFN-NA-DPM, LAPP-DCACHE' + \
            'PIC-DCACHE, SARA-DCACHE, LAPP-WEBDAV')
    parser.add_argument('-t', help="Slack API token?", action='store')
    parser.add_argument('-v', help="verbose?", action='store_true')
    args = parser.parse_args()

    if args.v:
        logger = Logger(level='DEBUG').get()
    else:
        logger = Logger(level='INFO').get()
    Logger(name="rucio", level='INFO')          # set Rucio logger format

    rses = args.rses.split(', ')

    @RTMClient.run_on(event="message")
    def reply(**payload):
        data = payload['data']
        web_client = payload['web_client']
        channel_id = data['channel']
        thread_ts = data['ts']
        user = data['user']

        if 'hello' in data['text']:
            text = 'Hello, <@{}>'.format(user)
            web_client.chat_postMessage(
                channel=channel_id,
                text=text,
                thread_ts=thread_ts
            )
        elif 'list commands' in data['text']:
            cmds = {
                'hello': 'test bot aliveness',
                'disable job *<job_idx>* for *<user>*': '',
                'enable job *<job_idx>* for *<user>*': '',
                'show information for *<rse>*': '',
                'list commands': '',
                'list jobs for *<user>*': '',
                'list rses': '',
                'show replications for *<rse>*': '',
                'start job *<job_idx>* for *<user>*': '',
            }
            web_client.chat_postMessage(
                channel=channel_id,
                text='\n'.join(['{}'.format(cmd, desc) for cmd, desc in cmds.items()]),
                thread_ts=thread_ts
            )
        elif 'list jobs' in data['text']:
            if len(data['text'].split()) != 4:
                text = "Incorrect syntax, please see *list commands*"
            else:
                user = data['text'].split()[3].strip()
                p = Popen(['id', '-u', user], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                output, err = p.communicate()
                if p.returncode:
                    text = "Could not find user"
                else:
                    cron = CronTab(user=user)
                    jobs = []
                    for idx, job in enumerate(cron):
                        if '#' in job.render():
                            jobs.append('[*{:2}*] {} (DISABLED)'.format(idx, job.render()))
                        else:
                            jobs.append('[*{:2}*] {}'.format(idx, job.render()))
                    text = '\n'.join(jobs)

            web_client.chat_postMessage(
                channel=channel_id,
                text=text,
                thread_ts=thread_ts
            )
        elif 'list rses' in data['text']:
            web_client.chat_postMessage(
                channel=channel_id,
                text=', '.join(rses),
                thread_ts=thread_ts
            )
        elif 'disable job' in data['text']:
            if len(data['text'].split()) != 5:
                text = "Incorrect syntax, please see *list commands*"
            else:
                user = data['text'].split()[4].strip()
                p = Popen(['id', '-u', user], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                output, err = p.communicate()
                if p.returncode:
                    text = "Could not find user"
                else:
                    job_idx = int(data['text'].split()[2].strip())
                    cron = CronTab(user=user)
                    cron[job_idx].enable(False)
                    
                    if cron[job_idx].is_valid():
                        cron.write()
                    text = 'disabled job {} ({})'.format(job_idx, cron[job_idx].render())

            web_client.chat_postMessage(
                channel=channel_id,
                text=text,
                thread_ts=thread_ts
            )
        elif 'enable job' in data['text']:
            if len(data['text'].split()) != 5:
                text = "Incorrect syntax, please see *list commands*"
            else:
                user = data['text'].split()[4].strip()
                p = Popen(['id', '-u', user], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                output, err = p.communicate()
                if p.returncode:
                    text = "Could not find user"
                else:
                    job_idx = int(data['text'].split()[2].strip())
                    cron = CronTab(user=user)
                    cron[job_idx].enable(True)
                    
                    if cron[job_idx].is_valid():
                        cron.write()
                    text = 'enabled job {} ({})'.format(job_idx, cron[job_idx].render())

            web_client.chat_postMessage(
                channel=channel_id,
                text=text,
                thread_ts=thread_ts
            )
        elif 'show information for' in data['text']:
            requestedRse = data['text'].split()[3].strip()
            dialog = []
            rucio = RucioWrappersAPI()

            try:
                rucio.whoAmI()
                dialog.append('*RSE Information*\n')
                for u in rucio.getRSEUsage(requestedRse):
                    if u['source'] == 'rucio':
                        nfiles = u['files']
                        used = u['used'] 
                    elif u['source'] == 'storage':
                        total = u['total']
                dialog.append('Space free: {}%'.format(round(100-(100*used/total), 1)))
                dialog.append('Number of files: {:.2e}'.format(nfiles))

                protocolsSupported = []
                for protocol in rucio.getRSEProtocols(requestedRse):
                    protocolsSupported.append(protocol['scheme'])
                dialog.append('Protocols supported: {}\n'.format(
                    ', '.join(protocolsSupported)))
            except Exception as e:
                dialog.append('Could not communicate with Rucio server. {}'.format(
                    repr(e)))
            
            web_client.chat_postMessage(
                channel=channel_id,
                text='\n'.join(dialog),
                thread_ts=thread_ts
            )
        elif 'show replications for' in data['text']:
            requestedRse = data['text'].split()[3].strip()
            dialog = []
            if args.dt == 'es':
                es = ES(args.du, logger)

                dialog.append('\n*Replications* for *{}*'.format(requestedRse))
                dialog.append('\n_As source_\n')
                for dstRse in rses:
                    if requestedRse == dstRse:
                        continue
                    res = es.search(index=args.di, maxRows=args.dsmr,
                        body={
                            "query": {
                                "bool": {
                                    "filter": [{
                                        "term": {
                                            'task_name.keyword': 'test-replication'
                                        }
                                    }, {
                                        "term": {
                                            "from_rse.keyword": requestedRse
                                        }
                                    }, {
                                        "term": {
                                            "to_rse.keyword": dstRse
                                        }
                                    }, {
                                        "range": {
                                            "created_at": {
                                                "gte": args.dsgte,
                                                "lte": args.dslte
                                            }
                                        }
                                    }]
                                }
                            }
                        })

                    isSubmitted = 0
                    isStuck = 0
                    isReplicating = 0
                    isDone = 0
                    for hit in res['hits']['hits']:
                        if hit['_source']['is_submitted']:
                            isSubmitted += 1
                        if hit['_source']['is_done']:
                            isDone += 1
                        elif hit['_source']['is_stuck']:
                            isStuck += 1
                        elif hit['_source']['is_replicating']:
                            isReplicating += 1

                    percentageSuccessful = 0 if isSubmitted == 0 else round(
                        100.*isDone/isSubmitted)
                    if percentageSuccessful == 0:
                        dialog.append(":skull: {}:\t{}%".format(dstRse, 0))
                    else:
                        dialog.append("{}:\t{}%".format(dstRse, percentageSuccessful))

                dialog.append('\n_As destination_\n')
                for srcRse in rses:
                    if requestedRse == srcRse:
                        continue
                    res = es.search(index=args.di, maxRows=args.dsmr,
                        body={
                            "query": {
                                "bool": {
                                    "filter": [{
                                        "term": {
                                            'task_name.keyword': 'test-replication'
                                        }
                                    }, {
                                        "term": {
                                            "from_rse.keyword": srcRse
                                        }
                                    }, {
                                        "term": {
                                            "to_rse.keyword": requestedRse
                                        }
                                    }, {
                                        "range": {
                                            "created_at": {
                                                "gte": args.dsgte,
                                                "lte": args.dslte
                                            }
                                        }
                                    }]
                                }
                            }
                        })

                    isSubmitted = 0
                    isStuck = 0
                    isReplicating = 0
                    isDone = 0
                    for hit in res['hits']['hits']:
                        if hit['_source']['is_submitted']:
                            isSubmitted += 1
                        if hit['_source']['is_done']:
                            isDone += 1
                        elif hit['_source']['is_stuck']:
                            isStuck += 1
                        elif hit['_source']['is_replicating']:
                            isReplicating += 1

                    percentageSuccessful = 0 if isSubmitted == 0 else round(
                        100.*isDone/isSubmitted)
                    if percentageSuccessful == 0:
                        dialog.append(":skull: {}:\t{}%".format(srcRse, 0))
                    else:
                        dialog.append("({}:\t{}%".format(srcRse, percentageSuccessful))
            web_client.chat_postMessage(
                channel=channel_id,
                text='\n'.join(dialog),
                thread_ts=thread_ts
            )
        elif 'start job' in data['text']:
            if len(data['text'].split()) != 5:
                text = "Incorrect syntax, please see *list commands*"
            else:
                user = data['text'].split()[4].strip()
                p = Popen(['id', '-u', user], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                output, err = p.communicate()
                if p.returncode:
                    text = "Could not find user"
                else:
                    job_idx = int(data['text'].split()[2].strip())
                    user = data['text'].split()[4].strip()
                    cron = CronTab(user=user)
                    cmd = cron[job_idx].command

                    text = 'running job {} ({})'.format(job_idx, cron[job_idx].command)

                    web_client.chat_postMessage(
                        channel=channel_id,
                        text=text,
                        thread_ts=thread_ts
                    )

                    p = Popen(cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    output, err = p.communicate()
                    if p.returncode:
                        text = "Failed to run job"
                    else:
                        text = output.decode('UTF-8')

                    web_client.chat_postMessage(
                        channel=channel_id,
                        text=text,
                        thread_ts=thread_ts
                    )
    rtm_client = RTMClient(token=args.t)
    rtm_client.start()