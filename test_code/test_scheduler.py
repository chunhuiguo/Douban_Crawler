"""
Demonstrates how to use the background scheduler to schedule a job that executes on 3 second
intervals.
"""

from datetime import datetime
import time
import os

import config
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor


'''
import logging

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)
'''

def tick_1(msg=''):
    print('Tick11111! The time is: %s' % datetime.now(config.TIME_ZONE), msg)

def tick_2(msg=''):
    print('Tick22222! The time is: %s' % datetime.now(config.TIME_ZONE), msg)

executors = {
    # max_workers – the maximum number of spawned threads.
    # max_workers – the maximum number of spawned processes.
    'default': ThreadPoolExecutor(20), # a ThreadPoolExecutor named “default”, with a worker count of 20
    'processpool': ProcessPoolExecutor(5) # a ProcessPoolExecutor named “processpool”, with a worker count of 5
}
job_defaults = {
    'coalesce': True, # whether to only run the job once when several run times are due
     'max_instances': 1 # the maximum number of concurrently executing instances allowed for this job
}
# the default time zone (defaults to the local timezone)


'''
#scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, timezone=config.TIME_ZONE)
#executor (str|unicode) – alias of the executor to run the job with

#replace_existing (bool) – True to replace an existing job with the same id (but retain the number of runs from the existing one)

#add_job(func, trigger=None, args=None, kwargs=None, id=None, name=None, executor='default', replace_existing=True, **trigger_args)
#scheduler.add_job(myfunc, 'interval', minutes=2, id='my_job_id', replace_existing=True)
#scheduler.reschedule_job('my_job_id', trigger='cron', minute='*/5')
#job.modify(max_instances=6, name='Alternate name')
#scheduler.reschedule_job('my_job_id', trigger='cron', minute='*/5')
try:
    scheduler.add_job(tick, id='job1', trigger='interval', seconds=3)
    scheduler.add_job(tick, args=['test'], id='job2', trigger='interval', seconds=3)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    scheduler.reschedule_job('job2', trigger='interval', seconds=5)
    # This is here to simulate application activity (which keeps the main thread alive).
    while True:
        time.sleep(10)
        
except (KeyboardInterrupt, SystemExit):
    # Not strictly necessary if daemonic mode is enabled but should be done if possible
    scheduler.shutdown()
'''
# :param int|None jitter: delay the job execution by ``jitter`` seconds at most
#Randomize ``next_fire_time`` by adding a random value between 0s and the jitter).

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.start()
    #scheduler.add_job(tick, 'interval', seconds=3)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.add_job(tick_1, args=['3 seconds'],  id='job_3s', replace_existing=True, trigger='interval', seconds=3)        
        #scheduler.add_job(tick_2, args=['5 seconds'], id='job_5s', trigger='interval', seconds=5)
        scheduler.print_jobs()
        while True:
            time.sleep(10)
            scheduler.add_job(tick_1, args=['3 seconds'],  id='job_3s', replace_existing=True, trigger='interval', seconds=2)
            scheduler.add_job(tick_2, args=['5 seconds'], id='job_5s', replace_existing=True, trigger='interval', seconds=5)
            scheduler.print_jobs()
        
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


'''
# https://apscheduler.readthedocs.io/en/3.x/modules/events.html#module-apscheduler.events
def my_listener(event):
    if event.exception:
        print('The job crashed :(')
    else:
        print('The job worked :)')

scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
'''
