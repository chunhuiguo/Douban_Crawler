'''The Scheduler Module

Summary
-------
This module defines functions to configure/start APSchedulers and (re-)schedule/modify jobs.
'''

import sys
import random
from datetime import datetime, timedelta
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler

import config
import util
import daily_job_dispatcher
import data_preprocess_dispatcher
import movie_info_crawl_dispatcher
import comment_crawl_dispatcher



def start_scheduler():
    '''Configure and start a BACKGROUND scheduler
    
    Parameters
    ----------
    None
    
    Returns
    -------
    apscheduler.Scheduler
        A BACKGROUND scheduler
    '''

    bg_scheduler = BackgroundScheduler(executors=config.EXECUTORS,
                                    job_defaults=config.JOB_DEFAULTS,
                                    timezone=config.TIME_ZONE)
    bg_scheduler.start()

    # ??? TO IMPLEMENT
    # add listeners to the scheduler
    
    return bg_scheduler


def schedule_daily_routine_jobs(bg_scheduler):
    '''Schedule daily routine job which are run daily at 00:00:00
    
    Parameters
    ----------
    bg_scheduler: apscheduler.Scheduler
        The background_scheduler to schedule daily routine jobs
    
    Returns
    -------
    None
    '''
    
    # Schedule jobs
    #add_job(func=?, kwargs=?, id=?, next_run_time=undefined, executor='default', replace_existing=True, trigger=?, **trigger_args)
    try:
        # Schedule dispatch_daily_routine_jobs() to run daily at 00:00:00
        # --NO NEED (run once in startup config): next_run_time=now: run the job immediately (with jitter) after scheduled regardless of the 00:00:00 trigger
        # --NO NEED: executor='processpool': less-frequent job, use the ProcessPoolExecutor
        # executor='default': use the ThreadPoolExecutor
        # --NO NEED: jitter=10: delay the job execution by x seconds, where x is a random int in [0, jitter]        
        func = daily_job_dispatcher.dispatch_daily_routine_jobs
        kwargs = {
                'bg_scheduler': bg_scheduler,
                'startup': False
            }
        job_id = 'daily_routine_job'

        # ??? TESTING CODE
        #now_jitter = datetime.now(config.TIME_ZONE) + timedelta(seconds=random.uniform(0, 10))

        bg_scheduler.add_job(func=func, kwargs=kwargs, id=job_id,
                          executor='default', replace_existing=True,
                          #next_run_time=now_jitter, # ??? TESTING CODE
                          trigger='cron', hour=0, minute=0, second=0, jitter=10)
    except Exception as e:
        msg = f'Schedule daily routine job failed. -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.SCHEDULER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.SCHEDULER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)



def schedule_data_preprocess_jobs(bg_scheduler):
    '''Schedule data pre-process job which are run daily at 00:05:00
    
    Parameters
    ----------
    bg_scheduler: apscheduler.Scheduler
        The background_scheduler to schedule daily routine jobs
    
    Returns
    -------
    None
    '''
    
    # Schedule jobs
    #add_job(func=?, kwargs=?, id=?, next_run_time=undefined, executor='default', replace_existing=True, trigger=?, **trigger_args)
    try:
        # Schedule dispatch_data_preprocess_jobs() to run daily at 00:00:05
        # --NO NEED (run once in startup config): next_run_time=now: run the job immediately (with jitter) after scheduled regardless of the 00:00:00 trigger
        # --NO NEED: executor='processpool': less-frequent job, use the ProcessPoolExecutor
        # executor='default': use the ThreadPoolExecutor
        # --NO NEED: jitter=10: delay the job execution by x seconds, where x is a random int in [0, jitter]        
        func = data_preprocess_dispatcher.dispatch_data_preprocess_jobs
        kwargs = {
                'startup': False
            }
        job_id = 'data_preprocess_job'

        # ??? TESTING CODE
        #now_jitter = datetime.now(config.TIME_ZONE) + timedelta(seconds=random.uniform(0, 10))

        bg_scheduler.add_job(func=func, kwargs=kwargs, id=job_id,
                          executor='default', replace_existing=True,
                          #next_run_time=now_jitter, # ??? TESTING CODE
                          trigger='cron', hour=0, minute=5, second=0, jitter=10)
    except Exception as e:
        msg = f'Schedule data pre-process job failed. -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.SCHEDULER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.SCHEDULER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)




def schedule_movie_info_crawl_jobs(bg_scheduler):
    '''Schedule movie info crawl job which are run daily at 00:10:00
    
    Parameters
    ----------
    bg_scheduler: apscheduler.Scheduler
        The background_scheduler to schedule daily routine jobs
    
    Returns
    -------
    None
    '''
    
    # Schedule jobs
    #add_job(func=?, kwargs=?, id=?, next_run_time=undefined, executor='default', replace_existing=True, trigger=?, **trigger_args)
    try:
        # Schedule dispatch_data_preprocess_jobs() to run daily at 00:00:05
        # --NO NEED (run once in startup config): next_run_time=now: run the job immediately (with jitter) after scheduled regardless of the 00:00:00 trigger
        # --NO NEED: executor='processpool': less-frequent job, use the ProcessPoolExecutor
        # executor='default': use the ThreadPoolExecutor
        # --NO NEED: jitter=10: delay the job execution by x seconds, where x is a random int in [0, jitter]        
        func = movie_info_crawl_dispatcher.dispatch_crawl_movie_info
        kwargs = None
        job_id = 'movie_info_crawl_job'

        # ??? TESTING CODE
        #now_jitter = datetime.now(config.TIME_ZONE) + timedelta(seconds=random.uniform(0, 10))

        bg_scheduler.add_job(func=func, kwargs=kwargs, id=job_id,
                          executor='default', replace_existing=True,
                          #next_run_time=now_jitter, # ??? TESTING CODE
                          trigger='cron', hour=0, minute=10, second=0, jitter=10)
    except Exception as e:
        msg = f'Schedule movie info crawl job failed. -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.SCHEDULER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.SCHEDULER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)



def schedule_comment_crawl_jobs(bg_scheduler):
    '''Schedule comment crawl jobs evenly distributed in the 24-hour window
    
    Parameters
    ----------
    bg_scheduler: apscheduler.Scheduler
        The background_scheduler to schedule comment crawl jobs
    
    Returns
    -------
    None
    '''    

    for movie_id, movie in config.movie_list_df.iterrows(): # movie_id as the index
        # Schedule a comment crawl job
        # Evenly distribute all comment crawl jobs in the 24-hour window
        #add_job(func=?, kwargs=?, id=?, next_run_time=undefined, executor='default', replace_existing=True, trigger=?, **trigger_args)
        try:
            # Schedule dispatch_crawl_comment() to run based on the cron schedule (with jitter)
            # executor='default': more-frequent jobs, use the ThreadPoolExecutor
            # --NO NEED: next_run_time=now: run the job immediately (with jitter) after scheduled regardless of the trigger
            # jitter=10: delay the job execution by x seconds, where x is a random int in [0, jitter]            
            # with jitter: avoid accessing 'douban.com' simultaneous to prevent network bottleneck and may bypass DouBan DoS detect
            func = comment_crawl_dispatcher.dispatch_crawl_comment
            kwargs = {
                'movie_id': movie_id,
                'last_crawl_total_comment_count': movie['last_crawl_total_comment_count']
            }
            job_id = config.COMMENT_CRAWL_JOB_ID(movie_id)

            # Retrive cron schedule from the dataframe
            day = config.comment_crawl_jobs_cron_schedule_df.at[movie_id, 'day']
            hour = config.comment_crawl_jobs_cron_schedule_df.at[movie_id, 'hour']
            minute = config.comment_crawl_jobs_cron_schedule_df.at[movie_id, 'minute']
            second = config.comment_crawl_jobs_cron_schedule_df.at[movie_id, 'second']

            # ??? TESTING CODE
            #now_jitter = datetime.now(config.TIME_ZONE) + timedelta(seconds=random.uniform(0, 10))

            bg_scheduler.add_job(func=func, kwargs=kwargs, id=job_id,
                              executor='default', replace_existing=True,
                              #next_run_time=now_jitter, # ??? TESTING CODE
                              trigger='cron', day=day, hour=hour, minute=minute, second=second, jitter=10) 
        except Exception as e:
            msg = f'Schedule comment crawl job with id \'{job_id}\' failed. -- Original Exception -- {e}'
            current_frame = sys._getframe()
            logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
            util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
            util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
            util.log(msg, config.SCHEDULER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
            util.log(msg, config.SCHEDULER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
    
    # Save all scheduled or re-scheduled jobs to a CSV file
    save_jobs_to_csv(bg_scheduler, config.SCHEDULED_JOBS_FILE)
    
    
    
def save_jobs_to_csv(bg_scheduler, csv_file):
    '''Save jobs in the 'bg_scheduler' to the 'csv_file'
    
    Parameters
    ----------
    bg_scheduler: apscheduler.Scheduler
        The background_scheduler of jobs
    csv_file: str
        The path of the CSV file to store jobs 

    Returns
    -------
    None
    '''  

    jobs = []

    raw_jobs = bg_scheduler.get_jobs()
    for raw_job in raw_jobs:
        job = {
            'id': raw_job.id,
            'name': raw_job.name,
            'func': raw_job.func,
            'args': raw_job.args,
            'kwargs': raw_job.kwargs,
            'coalesce': raw_job.coalesce,
            'trigger': raw_job.trigger,
            'executor': raw_job.executor,
            'misfire_grace_time': raw_job.misfire_grace_time,
            'max_instances': raw_job.max_instances,
            'next_run_time': raw_job.next_run_time
        }
        jobs.append(job)

    df = pd.DataFrame(jobs)
    df.to_csv(csv_file, index=False)    

