'''The DailyJobDispatcher Module

Summary
-------
This module defines functions to dispatch daily routine jobs.
'''

import pandas as pd

import config
import util
import movie_list_manager
import scheduler


def calculate_comment_crawl_job_cron_schedule(movie_list_df, comment_crawl_jobs_cron_schedule_df):
    '''Calculate/Update the cron schedule (including 'hour', 'minute', 'second') for comment crawl job of each movie
    in 'movie_list_df' and store the updated cron schedule in 'comment_crawl_jobs_cron_schedule_df'.
    -- the cron schedule of crawling comment includes 'day', 'hour', 'minute', 'second'
    -- this function only calculates 'hour', 'minute', 'second' in the cron schedule
        so that all comment crawl jobs are evenly distributed in the 24-hour window
    -- the 'day' in the cron schedule is updated by the 'comment_crawl_dispatcher.update_comment_crawl_job_cron_schedule'
        function based on crawled comment count increment
   
    Parameters
    ----------
    movie_list_df: pandas.DataFrame
        The dataframe storing movie list data
    comment_crawl_jobs_cron_schedule_df: pandas.DataFrame
        The dataframe storing cron schedule to be calculated/updated
        -- Each row describes the cron schedule of a comment crawl job, inculding 'day', 'hour', 'minute', 'second'
        -- Each row uses the movie_id of the corresponding crawl job as its index
    
    Returns
    -------
    pandas.DataFrame
        A dataframe storing the updated cron schedule
        -- Each row describes the cron schedule of a comment crawl job, inculding 'day', 'hour', 'minute', 'second'
        -- Each row uses the movie_id of the corresponding crawl job as its index
    '''
    
    if comment_crawl_jobs_cron_schedule_df is None:
        # create a new dataframe to store cron schedule data, i.e., the return
        comment_crawl_jobs_cron_schedule_df = pd.DataFrame(columns=['day', 'hour', 'minute', 'second'])

    job_count = len(movie_list_df.index)
    seconds_per_day = 86400 # 24 * 60 * 60
    seconds_per_hour = 3600 # 60 * 60
    seconds_per_minute = 60
    # The time interval (in seconds) between two adjacent jobs
    job_interval = seconds_per_day // job_count    
    
    i = 0 # job number (1st job, 2nd job, 3rd job, etc.)
    for movie_id in movie_list_df.index: # movie_id as the index
        # The schedule/time (in second) to run a job, relative to 00:00:00
        schedule_in_second = i * job_interval

        # Convert schedule_in_second to cron schedule including 'hour', 'minute', 'second'
        hour, remainning_minute = divmod(schedule_in_second, seconds_per_hour)
        minute, second = divmod(remainning_minute, seconds_per_minute)

        # Grace crawl jobs schedule (nearly) from 00:00:00 and 24:00:00
        # Schedule crawl jobs in the [00:00:00: + GRACE_MINUTE,  24:00:00 - GRACE_MINUTE] time window
        # There are some daily routine jobs scheduled to run at 00:00:00 each day
        # Prevent interference between comment crawl jobs and daily routine jobs
        if hour == 0:
            minute = max(minute, config.COMMENT_CRAWL_SCHEDULE_GRACE_MINUTE)
        if hour == 23:
            minute = min(minute, seconds_per_minute - config.COMMENT_CRAWL_SCHEDULE_GRACE_MINUTE)
        if hour == 24:
            hour = 23
            minute = max(minute, seconds_per_minute - config.COMMENT_CRAWL_SCHEDULE_GRACE_MINUTE)

        
        # the cron schedule for the comment crawl job of the movie with id 'movie_id' exist:
        # -- update the 'hour', 'minute', 'second' in the cron schedule
        if movie_id in comment_crawl_jobs_cron_schedule_df.index:
            comment_crawl_jobs_cron_schedule_df.at[movie_id, 'hour'] = hour
            comment_crawl_jobs_cron_schedule_df.at[movie_id, 'minute'] = minute
            comment_crawl_jobs_cron_schedule_df.at[movie_id, 'second'] = second
        # the cron schedule for the comment crawl job of the movie with id 'movie_id' does not exist:
        # -- create a new cron schedule and append it to the dataframe
        else:
            cron_schedule_dict = {
                'day': '*', # default: everyday
                'hour':hour,
                'minute': minute,
                'second': second
            }
            # Append the cron schedule into the dataframe with index of corresponding comment crawl job (i.e. movie_id) as index
            cron_schedule_series = pd.Series(cron_schedule_dict, name=movie_id)
            comment_crawl_jobs_cron_schedule_df = comment_crawl_jobs_cron_schedule_df._append(cron_schedule_series)

        i += 1 # prepare for the next movie
    
    return comment_crawl_jobs_cron_schedule_df


def dispatch_daily_routine_jobs(bg_scheduler, startup):
    '''Dispatch daily routine jobs, including:
    -- update log file
    -- update movie list information
    -- calculate comment crawl jobs cron schedule
    -- update/re-schedule comment crawl jobs in the scheduler, if 'startup' is False
    
    
    Parameters
    ----------
    bg_scheduler: apscheduler.Scheduler
        The background_scheduler to schedule jobs
    startup: bool
        The flag indicating whether this function is called by the program startup configuration
        -- True: called by startup configuration, just setup the environment,
            no need to schedule comment crawl jobs (the main() will schedule crawl jobs)
        -- False: called by the scheduler daily, need to schedule/update/re-schedule comment crawl jobs
    
    Returns
    -------
    None
    '''
    
    # update log file
    util.update_log_and_daily_file()

    # update movie list information
    config.movie_list_df = movie_list_manager.update_movie_list(
        config.movie_list_df,
        config.MOVIE_LIST_FILE,
        config.MOVIE_LIST_UPDATE_FILE
    )

    # calculate comment crawl jobs cron schedule
    config.comment_crawl_jobs_cron_schedule_df = calculate_comment_crawl_job_cron_schedule(config.movie_list_df, config.comment_crawl_jobs_cron_schedule_df)
    # save the calculated comment crawl jobs cron schedule to a csv file
    config.comment_crawl_jobs_cron_schedule_df.to_csv(config.COMMENT_CRAWL_JOBS_CRON_SCHEDULE_FILE, index_label='movie_id')
    
    # update/re-schedule comment crawl jobs in the scheduler, if 'startup' is False
    if not startup:
        scheduler.schedule_comment_crawl_jobs(bg_scheduler)