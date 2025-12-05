'''The CommentCrawlDispatcher Module

Summary
-------
This module defines functions to dispatch comment crawl jobs.
'''

import os
import time
from datetime import datetime

import config
import comment_crawler
import movie_list_manager


def update_comment_crawl_job_cron_schedule(movie_id, last_crawl_total_comment_count, total_comment_count, comment_crawl_jobs_cron_schedule_df):
    '''Update the 'day' in the cron schedule for the comment crawl job of the movie with id 'movie_id'.
    -- the cron schedule of crawling comment includes 'day', 'hour', 'minute', 'second'
    -- the 'day' in the cron schedule is updated based on crawled comment count increment
    -- the 'hour', 'minute', 'second' in the cron schedule is calculated by the 'daily_job_dispatcher.calculate_comment_crawl_job_cron_schedule'
        function to evenly distribute all comment crawl jobs in the 24-hour window    
   
    Parameters
    ----------
    movie_id: int
    last_crawl_total_comment_count: int
    total_comment_count: int
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

    comment_increment = total_comment_count - last_crawl_total_comment_count

    # The comment crawl interval fomula is ceiling(1 / comment_increment / COMMENT_INCREMENT_CRAWL_PER_DAY),
    # i.e., ceiling(COMMENT_INCREMENT_CRAWL_PER_DAY / comment_increment)
    comment_crawl_interval_in_days = config.COMMENT_INCREMENT_CRAWL_PER_DAY // comment_increment + 1

    # make sure the comment_crawl_interval is in the range of [MIN_COMMENT_CRAWL_INTERVAL, MAX_COMMENT_CRAWL_INTERVAL]
    # min(comment_crawl_interval, MAX_COMMENT_CRAWL_INTERVAL)
    comment_crawl_interval_in_days = min(comment_crawl_interval_in_days, config.MAX_COMMENT_CRAWL_INTERVAL)
    # max(comment_crawl_interval, MIN_COMMENT_CRAWL_INTERVAL)
    comment_crawl_interval_in_days = max(comment_crawl_interval_in_days, config.MIN_COMMENT_CRAWL_INTERVAL)

    # update the 'day' in the cron schedule for the comment crawl job of the movie with id 'movie_id'
    # movie_id is the index of the row
    comment_crawl_jobs_cron_schedule_df.at[movie_id, 'day'] = f'*/{comment_crawl_interval_in_days}'
    
    return comment_crawl_jobs_cron_schedule_df



def dispatch_crawl_comment(movie_id, last_crawl_total_comment_count):
    '''Dispatch the crawl_comment job for movie with id 'movie_id'.
    
    Parameters
    ----------
    movie_id: int
        The id of the movie/TV-series to crawl comments
    last_crawl_total_comment_count: str
        
    Returns
    -------
    None

    Notes: (THE FOLLOWING NOTES IS OUTDATED)
    ------
    There are two cases that this function dispatch jobs to crawl ALL comments of the movie with id 'movie_id'.
        The difference is that these two cases indicate different physical meaning and save data in
        different directories, details below.
    -- The 'crawl_all' is True, this case indicate a special occasion to (mannually) (re-)crawl ALL comments
        and save data in directory 'config.ALL_COMMENT_DIRECTORY(movie_id)'.
    -- The 'crawl_all' is False and 'crawled_latest_comment_timestamp' is None, this case indicates that
        it is the first time to crawl (history) comments for newly added movie and save data in
        directory 'config.DATA_DIRECTORY'.

    The m.douban.com CAN retrive ALL short review comments WITHOUT login, but m.douban.com can ONLY retrive
        status=P (看過) comments, cannot retrive status=F (想看) or status=N (在看) comments.
    The m.douban.com movie/TV_series comments URL is:
    -- https://m.douban.com/movie/subject/{movie_id}/comments?sort=time&start={comment_start_index}
    -- the {comment_start_index} starts at 0
    -- each page/request can ONLY load 20 comments, which cannot be customized by user/URL
    '''
    
    # The comment start index of each crawl job
    comment_start_index = 0

    crawl_total_comment_count = False
    total_comment_count = 0 
    
    while True:
        if comment_start_index == 0:
            crawl_total_comment_count = True
        else:
            crawl_total_comment_count = False

        results = comment_crawler.crawl_comment(movie_id, comment_start_index, crawl_total_comment_count)
        
        if crawl_total_comment_count:
            total_comment_count = results['total_comment_count']

        # No more comment to crawl
        if results['current_page_comment_count'] == 0:
            break
        
        # Set comment_start_index for the next crawl procedure/sub-job
        comment_start_index += config.MOVIE_COMMENT_INCR_STEP
        # Pause several seconds after each crawl procedure to bypass DouBan (D)DoS detect
        time.sleep(config.SLEEP_SECOND_AFTER_COMMENT_CRAWL_SUBJOB)

    # After the crawl job (all crawl procedures/sub-jobs)
    # Update the 'last_crawl_total_comment_count' of the movie in both the dataframe and the CSV movie_list file
    movie_list_manager.update_movie_total_comment_count(
        config.movie_list_df,
        config.MOVIE_LIST_FILE,
        movie_id,
        total_comment_count
    )

    # update the 'day' in the cron schedule for the comment crawl job of the movie with id 'movie_id'
    config.comment_crawl_jobs_cron_schedule_df = update_comment_crawl_job_cron_schedule(movie_id, last_crawl_total_comment_count, total_comment_count, config.comment_crawl_jobs_cron_schedule_df)

