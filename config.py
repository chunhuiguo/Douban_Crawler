'''The Config Module

Summary
-------
This module defines constants and globla variables used in the program.

The module defines constants in the following categores:
-- the general constants
-- the directory configuration constants
-- the crawler configuration constants
-- the APScheduler configuration constants
-- the job configuration constants
'''

import os
import pytz
from datetime import datetime
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

# --- General Constants ---
# The time zone used in the program
TIME_ZONE = pytz.utc

# The log level
LOG_LEVEL_DEBUG = 1
LOG_LEVEL_INFO = 2
LOG_LEVEL_WARNING = 3
LOG_LEVEL_ERROR = 4
LOG_LEVEL_CRITICAL = 5

'''
# The name of the program
PROGRAM_NAME = 'crawler'
'''


# --- Directory Configuration Constants ---
# The current working directory
CURRENT_WORKING_DIRECTORY = os.getcwd()

# The directory to store data
DATA_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, 'data')

# The directory to store program execution logs
LOG_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, 'log')
# The log file -- log ALL information
LOG_FILE = None
# The error log file -- ONLY log ALL ERROR/CRITICAL information
ERROR_LOG_FILE = None
# log information related to the scheduler
SCHEDULER_LOG_FILE = None
# ONLY log ERROR/CRITICAL information related to the scheduler
SCHEDULER_ERROR_LOG_FILE = None
# log information related to the data pre-processor
DATA_PREPROCESSOR_LOG_FILE = None
# ONLY log ERROR/CRITICAL information related to the data pre-processor
DATA_PREPROCESSOR_ERROR_LOG_FILE = None
# log information related to the movie info crawler
MOVIE_INFO_CRAWLER_LOG_FILE = None
# ONLY log ERROR/CRITICAL information related to the movie info crawler
MOVIE_INFO_CRAWLER_ERROR_LOG_FILE = None
# log information related to the comment crawler
COMMENT_CRAWLER_LOG_FILE = None
# ONLY log ERROR/CRITICAL information related to the comment crawler
COMMENT_CRAWLER_ERROR_LOG_FILE = None
# log information related to the movie list manager
MOVIE_LIST_MANAGER_LOG_FILE = None
# ONLY log ERROR/CRITICAL information related to the movie list manager
MOVIE_LIST_MANAGER_ERROR_LOG_FILE = None
'''
# The APScheduler log file -- log APScheduler information via APScheduler's logger
APSCHEDULER_LOG_FILE = None
'''
#??? TESTING CODE
#LOG_FILE = os.path.join(LOG_DIRECTORY, datetime.now(TIME_ZONE).strftime('%Y-%m-%d')  + '.log')


# The directory to store crawled comment data:
# one json file for each comment crawl process for each movie
# (i.e., about 20 comment records in each json file)
COMMENT_CRAWLED_DIRECTORY = os.path.join(DATA_DIRECTORY, 'comment_data_crawled')
# The directory to store daily(-crawled) comment data:
# one json file for each day's crawled comment data for each movie
# (i.e., combine a movie's one day's crawled comment data into one json file)
COMMENT_DAILY_DIRECTORY = os.path.join(DATA_DIRECTORY, 'comment_data_daily')
# The directory to store merged comment data:
# one json file for each movie's crawled comment data
# (i.e., merge a movie's (all days') crawled comment data into one json file and remove duplicates)
COMMENT_MERGED_DIRECTORY = os.path.join(DATA_DIRECTORY, 'comment_data_merged')

# The file to store crawled comment data
COMMENT_CRAWLED_FILE = os.path.join(COMMENT_CRAWLED_DIRECTORY, 'comment_{movie_id}_{date_str}_{timestamp_str}.json')
# The file to store daily(-crawled) comment data
COMMENT_DAILY_FILE = os.path.join(COMMENT_DAILY_DIRECTORY, 'comment_{movie_id}_{date_str}.json')
# The file to store merged comment data
COMMENT_MERGED_FILE = os.path.join(COMMENT_MERGED_DIRECTORY, 'comment_{movie_id}.json')

# The directory to store crawled movie info data
MOVIE_INFO_DIRECTORY = os.path.join(DATA_DIRECTORY, 'movie_info_data')

# The directory to store movie list files
MOVIE_LIST_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, 'movie_list')
# The CSV file to store movie list
MOVIE_LIST_FILE = os.path.join(MOVIE_LIST_DIRECTORY, 'movie_list.csv')
# The CSV file to store movie list to be updated
MOVIE_LIST_UPDATE_FILE = os.path.join(MOVIE_LIST_DIRECTORY, 'movie_list_to_update.csv')

# The directory to store job scheduling information
SCHEDULING_DIRECTORY = os.path.join(CURRENT_WORKING_DIRECTORY, 'scheduling')
# The daily CSV file to store comment crawl job cron schedule information
COMMENT_CRAWL_JOBS_CRON_SCHEDULE_FILE = None
# The daily CSV file to store scheduled jobs information
SCHEDULED_JOBS_FILE = None



# --- Crawler Configuration Constants ---
# The User-Agent of the Android Chrome browser, to access m.douban.com
CHROME_ANDROID_USER_AGENT = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36'
# The User-Agent of the Desktop Chrome browser, to access douban.com
#CHROME_DESKTOP_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
CHROME_DESKTOP_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'


# The base URL of m.douban.com
M_DOUBAN_BASE_URL = 'https://m.douban.com'
# The base URL of douban.com
DOUBAN_BASE_URL = 'https://www.douban.com'
# The base URL of Douban Movie
DOUBAN_MOVIE_BASE_URL = 'https://movie.douban.com'

# The URL pattern of a movie/TV-series (info) page to be crawled
# movie_id: the id of the movie/TV-series
# https://movie.douban.com/subject/<movie_id>
MOVIE_INFO_URL = DOUBAN_MOVIE_BASE_URL + '/subject/{movie_id}'

# The URL pattern of a movie/TV-series comment page to be crawled
# movie_id: the id of the movie/TV-series
# comment_start_index: the start index of movie/TV-series comment to be crawled
# https://m.douban.com/movie/subject/<movie_id>/comments?sort=time&start=<comment_start_index>
MOVIE_COMMENT_URL = M_DOUBAN_BASE_URL + '/movie/subject/{movie_id}/comments?sort=new_score&start={comment_start_index}'

# The increment step of movie/TV-series comments for each crawl process
# The m.douban.com displays 20 comments each webpage, which cannot be customized by user
MOVIE_COMMENT_INCR_STEP = 20

# The maximum seconds to wait for the webbrowser to load the comment page before crawling movie comments
CHROME_WAIT_SECONDS_COMMENT = 30
# The maximum seconds to wait for the webbrowser to load the movie page before crawling movie info
CHROME_WAIT_SECONDS_MOVIE_INFO = 30


# --- APScheduler Configuration Constants ---
'''
# The scheduler's jobstores configuration
from apscheduler.jobstores.memory import MemoryJobStore
JOBSTORES = {
    'default': MemoryJobStore()
}
'''

# The scheduler's executors configuration
EXECUTORS = {
    # a ThreadPoolExecutor named 'default', with 100 spawned threads
    # use the ThreadPoolExecutor for more-frequent and/or small-workload jobs
    'default': ThreadPoolExecutor(500),
    # a ProcessPoolExecutor named 'processpool', with 10 spawned processes
    # use the ProcessPoolExecutor for less-frequent and/or large-workload jobs
    'processpool': ProcessPoolExecutor(50)
}

# The scheduler's job defaults configuration
JOB_DEFAULTS = {
    'coalesce': True, # whether to only run a job once when several run times are due
    'max_instances': 1 # the maximum number of concurrently executing instances allowed for a job
}


# --- Job Configuration Constants ---
# The id of comment crawl job for movie with id 'movie_id'
COMMENT_CRAWL_JOB_ID = lambda movie_id: f'comment_crawl_{movie_id}'

# The id of movie_info crawl job for movie with id 'movie_id'
MOVIE_INFO_CRAWL_JOB_ID = lambda movie_id: f'movie_info_crawl_{movie_id}'

# The grace period in minutes for scheduling comment crawl jobs
# Avoid schedule comment crawl jobs at/near 00:00:00 and 24:00:00
# Schedule comment crawl jobs in the [00:00:00: + GRACE_MINUTE,  24:00:00 - GRACE_MINUTE] time window
# There are some daily routine jobs scheduled to run at 00:00:00 each day
# Prevent interference between comment crawl jobs and daily routine jobs
COMMENT_CRAWL_SCHEDULE_GRACE_MINUTE = 30

# The comment increment to crawl each day
# If the comment increment is less than COMMENT_INCREMENT_CRAWL_PER_DAY, then linearly increase comment crawl interval (in days)
# The comment crawl interval fomula is ceiling(1 / comment_increment / COMMENT_INCREMENT_CRAWL_PER_DAY), i.e., ceiling(COMMENT_INCREMENT_CRAWL_PER_DAY / comment_increment)
COMMENT_INCREMENT_CRAWL_PER_DAY = 1000

# The maximum comment crawl interval in days
# The comment crawl interval is min(comment_crawl_interval, MAX_COMMENT_CRAWL_INTERVAL)
MAX_COMMENT_CRAWL_INTERVAL = 10
# The minimum comment crawl interval in days
# The comment crawl interval is max(comment_crawl_interval, MIN_COMMENT_CRAWL_INTERVAL)
MIN_COMMENT_CRAWL_INTERVAL = 1

# The seconds to pause after each comment crawl procedure/subjob
# To bypass DouBan (D)DoS detect
SLEEP_SECOND_AFTER_COMMENT_CRAWL_SUBJOB = 3



# --- Global Variables ---
# The APScheduler: use the BackgroundScheduler
bg_scheduler = None

# The pandas.DataFrame to store movie list information which are read from the movie list CSV file
# Each row describes a movie, including index, movie_id, last_crawl_total_comment_count, rating_start_date, have_rates
# Each row uses the first column value (same as movie_id) as its index
# The index column (with same value of movie_id) is added to make locating row easily
movie_list_df = None


# The pandas.DataFrame to store cron schedule information for comment crawl jobs
# Each row describes the cron schedule information of a comment crawl job, inculding hour, minute, second
# Each row uses the movie_id of the corresponding comment crawl job as its index
# The cron schedule information are auto-calculated so that all comment crawl jobs (of different movies) are evenly distributed in the 24-hour window
comment_crawl_jobs_cron_schedule_df = None
