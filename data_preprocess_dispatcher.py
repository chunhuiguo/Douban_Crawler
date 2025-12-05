'''The DataPreprocessDispatcher Module

Summary
-------
This module defines functions to dispatch data pre-process jobs.
'''

import os
import re
from glob import glob
import pandas as pd
from datetime import datetime, timedelta

import config

import data_preprocessor


def dispatch_data_preprocess_jobs(startup):
    '''Dispatch data pre-process jobs.
    
    Parameters
    ----------
    startup: bool
        The flag indicating whether this function is called by the program startup configuration
        -- True: called by startup configuration, need to pre-process all existing comment data            
        -- False: called by the scheduler daily, just need to pre-process comment data from the previous day    
    
    Returns
    -------
    None
    '''

    if startup: # pre-process all existing comment data

        # gather dates with crawled comment data for all movies
        # data are stored in a dictionary: movie_id as key, set of dates as value
        dates_of_movies = gather_dates_for_all_movies()

        # pre-process comment data for each movie and each date:
        # (1) combine comment data crawled on the same day into one json file
        # (2) merge all all comment data into one json file and remove duplicates
        for movie_id, dates in dates_of_movies.items():
            for date_str in dates:
                data_preprocessor.combine_daily_comment_data(movie_id, date_str)
                data_preprocessor.merge_all_comment_data(movie_id, date_str)
            
    else: # only pre-process comment data from the previous day

        # get the date of the previous day        
        now_previous_day = datetime.now(config.TIME_ZONE) - timedelta(days=1)
        date_previous_day = now_previous_day.strftime("%Y-%m-%d")

        # pre-process comment data for each movie:
        # (1) combine comment data crawled on the same day into one json file
        # (2) merge all all comment data into one json file and remove duplicates
        for movie_id in config.movie_list_df['movie_id']:
            data_preprocessor.combine_daily_comment_data(movie_id, date_previous_day)
            data_preprocessor.merge_all_comment_data(movie_id, date_previous_day) 



def gather_dates_for_all_movies():
    '''Gather dates with crawled comment data for all movies.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    dates_of_movies: dict
        A dictionary containing dates with crawled comment data for all movies
        -- movie_id as key, set of dates as value
    '''

    dates_of_movies = {}

    # for eack movie, gather dates with crawled comment data
    # (1) get dates from file names of raw crawled comment data json files in COMMENT_CRAWLED_DIRECTORY
    # (2) remove dates for which comment data is already pre-processed, i.e., existing json files in COMMENT_DAILY_DIRECTORY
    for movie_id in config.movie_list_df['movie_id']:
        raw_dates = retrieve_dates_from_files(movie_id, daily_combined=False)
        preprocessed_dates = retrieve_dates_from_files(movie_id, daily_combined=True)
        dates_of_movies[movie_id] = raw_dates - preprocessed_dates    

    return dates_of_movies


def retrieve_dates_from_files(movie_id, daily_combined):
    '''Retrieve the dates with crawled comment data from file names for the movie with id 'movie_id'.
    
    Parameters
    ----------
    movie_id: int
        The id of the movie to to retrieve dates with crawled comment data
    daily_combined: bool
        The flag indicating whether to retrieve dates from crawled raw comment files or daily combined comment files
        -- True: retrieve dates from daily combined comment files            
        -- False: retrieve dates from crawled raw comment files
    
    Returns
    -------
    dates: set
        A set containing dates with crawled comment data for the movie with id 'movie_id'
        -- use set to eliminate duplicates
    '''

    dates = set()

    path_pattern = ''
    if daily_combined:
        path_pattern = config.COMMENT_DAILY_FILE.format(movie_id=movie_id, date_str='*')
    else:
        path_pattern = config.COMMENT_CRAWLED_FILE.format(movie_id=movie_id, date_str='*', timestamp_str='*')

    files = glob(path_pattern)
    for file in files:
        # extract file name from the full file path
        file_name = os.path.basename(file)
        
        # extract date string from the file name
        date_str = re.split(r'_|\.', file_name)[2]

        dates.add(date_str)

    return dates


