'''The DataPreprocessor Module

Summary
-------
This module defines functions to pre-process crawled comment data of a movie.
'''

import os
import sys
from glob import glob
import json
import pandas as pd

import config
import util


def save_dataframe_as_json(df, json_file_path):
    '''Save the dataframe 'df' as a json file with path 'json_file_path'.
    
    Parameters
    ----------
    df: pandas.DataFrame
        The dataframe to be saved in the json file
    json_file_path: str
        The full path of the json file, including directory and file name
    
    Returns
    -------
    None
    '''

    # convert df to json object
    json_str = df.to_json(orient='records')
    json_object = json.loads(json_str)

    # write json object into file
    with open(json_file_path, mode='w', encoding='utf-8') as file:
        json.dump(json_object, file, indent=4, ensure_ascii=False)


def combine_daily_comment_data(movie_id, date_str):
    '''Combine the crawled comment data of the movie with id 'movid_id'
    on the date 'date_str' into one json file.
    
    Parameters
    ----------
    movie_id: int
        The id of the movie to combine crawled comment data
    date_str: str
        The date of crawled comment data to be combined
    
    Returns
    -------
    None
    
    Notes
    -----
    The method just simply puts all comment records together in one json file,
    doesn't do anything else (e.g., remove duplicates, re-format comment data, etc.).
    '''

    try:
        # Find all comment json files of movie 'movid_id' that are crawled on 'date_str'
        path_pattern = config.COMMENT_CRAWLED_FILE.format(movie_id=movie_id, date_str=date_str, timestamp_str='*')     
        comment_crawled_files = glob(path_pattern)

        if len(comment_crawled_files) == 0: # no need to combine
            return

        # Read each json file into a dataframe and concatenate them
        df_daily = None
        for file in comment_crawled_files:
            df = pd.read_json(file)
            df_daily = pd.concat([df_daily, df], ignore_index=True)

        # Write the concatenated dataframe into a json file
        comment_daily_file = config.COMMENT_DAILY_FILE.format(movie_id=movie_id, date_str=date_str)
        save_dataframe_as_json(df_daily, comment_daily_file)

    except Exception as e:
        msg = f'Combine comment data of the movie with id \'{movie_id}\' that are crawled on the date \'{date_str}\' failed. -- Original Exception -- {e}'
        log_level = config.LOG_LEVEL_ERROR
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.DATA_PREPROCESSOR_LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.DATA_PREPROCESSOR_ERROR_LOG_FILE, logger_name=logger_name, log_level=log_level)
        
    else:
        msg = f'Combine comment data of the movie with id \'{movie_id}\' that are crawled on the date \'{date_str}\' sucessfully. Combined data saved in \'{comment_daily_file}\' file.'
        log_level = config.LOG_LEVEL_INFO
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.DATA_PREPROCESSOR_LOG_FILE, logger_name=logger_name, log_level=log_level)


def merge_all_comment_data(movie_id, date_str):
    '''Merge the comment data of the movie with id 'movid_id'
    that are crawled on the date 'date_str' into the merged json file
    and remove duplicates.

    Parameters
    ----------
    movie_id: int
        The id of the movie to merge daily comment data
    date_str: str
        The date of daily comment data to be merged
    
    Returns
    -------
    None
    '''

    try:
        # Read the daily comment data json file of the date 'date_str' into a dataframe
        df_daily = None
        comment_daily_file = config.COMMENT_DAILY_FILE.format(movie_id=movie_id, date_str=date_str)
        if os.path.isfile(comment_daily_file):
            df_daily = pd.read_json(comment_daily_file)
        
        if df_daily is None: # no need to merge
            return
        
        # Read the merged comment data json file into a dataframe
        df_merged = None
        comment_merged_file = config.COMMENT_MERGED_FILE.format(movie_id=movie_id)        
        if os.path.isfile(comment_merged_file):
            df_merged = pd.read_json(comment_merged_file)
        
        # Merge dataframes and remove duplicates
        df_merged = pd.concat([df_merged, df_daily], ignore_index=True)
        # keep='last': keep the latest copy of duplicate records
        df_merged = df_merged.drop_duplicates(subset=['user_name', 'comment_timestamp'], keep='last', ignore_index=True)

        # Write the merged dataframe into a json file
        save_dataframe_as_json(df_merged, comment_merged_file)        

    except Exception as e:
        msg = f'Merge \'{comment_daily_file}\' into \'{comment_merged_file}\' failed. -- Original Exception -- {e}'
        log_level = config.LOG_LEVEL_ERROR
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.DATA_PREPROCESSOR_LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.DATA_PREPROCESSOR_ERROR_LOG_FILE, logger_name=logger_name, log_level=log_level)
    else:
        msg = f'Merge \'{comment_daily_file}\' into \'{comment_merged_file}\' successfully.'
        log_level = config.LOG_LEVEL_INFO
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.DATA_PREPROCESSOR_LOG_FILE, logger_name=logger_name, log_level=log_level)
