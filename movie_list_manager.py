'''The MovieListManager Module

Summary
-------
This module defines functions to manage/operate the movie list (update) CSV files.
'''

import os
import sys
from datetime import datetime
import pandas as pd

import config
import util


def normalize_movie_list_df(df, update=False):
    '''Normalize the given movie list dataframe
    The normalize procedure is:
    -- drop blank rows (before type-casting to avoid type-casting errors for NaN)
    -- type-cast columns and indices
    -- drop duplicate rows
    
    Parameters
    ----------
    df: pandas.DataFrame
        The movie list dataframe to be normalized
    update: bool, optional
        Whether this function is called by the update or read/setup movie list procedure
        -- True: update, exception -- log but not terminate program
        -- False: read, default value, exception -- error initializing movie list data, TERMINATE program
    
    Returns
    -------
    pandas.DataFrame
        The normalized movie list dataframe
    '''

    # Drop rows if its all column values are blank
    # Before columns' type-casting, to avoid type-casting errors for NaN
    df = df.dropna(how='all')
    
    # Desired column types
    col_types = {
        'movie_id': 'int64',
        'last_crawl_total_comment_count': 'int64',
        'rating_start_date': 'object', # as str
        'have_rates': 'object', # as str
        'note': 'object' # as str
    }
    try:
        # Cast columns to desired types
        df = df.astype(col_types)
        # Cast indices to int
        df.index = df.index.astype('int64')
    except Exception as e:
        msg = (
            f'Data error in the \'movie_list_to_update\' CSV file. -- Original Exception -- {e}'
            if update
            else f'Data error in the \'movie_list\' CSV file. The program is TERMINATED! -- Original Exception -- {e}'
        )
        log_level = config.LOG_LEVEL_ERROR if update else config.LOG_LEVEL_CRITICAL
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=log_level)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=log_level)

        if update:
            # Error in movie list update data, discard updates
            raise Exception('Data error in the \'movie_list_to_update\' CSV file.')
            return None
        else:
            exit() # Error in initial movie list data, terminate program
    
    # Drop duplicates that have the same movie_id, only keep the first row
    df = df.drop_duplicates(subset=['movie_id'], keep='first')

    return df


def read_movie_list(csv_file):
    '''Read movie list from the CSV file
    
    Parameters
    ----------
    csv_file: str
        The full path of the CSV file storing the movie list
    
    Returns
    -------
    pandas.DataFrame
        A dataframe describing movie list
    '''

    try:
        # Read the CSV file
        df = pd.read_csv(csv_file, index_col=0)
        # Normalize the dataframe
        df = normalize_movie_list_df(df, update=False)        
    except Exception as e:
        msg = f'Read movie list file \'{csv_file}\' failed. The program is TERMINATED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        exit()
    
    return df


def update_movie_list(df, csv_file, update_csv_file):
    '''Update movie list dataframe with data from the CSV update file
    The update procedure is:
    -- read movie list update data from the CSV update file
    -- merge updates into the given dataframe
    -- save updated dataframe into the CSV file
    -- delete the CSV update file
    
    Parameters
    ----------
    df: pandas.DataFrame
        The movie list dataframe to be updated
    csv_file: str
        The CSV file to save the updated dataframe, i.e. the original CSV file of movie list
    update_csv_file: str
        The CSV update file containing the update data
    
    Returns
    -------
    pandas.DataFrame
        The updated dataframe describing latest movie list
    '''

    # Movie list data never been read, read first then update
    if df is None:
        df = read_movie_list(csv_file)    
    
    # The movie list update file does not exist, no need to update, return the original dataframe
    if not os.path.isfile(update_csv_file):
        return df
    
    # Update procedure: read update, merge data, save updates to the original file, delete the update file
    update_df = None

    # read and normalize update data
    try:
        update_df = pd.read_csv(update_csv_file, index_col=0)
        update_df = normalize_movie_list_df(update_df, update=True)
    except Exception as e:
        msg = f'Read movie list update file \'{update_csv_file}\' failed. The updates are DISCARDED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        return df # discard updates, return without updates
    
    # merge data
    update_df = pd.concat([df, update_df])
    # Drop duplicates that have the same movie_id, only keep the first row
    update_df = update_df.drop_duplicates(subset=['movie_id'], keep='first')

    # save updates to the original file
    try:
        update_df.to_csv(csv_file)
    except Exception as e:
        msg = f'Save updates to movie list file \'{csv_file}\' failed. The updates are DISCARDED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        return df # discard updates, return without updates
    
    # delete the update file
    try:
        os.remove(update_csv_file)
    except Exception as e:
        msg = f'Delete the movie list update file \'{update_csv_file}\' failed. Please DELETE it MANUALLY! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        return update_df  # contain updates, return with updates
    
    return update_df # return with updates


def update_movie_total_comment_count(df, csv_file, movie_id, total_comment_count):
    '''Update the 'last_crawl_total_comment_count' of the movie with id 'movie_id'
    to be 'total_comment_count' in both the dataframe 'df' and the CSV movie_list file 'csv_file'
    
    Parameters
    ----------
    df: pandas.DataFrame
        The comment crawl jobs dataframe to be updated
    csv_file: str
        The CSV movie_list file to be updated
    movie_id: int
        The id of the movie to update info
    total_comment_count: int ????
        The new timestamp to be updated to be
    
    Returns
    -------
    None
    '''

    # The dataframe has no data, no need to update
    if df is None:
        return
    
    # Update the 'last_crawl_total_comment_count' of the dataframe
    # use 'movie_id' as index to locate the cell
    df.loc[movie_id, 'last_crawl_total_comment_count'] = total_comment_count

    # Save the update to the CSV movie_list file
    try:
        df.to_csv(csv_file)
    except Exception as e:
        # ?????
        msg = f'Save the updated \'last_crawl_total_comment_count\' with value \'{total_comment_count}\' of movie with id \'{movie_id}\' to movie list file \'{csv_file}\' failed. The update is DISCARDED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)


def update_movie_rating_start_info(df, csv_file, movie_id, rating_start_date):
    '''Update the 'rating_start_date' and 'have_rates' of the movie with id 'movie_id'
    in both the dataframe 'df' and the CSV movie_list file 'csv_file'
    
    Parameters
    ----------
    df: pandas.DataFrame
        The comment crawl jobs dataframe to be updated
    csv_file: str
        The CSV movie_list file to be updated
    movie_id: int
        The id of the movie to update info
    timestamp: str ????
        The new timestamp to be updated to be
    
    Returns
    -------
    None
    '''

    # The dataframe has no data, no need to update
    if df is None:
        return
    
    # Update the 'crawled_latest_comment_timestamp' of the dataframe
    # use 'movie_id' as index to locate the cell
    df.loc[movie_id, 'rating_start_date'] = rating_start_date   
    df.loc[movie_id, 'have_rates'] = 'yes'

    # Save the update to the CSV movie_list file
    try:
        df.to_csv(csv_file)
    except Exception as e:
        # ?????
        msg = f'Save the updated \'rating_start_date\' and \'have_rates\' of movie with id \'{movie_id}\' to movie list file \'{csv_file}\' failed. The update is DISCARDED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)