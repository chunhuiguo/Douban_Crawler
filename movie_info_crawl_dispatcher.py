'''The MovieInfoCrawlDispatcher Module

Summary
-------
This module defines functions to dispatch movie info crawl jobs.
'''

import os
import time
from datetime import datetime

import config
import movie_info_crawler
import movie_list_manager


def dispatch_crawl_movie_info():
    '''Dispatch the crawl_movie_info job for all movies.    
    
    Parameters
    ----------
    None    
        
    Returns
    -------
    None  
    '''

    # for each movie in the movie list, crawl movie info and update the movie list if necessary
    for index, movie in config.movie_list_df.iterrows():
        movie_id = movie['movie_id']
        have_rates = movie['have_rates']
        crawl_rating_only = True if have_rates == 'yes' else False

        rating_start_date = movie_info_crawler.crawl_movie_info(movie_id, crawl_rating_only)

        # Update the 'rating_start_date' and 'have_rates' of the movie in both the dataframe and the CSV movie_list file
        if not crawl_rating_only and rating_start_date:
            movie_list_manager.update_movie_rating_start_info(
                config.movie_list_df,
                config.MOVIE_LIST_FILE,
                movie_id,
                rating_start_date
            )
            