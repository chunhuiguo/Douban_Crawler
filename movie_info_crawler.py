'''The MovieInfoCrawler Module

Summary
-------
This module defines functions to crawl and parse basic information and aggregate rating of a movie/TV-series.
'''

import os
import sys
import json
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ExpectedConditions
from selenium.webdriver.support.wait import WebDriverWait
#from selenium.webdriver.chrome.service import Service as ChromeService
#from webdriver_manager.chrome import ChromeDriverManager
#from selenium.common.exceptions import TimeoutException
#from selenium.common.exceptions import NoSuchElementException
#from selenium.common.exceptions import ElementNotVisibleException

import config
import util


def crawl_movie_info(movie_id, crawl_rating_only):
    '''Crawl movie information and aggregate rating of a movie/TV-series webpage

    Parameters
    ----------
    movie_id: int
        The id of the movie/TV-series to crawl info
    crawl_rating_only: bool
        The flag indecating whether to crawl rating only
    
    Returns
    -------
    rating_start_date: str
    '''

    # Initialize the movie information and rating dict
    #???
    movie_info = {
        'id': movie_id
    }
    movie_rating = {}  

    # Initialize the return dict
    rating_start_date = None
    
    # Set up Chrome options
    chrome_options = webdriver.ChromeOptions()
    # headless mode: no webbrowser GUI
    chrome_options.add_argument('--headless')
    # log-level=3: less Selenium log information (to be displayed in the terminal/console)
    chrome_options.add_argument('--log-level=3')
    # desktop browser user-agent: to access douban.com
    chrome_options.add_argument(f'--user-agent={config.CHROME_DESKTOP_USER_AGENT}')

    # Get URL of the comment page to be crawled
    url = config.MOVIE_INFO_URL.format(movie_id=movie_id)
    #print(url)
    movie_info['url'] = url

    # Start to crawl movie info and rating, the crawl procedure is as follows:
    # (1) Open the movie webpage to be crawled
    # (2) Wait until the movie info block is loaded
    # (3) Crawl movie information data:
    # ---- (3.1) Parse movie basics from the content of '<script type="application/ld+json">...</script>'
    # ---- (3.2) Crawl other movie info from the webpage
    # (4) Crawl movid rating data:
    # ---- (4.1) Parse rating basics from the content of '<script type="application/ld+json">...</script>'
    # ---- (4.2) Crawl rating details from the webpage
    # (5) Exit the webbrowser
    # (6) Log the sucessful crawl information or any raised exception

    # Open a Chrome webbrowser instance
    #chrome = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    chrome = webdriver.Chrome(options=chrome_options)

    try:        
        # Open the webpage to crawl movie data        
        chrome.get(url)    
        # Wait for a maximum of CHROME_WAIT_SECONDS_MOVIE_INFO seconds to load the movie info block
        # Raise a TimeoutException, if no element is found in that time (i.e., load movie info FAIL)
        # The movid info block is <div id="content"> ... </div>
        #     Refer to the following files:
        #         './webpage_sample/movie-page-WITH-rating-sample-SIMPLIFIED.html'
        #         './webpage_sample/movie-page-WITHOUT-rating-sample-SIMPLIFIED.html'
        #         './webpage_sample/TVseries-page-WITH-rating-sample-SIMPLIFIED.html'
        #         './webpage_sample/TVseries-page-WITHOUT-rating-sample-SIMPLIFIED.html'
        
        chrome_wait = WebDriverWait(chrome, config.CHROME_WAIT_SECONDS_MOVIE_INFO)        
        chrome_wait.until(ExpectedConditions.presence_of_element_located((By.ID, 'content')))        


        #???   
        elem = chrome.find_element(By.CSS_SELECTOR, value='script[type="application/ld+json"]')
        elem_text = elem.get_attribute('innerHTML')
        elem_text = elem_text.replace('\n', '') #replace \n to avoid json.load error below
        json_data = json.loads(elem_text)
        
        # crawl movie info
        if not crawl_rating_only:
            movie_info['title'] = json_data['name']
            movie_info['type'] = json_data['@type']
            
            e = chrome.find_element(By.CSS_SELECTOR, value='span[class="year"]')
            movie_info['year'] = e.text

            '''
            e = chrome.find_element(By.CSS_SELECTOR, value='span[property="v:runtime"]')
            movie_info['runtime'] = e.get_attribute('content')
            '''
            

            release_date_list = []        
            es = chrome.find_elements(By.CSS_SELECTOR, value='span[property="v:initialReleaseDate"]')
            for e in es:
                release_date_list.append(e.text)
            movie_info['release_date'] = release_date_list

            genre_list = []
            es = chrome.find_elements(By.CSS_SELECTOR, value='span[property="v:genre"]')
            for e in es:
                genre_list.append(e.text)
            movie_info['genre'] = genre_list

            #???            
            rating_count = json_data['aggregateRating']['ratingCount']
            if int(rating_count) > 0:
                today = datetime.now(config.TIME_ZONE).strftime("%Y-%m-%d")
                rating_start_date = today
            movie_info['rating_start_date'] = rating_start_date

            e = chrome.find_element(By.CSS_SELECTOR, value='span[property="v:summary"]')
            movie_info['summary'] = e.text

            director_list = []
            list = json_data['director']
            for item in list:
                director = {}
                director['name'] = item['name']
                director['url'] = config.DOUBAN_MOVIE_BASE_URL + item['url']
                director_list.append(director)
            movie_info['director'] = director_list
            
            writer_list = []
            list = json_data['author']
            for item in list:
                writer = {}
                writer['name'] = item['name']
                writer['url'] = config.DOUBAN_MOVIE_BASE_URL + item['url']
                writer_list.append(writer)
            movie_info['writer'] = writer_list
                    
            cast_list = []
            list = json_data['actor']
            for item in list:
                cast = {}
                cast['name'] = item['name']
                cast['url'] = config.DOUBAN_MOVIE_BASE_URL + item['url']
                cast_list.append(cast)
            movie_info['cast'] = cast_list

            file_name = f'{movie_id}_movie_info.json'
            output_file = os.path.join(config.MOVIE_INFO_DIRECTORY, file_name)
            with open(output_file, mode='w', encoding='utf-8') as file:
                json.dump(movie_info, file, indent=4, ensure_ascii=False)
        
        # crawl rating
        rating_count = json_data['aggregateRating']['ratingCount']
        rating = {            
            'avg': json_data['aggregateRating']['ratingValue'],
            'count': json_data['aggregateRating']['ratingCount']
        }
        if int(rating_count) > 0:                
            rating_weight = {}
            e = chrome.find_element(By.CSS_SELECTOR, value='div[class="ratings-on-weight"]')
            es = e.find_elements(By.CSS_SELECTOR, value='div[class="item"]')
            es.reverse()
            
            i = 1
            while i <= 5:
                e = es[i-1].find_element(By.CSS_SELECTOR, value='span[class="rating_per"]')                
                key = str(i) + '_star'
                rating_weight[key] = e.text
                i += 1           
            rating['rating_weight'] = rating_weight

        date = datetime.now(config.TIME_ZONE).strftime("%Y-%m-%d")
        movie_rating[date] = rating
        file_name = f'{movie_id}_movie_rating.json'
        output_file = os.path.join(config.MOVIE_INFO_DIRECTORY, file_name)
        with open(output_file, mode='a', encoding='utf-8') as file:
            json.dump(movie_rating, file, indent=4, ensure_ascii=False)       
 
    except Exception as e:
        # Log the exception and error msg
        msg = f'Crawl movie info from \'{url}\' failed. The movie info crawl job for movie with id \'{movie_id}\' was CANCELLED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_INFO_CRAWLER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.MOVIE_INFO_CRAWLER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        #print(e)
    else:
        # Log the sucessful crawl information
        msg = f'Crawl movie info from \'{url}\' successfully.'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_INFO)
        util.log(msg, config.MOVIE_INFO_CRAWLER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_INFO)
        #print(e)
    finally:
        chrome.quit() # Exit the webbrowser
    
    return rating_start_date


#??? TESTING CODE
#crawl_movie_info(35268614, config.MOVIE_INFO_DIRECTORY, False)
#crawl_movie_info(35749842, config.MOVIE_INFO_DIRECTORY, False)
#crawl_movie_info(36847744, config.MOVIE_INFO_DIRECTORY, False)

