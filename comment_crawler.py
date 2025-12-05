'''The CommentCrawler Module

Summary
-------
This module defines functions to crawl and parse short review comments (短評) of a movie/TV-series.
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

import config
import util


def parse(movie_id, comment_elems):
    '''Parse comment data

    Parameters
    ----------
    movie_id: str
        The id of the movie/TV-series of the comments
    comment_elems: list
        The list of comment <li> elements (selenium.webdriver.remote.webelement.WebElement) to be parsed

    Returns
    -------
    list
        A list of dicts containing comment data
        Each comment dict contains following keys:
        -- user_url
        -- user_name
        -- rating_stars
        -- comment_timestamp
        -- comment_content
        -- comment_like_ct
    '''

    # the return list of comment dicts
    comments = []

    # each comment <li> element contains:
    # -- a <div class="desc"> element
    # -- a <div class="comment-content"> element
    # -- a <div class="btn-info"> element
    for comment_elem in comment_elems:
        # a <div class="desc"> element contains:
        # -- the url of the user's page
        # -- the user name
        # -- the user's rating score (out of 5 stars)
        # -- the timestamp of the comment
        desc_elem = comment_elem.find_element(By.CLASS_NAME, value='desc')
        user_url = desc_elem.find_element(By.TAG_NAME, value='a').get_attribute('href').strip()
        user_name = desc_elem.find_element(By.CLASS_NAME, value='user-name').text.strip()
        rating_stars = int(desc_elem.find_element(By.CLASS_NAME, value='rating-stars').get_attribute('data-rating').strip())
        comment_timestamp = desc_elem.find_element(By.CLASS_NAME, value='date').text.strip()
        
        # a <div class="comment-content"> element contains:
        # -- the content of the comment
        comment_content_elem = comment_elem.find_element(By.CLASS_NAME, value='comment-content')
        comment_content = comment_content_elem.find_element(By.TAG_NAME, value='p').text.strip()

        # a <div class="btn-info"> element contains:
        # -- the like count (by other users) of the comment
        btn_info_elem = comment_elem.find_element(By.CLASS_NAME, value='btn-info')
        comment_like_ct = int(btn_info_elem.find_element(By.CLASS_NAME, value='text').text.strip())

        # assemble comment data
        comment = {
            'movie_id': movie_id,
            'user_url': user_url,
            'user_name': user_name,
            'rating_stars': rating_stars,
            'comment_timestamp': comment_timestamp,
            'comment_content': comment_content,
            'comment_like_ct': comment_like_ct
        }
        comments.append(comment)

    return comments


def save_data_as_json(movie_id, comments):
    '''Save comment data as a json file

    Parameters
    ----------
    movie_id: str
        The id of the movie/TV-series associated with the comments to be saved
    comments: list
        The list of comment dicts to be saved
        Each comment dict contains following keys:
        -- user_url
        -- user_name
        -- rating_stars
        -- comment_timestamp
        -- comment_content
        -- comment_like_ct
    
    Returns
    -------
    str
        The full path of the json file
    '''

    now = datetime.now(config.TIME_ZONE)
    date_str = now.strftime("%Y-%m-%d")
    timestamp_str = now.strftime("%H.%M.%S.%f")

    output_file = config.COMMENT_CRAWLED_FILE.format(movie_id=movie_id, date_str=date_str, timestamp_str=timestamp_str)

    with open(output_file, mode='w', encoding='utf-8') as file:
        json.dump(comments, file, indent=4, ensure_ascii=False)
    
    return output_file


def crawl_comment(movie_id, comment_start_index, crawl_total_comment_count):
    '''Crawl data of a movie/TV-series comment webpage

    Parameters
    ----------
    movie_id: int
        The id of the movie/TV-series to crawl comments
    comment_start_index: int
        The start index of movie/TV-series comment to be crawled
    crawl_total_comment_count: bool
        ???
    
    Returns
    -------
    dict
        A dict with two keys:
        -- latest_comment_timestamp (the latest timestamp of comments crawled by this job)
        -- comment_count (the comment count crawled by this job)
        -- initial values: results = {'latest_comment_timestamp': None, 'comment_count': 0}
    '''

    # Initialize the return dict
    results = {
        'total_comment_count': 0,
        'current_page_comment_count': 0
    }
    

    # Set up Chrome options
    chrome_options = webdriver.ChromeOptions()
    # headless mode: no webbrowser GUI
    chrome_options.add_argument('--headless')
    # log-level=3: less Selenium log information (to be displayed in the terminal/console)
    chrome_options.add_argument('--log-level=3')
    # mobile browser user-agent: to access m.douban.com
    chrome_options.add_argument(f'--user-agent={config.CHROME_ANDROID_USER_AGENT}')

    # Get URL of the comment page to be crawled
    url = config.MOVIE_COMMENT_URL.format(movie_id=movie_id, comment_start_index = comment_start_index)
    #print(url)

    # Start to crawl comments, the crawl procedure is as follows:
    # (1) Open the comment webpage to be crawled
    # (2) Wait until all comments are dynamically loaded by JavaScript
    # (3) Expand all long comments, i.e., simulate clicking the '展开' on the webpage 
    # (4) Crawl and parse all comment data
    # (5) Exit the webbrowser
    # (6) Log the sucessful crawl information or any raised exception

    # Open a Chrome webbrowser instance
    #chrome = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    chrome = webdriver.Chrome(options=chrome_options)
    
    try:
        # Open the webpage to crawl comments
        chrome.get(url)
        # Wait for a maximum of CHROME_WAIT_SECONDS_COMMENT seconds to load the comment block
        # Raise a TimeoutException, if no element is found in that time (i.e., load comments FAIL)
        # The comment block is <div id="comment-list"> ... <ul class="list comment-list"> <li>...</li> ... </ul> </div>
        #     Refer to file './webpage_sample/comments-page-sample-SIMPLIFIED.html'
        # Note: For successfully loaded comment block with ZERO comment (ex: when comment_start_index is large)
        #       the <ul> exist, but NO <li> inside the <ul>.
        #       Refer to file './webpage_sample/comments-page-EMPTY-sample-SIMPLIFIED.html'
        chrome_wait = WebDriverWait(chrome, config.CHROME_WAIT_SECONDS_COMMENT)
        comment_ul_elem = chrome_wait.until(ExpectedConditions.presence_of_element_located((By.CSS_SELECTOR, '#comment-list ul')))

        # crawl the total count of comments
        if crawl_total_comment_count:
            total_comment_count_elem = chrome.find_element(By.CSS_SELECTOR, value='h1[class="title"]')
            total_comment_count = int(total_comment_count_elem.text.replace('(', '').replace(')', '').split()[1])
            results['total_comment_count'] = total_comment_count

        # Expand all long comments, i.e., simulate clicking the '展开' on the webpage
        expand_link_elems = comment_ul_elem.find_elements(By.CLASS_NAME, value='LinesEllipsis-readmore')
        for expand_link in expand_link_elems:
            expand_link.click()
        
        # Find all comment <li> elements and parse/store comment data (if any)
        comment_li_elems = comment_ul_elem.find_elements(By.TAG_NAME, value='li')
        comments = [] # Initialize the list of dicts containing comment data
        
        if len(comment_li_elems) > 0:
            comments = parse(movie_id, comment_li_elems)
            json_file = save_data_as_json(movie_id, comments)
            results['current_page_comment_count'] = len(comments)   

    except Exception as e:
        # Log the exception and error msg
        msg = f'Crawl comments from \'{url}\' failed. The comment crawl job for movie with id \'{movie_id}\' was CANCELLED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.COMMENT_CRAWLER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        util.log(msg, config.COMMENT_CRAWLER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_ERROR)
        #print(f'ERROR: --Comment Crawler-- Crawl comments from \'{url}\' failed. See log for details.\n')
    else:
        # Log the sucessful crawl information
        if len(comments) > 0: # There are comments on the requested webpage
            msg = f'Crawl {len(comments)} comments from \'{url}\' successfully. Data saved in \'{json_file}\' file.'
        else: # No comments on the requested webpage
            msg = f'Crawl {len(comments)} comments from \'{url}\' successfully. There is no more comment to crawl. The comment crawl job for movie with id \'{movie_id}\' stopped.'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_INFO)
        util.log(msg, config.COMMENT_CRAWLER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_INFO)
        #print(f'INFO: --Comment Crawler-- Crawl {len(comments)} comments from \'{url}\' successfully. See log for details.\n')
    finally:
        chrome.quit() # Exit the webbrowser
    
    return results


#??? TESTING CODE
#crawl_comment(35633650, 100, config.COMMENT_CRAWLED_DIRECTORY, True)
#rawl_comment(35633650, 233, config.COMMENT_CRAWLED_DIRECTORY, False)
#crawl_comment(36847744, 20, config.COMMENT_CRAWLED_DIRECTORY, True)