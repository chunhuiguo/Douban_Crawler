from selenium import webdriver

CHROME_ANDROID_USER_AGENT = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36'
CHROME_DESKTOP_USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'


def test_mobile_chrome():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--log-level=3')

    chrome_options.add_argument(f'--user-agent={CHROME_ANDROID_USER_AGENT}')
    url = "https://m.douban.com/home_guide"

    chrome = webdriver.Chrome(options=chrome_options)

    chrome.get(url)
    print(chrome.title)

    chrome.quit()



def test_desktop_chrome():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--log-level=3')

    chrome_options.add_argument(f'--user-agent={CHROME_DESKTOP_USER_AGENT}')
    url = "https://www.douban.com/"

    chrome = webdriver.Chrome(options=chrome_options)

    chrome.get(url)
    print(chrome.title)

    chrome.quit()





test_desktop_chrome()
test_mobile_chrome()



