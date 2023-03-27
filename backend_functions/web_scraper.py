from google.cloud import language_v1
import requests
import tldextract as tld
from bs4 import BeautifulSoup as bs
from bs4.element import Comment
import re
from firebase_admin import firestore

class WebScraper:
    '''
    Class for web scraping
    Initial development: Jan 2021
    '''

    def get_soup(self, target_url):
        '''
        Returns a BeautifulSoup representation of the given url.
        '''
        try:
            body = requests.get(target_url, timeout=2)
        except:
            return None
        soup = bs(body.text, 'html.parser')
        return soup


    def get_text(self, soup):
        '''
        Returns all visible text on a page.
        '''
        texts = soup.findAll(text=True)

        visible = " ".join([str(text).strip() for text in texts if self.tag_visible(text)])

        #visible = str(visible.encode('ascii', errors='ignore').decode('utf-8'))
        return visible


    def tag_visible(self, element):
        '''
        Returns true if a given bs4 element is a visible tag, otherwise returns false.
        '''
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta',
                                   '[document]', 'nav', 'href', 'form']:
            return False
        if isinstance(element, Comment):
            return False
        if re.match(r"[\s\r\n]+", str(element)):
            return False
        return True

    
    def valid_links(self, href):
        '''
        Returns true if href directs to a valid internal link, otherwise returns false.
        '''
        ignore_values = {"", "/"}

        if not href:
            return False

        if "http" in href:
            return False
        
        if href.startswith('#'):
            return False

        if href in ignore_values:
            return False
        
        return True


    def scrape(self, target_url, single_page=True, depth=1):
        '''
        INPUTS
        target_url: full url of target website
        single_page: True - only scrape the target url
                     False - scrape target url and all links from the same domain
        MODIFIES
        N/A
        RETURNS
        cleaned_text: cleaned text string of a all scraped website
        '''
        if 'http' not in target_url:
            target_url = 'https://' + target_url

        if single_page:
            soup = self.get_soup(target_url)
            return self.get_text(soup)

        (subdomain, domain, suffix) = tld.extract(target_url)

        if subdomain != "":
            base_url = subdomain + "." + domain + "." + suffix
        else:
            base_url = domain + "." + suffix

        try:
            body = requests.get(target_url, timeout=2)
        except:
            return ""
        
        soup = bs(body.text, 'html.parser')
        links = ["http://" + base_url + a['href'] for a in soup.find_all("a", href=self.valid_links)]

        text = [self.get_text(soup)]
        count = 0
        for link in links:
            #print(link)
            soup = self.get_soup(link)
            if soup is not None:
                text.append(self.get_text(soup))
            count += 1
            if count > 10:
                break
        return " ".join(text)

        print(links)


if __name__ == '__main__':
    import time
    url = "https://coopsight.com/"
    url = 'coopsight.com'
    ws = WebScraper()
    start = time.time()
    text = ws.scrape(url, False)
    print('completed in', time.time()-start)
    print(text)
