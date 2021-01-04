#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import argparse
import requests
import os, codecs
from datetime import datetime 
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse, unquote, urlsplit, urlunsplit
from requests.exceptions import HTTPError

from bs4 import BeautifulSoup
import transcript as tc

import ssl
ssl._create_default_https_context = ssl._create_unverified_context


# In[2]:


def remove_query_from_url(url):
    parsed = urlparse(url)
    return "".join([parsed.scheme,"://",parsed.netloc,parsed.path])


# In[7]:


class Scheduler:

    def __init__(self, seed_url, num_crawler, whitelist_file_types, user_agent, whitelist_domain):
        self.seed_url = urljoin(seed_url, '/')[:-1]
        self.frontier_q = [seed_url]
        self.visited_q = []
        self.num_crawler = num_crawler
        self.whitelist_file_types = whitelist_file_types
        self.user_agent = user_agent
        self.whitelist_domain = whitelist_domain
        self.headers = {
            'User-Agent':  user_agent,
            'From': 'thitiwat.tha@ku.th'
        }
        self.parsed_sitemap_domains = []
        self.parsed_robots_domains = {}

    # @param 'links' is a list of extracted links to be stored in the queue
    def enqueue(self, links):
        for link in links:
            if link not in self.frontier_q and link not in self.visited_q:
                self.frontier_q.append(link)

    # FIFO queue
    def dequeue(self):
        current_url = self.frontier_q[0]
        self.frontier_q = self.frontier_q[1:]
        return current_url

    def isQueueEmpty(self):
        return len(self.frontier_q) == 0
    
    def saveDomain(self, filename, urls):
        domains = []
        for url in urls:
            parsed = urlparse(url)
            hostname = parsed.hostname
            domain = re.sub(r'www.', '', hostname)
            domains.append(domain)
        domains = list(set(domains))
        with open(filename, 'w') as f:
            f.write('\n'.join(domains))

    def link_parser(self, raw_html):
        soup = BeautifulSoup(raw_html, 'html.parser')
        urls = []
        for link in soup.find_all('a', href=True):
            urls.append(link.get('href'))
        # pattern = '<a href="([^"]*)"'
        # urls = re.findall(pattern, raw_html)
        return urls

    def de_duplicate_urls(self, urls):
        return list(set(urls))
    
    def get_base_url(self, url):
        return urljoin(url, '/')[:-1]

    def save_to_disk(self, url, raw_html):
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            url_path = parsed.path

            # print(f'hostname {hostname}')
            # save_folder_path = 'html/' + hostname + url_path
            # save_filename = 'index.html'
            filetype = re.match(r'.*\.(.*)$', url_path)

            if(filetype != None):
                # urljoin 'http://localhost:8888/asdasd/htasd.html' => 'http://localhost:8888/asdasd/'
                save_folder_path = 'html/' + hostname + "/".join(url_path.split('/')[:-1])
                save_filename = url.split(urljoin(url, '.'))[1]
            else:
                save_folder_path = 'html/' + hostname + url_path
                save_filename = 'index.html'
            
            save_abs_path = save_folder_path + '/' + save_filename

            print(f'savepath: {save_folder_path}')
            print(f'save filename: {save_filename}')
            print(f'save_abs_path: {save_abs_path}')

            os.makedirs(save_folder_path, 0o755, exist_ok=True)
            f = codecs.open(save_abs_path, 'w', 'utf-8')
            f.write(raw_html)
            f.close()
        except:
            print(f'Error in save_to_disk')

    def normalization_urls(self, urls, base_url):
        try:
            # absolute
            urls = [urljoin(base_url, url) for url in urls]

            # remove # (self reference)
            urls = [re.sub(r'#.*', '', url) for url in urls]

            # parse to utf8
            urls = [unquote(url) for url in urls]

            # strip / (backslash)
            urls = [url.strip('/') for url in urls]

            # remove query string
            urls = [remove_query_from_url(url) for url in urls]

            return urls
        except:
            print(f'Error in normalization_urls')

    def get_raw_html(self, url):
        text = ''
        try:
            response = requests.get(url, headers=self.headers, timeout=30, verify=False)
            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6
        else:
            # print('Success!')
            text = response.text
        finally:
            return text

    def check_and_save_robots(self, robot_url):
        try:
            raw = self.get_raw_html(robot_url)
            is_valid = 'User-agent:' in raw
            if(is_valid):
                self.save_to_disk(robot_url, raw)
            return is_valid
        except:
            print(f'Error in check_and_save_robots')
        finally:
            return False

    def get_parsed_robots(self, base_url):
        rp = RobotFileParser()
        robots_url = urljoin(base_url, 'robots.txt')
        try:
            is_valid = self.check_and_save_robots(robots_url)
            if(not is_valid):
                raise Exception("Not found robots")

            if(base_url in self.parsed_robots_domains.keys()):
                rp = self.parsed_robots_domains[base_url]
            else:
                rp.set_url(robots_url)
                rp.read()
                self.parsed_robots_domains[base_url] = rp
        except:
            # allow all
            rp.set_url('https://ku.ac.th')
            rp.read()
            self.parsed_robots_domains[base_url] = rp
        finally:
            return rp


    def filters_urls(self, urls, base_url):
        filtered_urls = []
        rp = self.get_parsed_robots(base_url)
        for url in urls:
            parsed = urlparse(url)
            url_path = parsed.path
            hostname = parsed.hostname
            # check domain allow only ku.ac.th
            if(not hostname or self.whitelist_domain not in hostname):
                continue

            # check can fetch from robots.txt
            can_fetch = rp.can_fetch(self.user_agent, url)
            if(not can_fetch):
                continue

            # check filetype
            filetype = re.match(r'.*\.(.*)$', url_path)

            if(not filetype):
                filtered_urls.append(url)
            elif(filetype[1] in self.whitelist_file_types):
                filtered_urls.append(url)
            else:
                pass

        return filtered_urls

    def include_urls(self, urls, base_url):
        try:
            if(base_url in self.parsed_sitemap_domains):
                return urls
            else:
                self.parsed_sitemap_domains.append(base_url)

            xml = self.get_raw_html(base_url + '/sitemap.xml')
            soup = BeautifulSoup(xml)
            urlsetTag = soup.find_all("loc")
            sitemap_urls = [url.getText() for url in urlsetTag]
            urls[0:0] = sitemap_urls
            return urls
        except:
            print(f'Error in include_urls')
        finally:
            return urls

    def crawler_url(self, url):
        print(f'crawler: {url}')
        base_url = self.get_base_url(url)

        # Downloader
        raw_html = self.get_raw_html(url)

        # Analyzer
        urls = self.link_parser(raw_html)
        urls = self.include_urls(urls, base_url)
        urls = self.normalization_urls(urls, base_url)
        urls = self.filters_urls(urls, base_url)
        urls = self.de_duplicate_urls(urls)

        # store to disk
        self.save_to_disk(url, raw_html)
        
        return urls


    def run(self):
        #--- main process ---#
        cur = 0
        start_time = datetime.now()
        tc.start('output.txt')
        while(not self.isQueueEmpty() and cur < self.num_crawler):
            print(f'i: {cur}')
            current_url = self.dequeue()
            self.visited_q.append(current_url)

            urls = self.crawler_url(current_url)
            self.enqueue(urls)
            # print(urls)
            cur += 1
            print('\n')
            # print(self.frontier_q)
            # print(self.visited_q)
        time_elapsed = datetime.now() - start_time
        tc.stop()
        print('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))
        self.saveDomain('list_robots.txt', self.parsed_robots_domains.keys())
        self.saveDomain('list_sitemap.txt', self.parsed_sitemap_domains)
        # print(self.parsed_sitemap_domains)
        # print(self.parsed_robots_domains)


# In[ ]:


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num', help='number of crawlers', default=10000)
    args = parser.parse_args()

    num_crawler = int(args.num)
    Scheduler(
        seed_url ='https://ku.ac.th/th',
        num_crawler = num_crawler,
        whitelist_file_types = ['html', 'htm'],
        whitelist_domain = 'ku.ac.th',
        user_agent = "Thitiwat_Bot"
    ).run()


# In[8]:


# num_crawler = 5
# crawler = Scheduler(
#     seed_url ='https://login.ku.ac.th',
#     num_crawler = num_crawler,
#     whitelist_file_types = ['html', 'htm'],
#     whitelist_domain = 'ku.ac.th',
#     user_agent = "Oporbot"
# )
# crawler.run()
# raw = crawler.save_to_disk('https://www.pr.ku.ac.th/asd/robots.txt', '')


# In[ ]:




