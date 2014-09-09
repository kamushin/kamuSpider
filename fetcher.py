import tornado.gen
import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import options

import logging
import datetime
import os
from pybloom import ScalableBloomFilter
from queue import Queue

from util import HtmlAnalyzer, isValidScheme, Singleton
from sender import Sender

logger = logging.getLogger()


class Fetcher(metaclass=Singleton):

    def __init__(self, ioloop=None, start_url=None, max_depth=5):
        super().__init__() 
        
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.start_url = start_url or {}
        self.fetch_queue = Queue()
        self.fetched = []
        self.fetched_filter = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        self.fetch_finished = []

        for u in start_url:
            self.fetch_queue.put(u)

        self.fetching = 0
        self.max_depth = max_depth


    def add_url(self, url):
        if not isValidScheme(url):
            logger.warning("not vaild_scheme")
            return

        logger.debug("get url: %s" % url)

        self.fetch_queue.put(url)    

    @tornado.gen.coroutine
    def fetch(self, url):
        '''
        抓取器
        '''
        http_cilent = AsyncHTTPClient()
        request = HTTPRequest(url=url.encode('utf-8'), connect_timeout=options.timeout, request_timeout=options.timeout)
        response = yield http_cilent.fetch(request)
        logger.debug("fetched url: %s" % url)

        return response

    def parse(self, response):
        '''
        解析URL, 保存结果, 传递新的URL
        '''
        
        #self.save_tofile(response)

        url_gen = HtmlAnalyzer.extract_links(response.body, response.effective_url,[])

        return url_gen
        

    def save_tofile(self, response):
        '''
        暂时使用blocking的f.write代替db
        这里的io比较快,影响不大
        '''
        path = response.effective_url.split('/')[-1] 
        if path is None or path is "":
            path = response.effective_url.split('/')[-2]
        try:
            with open(os.path.join("tmp", path), "a") as f:
                f.write(response.effective_url+'\n')
                f.write(str(response.body) + '\n')
        except:
            logger.error("path %s" % path)


    @tornado.gen.coroutine
    def do_work(self, url):
        if not isValidScheme(url):
            logger.warning("not vaild_scheme")
            return None

        try:
            response = yield self.fetch(url)

        except tornado.httpclient.HTTPError as e:
            #import traceback
            #traceback.print_exc()

            #TODO
            #Some bug here. Too many file open.

            #with open('httperror.txt', "a") as f:
                #f.write("Url: %s HTTPError: %s \n"% (url,e.code))

            logger.error("Url: %s HTTPError: %s "% (url,e.code))

        except:
            import traceback
            traceback.print_exc()
            logger.error("Unknow error with url: %s" % url)

        else:
            url_gen = self.parse(response)
            self.fetch_finished.append(url)

            sender = Sender()
            for u in url_gen:
                sender.add_url(u)
            logging.info("fetched %s" % url)

        self.fetching -= 1

    def run(self):
        '''
        Get url from fetch_queue to fetch
        '''

        logging.error("fetching: %s " % self.fetching)
        while not self.fetch_queue.empty() and self.fetching <= options.max_fetch_clients:
            
            url = self.fetch_queue.get()
            if url in self.fetched_filter:
                logging.info("url in fetched_filter")
                continue
            else:
                self.fetched_filter.add(url)
                self.fetched.append(url)
                self.fetching += 1
             
            self.ioloop.add_callback(self.do_work, url)

        self.ioloop.add_timeout(datetime.timedelta(seconds=1), self.run)
