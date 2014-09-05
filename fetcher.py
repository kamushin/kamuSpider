import tornado.gen
import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import options

import logging
import datetime
import os
from queue import Queue

from util import HtmlAnalyzer, isValidScheme
from sender import send

logger = logging.getLogger()


class Fetcher(object):

    def __init__(self, ioloop, start_url=[], max_depth=5):
        object.__init__(self)
        
        Fetcher._instance = self

        self.start_url = start_url
        self.fetch_queue = Queue()
        self.fetched = []
        self.fetch_finished = []

        for u in start_url:
            self.fetch_queue.put(u)

        self.ioloop = ioloop
        self.fetching = 0
        self.max_depth = max_depth

        # curl_httpclient is faster, it is said 
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=options.max_clients)
        #AsyncHTTPClient.configure(None, max_clients=options.max_clients)

    @staticmethod
    def instance():
        if not hasattr(Fetcher, "_instance"):
            Fetcher._instance = Fetcher(tornado.ioloop.IOLoop.instance())

        return Fetcher._instance
    

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
        
        #yield self.save_tofile(response)

        url_gen = HtmlAnalyzer.extract_links(response.body, response.effective_url,[])

        return url_gen
        

    @tornado.gen.coroutine
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
            import traceback
            traceback.print_exc()
            with open('httperror.txt', "a") as f:
                f.write("Url: %s HTTPError: %s \n"% (url,e.code))
            logger.error("Url: %s HTTPError: %s "% (url,e.code))
        except:
            import traceback
            traceback.print_exc()
            logger.error("Unknow error with url: %s" % url)
        else:
            url_gen = self.parse(response)
            self.fetch_finished.append(url)
            self.fetching -= 1
            yield [send(self.fetch_queue, url) for url in url_gen]
            logging.error("fetched %s" % url)

    def run(self):
        '''
        Get url from fetch_queue to fetch
        '''

        while not self.fetch_queue.empty() and self.fetching <= options.max_clients / 2:
            
            url = self.fetch_queue.get()
            if url in self.fetched:
                continue
            else:
                self.fetched.append(url)
                self.fetching += 1
             
            self.ioloop.add_callback(self.do_work, url)

        self.ioloop.add_timeout(datetime.timedelta(seconds=1), self.run)
