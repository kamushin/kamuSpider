import tornado.ioloop
from tornado.options import options, define
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient

import logging
import datetime
import signal
import sys
from functools import partial
from queue import Queue

from util import HtmlAnalyzer

fetch_queue = Queue()
fetched = []
fetch_finished = []

logger = logging.getLogger()

def checkUrlScheme(url):
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'http://' + url
    
    return url

@tornado.gen.coroutine
def send(url):
    '''
    把 url hash后传递给对应的服务器去抓取
    '''
    fetch_queue.put(url)
    #http_cilent = AsyncHTTPClient()
    
    #hashed = hash(url) % len(server_list)

    #target_url = server_list[hashed]+'/crawler/'+url

    #logger.debug("target_url: %s" % target_url)

    #yield http_cilent.fetch(target_url)


class Crawler(tornado.web.RequestHandler):
    """接受其他服务器传递的需要抓取的URL, 并整理后加入队列"""

    @tornado.gen.coroutine
    def get(self, url):
        url = checkUrlScheme(url)

        logger.debug("get url: %s" % url)

        fetch_queue.put(url)    
        
        if fetch_queue.qsize() %100 == 0:
            logger.warning(fetch_queue.qsize())


start_url = ['http://jandan.net']

server_list = ['http://127.0.0.1:8887']#, 'http://127.0.0.1:8888']

class Fetcher(object):

    def __init__(self, ioloop, max_depth=5):
        object.__init__(self)
        for u in start_url:
            fetch_queue.put(u)

        self.ioloop = ioloop
        self.fetching = 0
        self.max_depth = max_depth

        # curl_httpclient is faster, it is said 
        #AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=options.max_clients)
        AsyncHTTPClient.configure(None, max_clients=options.max_clients)

    @tornado.gen.coroutine
    def fetch(self, url):
        '''
        抓取器
        '''
        http_cilent = AsyncHTTPClient()
        response = yield http_cilent.fetch(url)
        logger.debug("fetched url: %s" % url)

        return response

    @tornado.gen.coroutine
    def parse(self, response):
        '''
        解析URL, 保存结果, 传递新的URL
        '''
        url_gen = HtmlAnalyzer.extract_links(response.body, response.effective_url,[])
        
        yield [send(url) for url in url_gen]


    @tornado.gen.coroutine
    def do_work(self, url):
        url = checkUrlScheme(url)
        try:
            response = yield self.fetch(url)
        except tornado.httpclient.HTTPError as e:
            import traceback
            traceback.print_exc()
            logger.error("Url: %s HTTPError: %s "% (url,e.code))
        except:
            import traceback
            traceback.print_exc()
            logger.error("Unknow error with url: %s" % url)
        else:
            yield self.parse(response)
            fetch_finished.append(url)
            self.fetching -= 1

    def run(self):
        '''
        Get url from fetch_queue to fetch
        '''

        while not fetch_queue.empty() and self.fetching <= options.max_clients / 2:
            
            url = fetch_queue.get()
            if url in fetched:
                continue
            else:
                fetched.append(url)
                self.fetching += 1
             
            ioloop.add_callback(self.do_work, url)

        ioloop.add_timeout(datetime.timedelta(seconds=1), self.run)


if __name__ == '__main__':
    tornado.options.parse_command_line()

    logger.info('Start up')
    app = tornado.web.Application([
            (r'/crawler/(.*)',Crawler),
        ], debug=True)

    port = 8887
    define("max_clients", default=500)
    app.listen(port)


    ioloop = tornado.ioloop.IOLoop.instance()
    def on_shutdown():
    #监听ctrl+c 以保证在退出时保存fetched
        logging.info("save fetched")
        with open("fetched", "w") as f:
            for u in fetch_finished:
                f.write(u + '\n')

        ioloop.stop()

    signal.signal(signal.SIGINT, lambda sig, frame:ioloop.add_callback_from_signal(on_shutdown))
    fetch = Fetcher(ioloop)
    
    ioloop.add_callback(fetch.run)
    
    ioloop.start()
