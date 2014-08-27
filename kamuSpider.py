import tornado.ioloop
import tornado.options
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

logger = logging.getLogger()

def checkUrlScheme(url):
    if not url.startswith('http://'):
        url = 'http://' + url
    
    return url

@tornado.gen.coroutine
def send(url):
    '''
    把 url hash后传递给对应的服务器去抓取
    '''
    http_cilent = AsyncHTTPClient()
    
    hashed = hash(url) % len(server_list)

    target_url = server_list[hashed]+'/crawler/'+url

    logger.debug("target_url: %s" % target_url)

    yield http_cilent.fetch(target_url)


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

    def __init__(self, ioloop):
        object.__init__(self)
        for u in start_url:
            fetch_queue.put(u)

        self.ioloop = ioloop

        AsyncHTTPClient.configure(None, max_clients=500)

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
        if url in fetched:
            return
        
        response = yield self.fetch(url)
        yield self.parse(response)

        fetched.append(url)

    def run(self):
        '''
        Get url from fetch_queue to fetch
        '''
        while not fetch_queue.empty():
            
            url = fetch_queue.get()
            ioloop.add_callback(self.do_work, url)

        ioloop.add_timeout(datetime.timedelta(seconds=1), self.run)


if __name__ == '__main__':
    tornado.options.parse_command_line()

    logger.info('Start up')
    app = tornado.web.Application([
            (r'/crawler/(.*)',Crawler),
        ], debug=True)

    port = 8887

    app.listen(port)


    ioloop = tornado.ioloop.IOLoop.instance()
    def on_shutdown():
    #监听ctrl+c 以保证在退出时保存fetched
        logging.info("save fetched")
        with open("fetched", "w") as f:
            for u in fetched:
                f.write(u + '\n')

        ioloop.stop()

    signal.signal(signal.SIGINT, lambda sig, frame:ioloop.add_callback_from_signal(on_shutdown))
    fetch = Fetcher(ioloop)
    
    ioloop.add_callback(fetch.run)
    
    ioloop.start()
