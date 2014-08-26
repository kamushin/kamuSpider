import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient

import logging
import datetime
import sys
from functools import partial
from queue import Queue

from util import HtmlAnalyzer

fetch_queue = Queue()

def checkUrlScheme(url):
    if not url.startswith('http://'):
        url = 'http://' + url
    
    return url

@tornado.gen.coroutine
def send(url):
    '''
    把 url hash后传递给对应的服务器去抓取
    '''
    logging.info(url)

    http_cilent = AsyncHTTPClient()
    
    hashed = hash(url) % len(server_list)

    target_url = server_list[hashed]+'/crawler/'+url

    logging.info("target_url: %s" % target_url)

    yield http_cilent.fetch(target_url)


class Crawler(tornado.web.RequestHandler):
    """接受其他服务器传递的需要抓取的URL, 并整理后加入队列"""

    @tornado.gen.coroutine
    def get(self, url):
        url = checkUrlScheme(url)

        logging.info("get url: %s" % url)

        fetch_queue.put(url)    
        self.write(url)


start_url = ['http://www.baidu.com']

server_list = ['http://127.0.0.1:8887', 'http://127.0.0.1:8888']

class Fetcher(object):

    def __init__(self, ioloop):
        object.__init__(self)
        for u in start_url:
            fetch_queue.put(u)

        self.ioloop = ioloop

    @tornado.gen.coroutine
    def fetch(self, url):
        '''
        抓取器
        '''
        http_cilent = AsyncHTTPClient()
        response = yield http_cilent.fetch(url)
        logging.info("fetched url: %s" % url)

        return response

    @tornado.gen.coroutine
    def parse(self, response):
        '''
        解析URL, 保存结果, 传递新的URL
        '''
        logging.info(response.code)
        url_gen = HtmlAnalyzer.extract_links(response.body, response.effective_url,[])
        
        yield [send(url) for url in url_gen]


    @tornado.gen.coroutine
    def do_work(self, url):
        logging.info('do_work')
        response = yield self.fetch(url)
        yield self.parse(response)

    def run(self):
        logging.info("run")

        while not fetch_queue.empty():
            logging.info("not empty")
            
            url = fetch_queue.get()
            ioloop.add_callback(self.do_work, url)

        ioloop.add_timeout(datetime.timedelta(seconds=1), self.run)



if __name__ == '__main__':
    tornado.options.parse_command_line()
    logging.info('Start up')
    app = tornado.web.Application([
            (r'/crawler/(.*)',Crawler),
        ], debug=True)

    port = sys.argv[1]
    app.listen(port)

    ioloop = tornado.ioloop.IOLoop.instance()
    fetch = Fetcher(ioloop)
    
    ioloop.add_callback(fetch.run)
    
    ioloop.start()
