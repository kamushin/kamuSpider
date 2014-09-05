import tornado.ioloop
from tornado.options import options, define
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

import logging
import signal
from queue import Queue

from fetcher import Fetcher
from util import isValidScheme

fetch_queue = Queue()
fetched = []
fetch_finished = []

logger = logging.getLogger()


class AddCrawler(tornado.web.RequestHandler):
    "收到其他服务器加入集群的通知, 加入server_list"
    
    def get(self, ip):
        if ip not in server_list:
            server_list.append(ip)


class Crawler(tornado.web.RequestHandler):
    """接受其他服务器传递的需要抓取的URL, 并整理后加入队列"""

    @tornado.gen.coroutine
    def get(self, url):
        if not isValidScheme(url):
            logger.warning("not vaild_scheme")
            return

        logger.debug("get url: %s" % url)

        fetch_queue.put(url)    
        
        if fetch_queue.qsize() %100 == 0:
            logger.warning(fetch_queue.qsize())


start_url = ['http://jandan.net/tag/%E6%B2%A1%E5%93%81%E7%AC%91%E8%AF%9D%E9%9B%86']

server_list = ['http://127.0.0.1:8887']#, 'http://127.0.0.1:8888']

if __name__ == '__main__':
    tornado.options.parse_command_line()

    logger.info('Start up')
    app = tornado.web.Application([
            (r'/crawler/(.*)',Crawler),
            (r'/add_crawler/(.*)',AddCrawler),
        ], debug=True)

    port = 8887
    define("max_clients", default=500)
    define("timeout", default=60)
    app.listen(port)


    ioloop = tornado.ioloop.IOLoop.instance()
    fetch = Fetcher(ioloop, start_url=start_url)

    def on_shutdown():
    #监听ctrl+c 以保证在退出时保存fetched
        logging.info("save fetched")
        with open("fetched", "w") as f:
            for u in fetch.fetch_finished:
                f.write(u + '\n')

        ioloop.stop()

    signal.signal(signal.SIGINT, lambda sig, frame:ioloop.add_callback_from_signal(on_shutdown))
    
    ioloop.add_callback(fetch.run)
    
    ioloop.start()
