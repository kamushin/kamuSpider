import tornado.ioloop
from tornado.options import options, define
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

import logging
import signal

from fetcher import Fetcher
from config import config
from sender import Sender

logger = logging.getLogger()


class AddCrawler(tornado.web.RequestHandler):
    "收到其他服务器加入集群的通知, 加入server_list"
    
    def get(self, ip):
        if ip not in options.server_list:
            options.server_list.append(ip)


class Crawler(tornado.web.RequestHandler):
    """接受其他服务器传递的需要抓取的URL, 并整理后加入队列"""

    def get(self, url):
        logger.info("get url %s" % url)
        fetch = Fetcher()
        fetch.add_url(url)


if __name__ == '__main__':
    tornado.options.parse_command_line()
    config()

    logger.info('Start up')
    app = tornado.web.Application([
            (r'/crawler/(.*)',Crawler),
            (r'/add_crawler/(.*)',AddCrawler),
        ], debug=True)

    app.listen(options.port)


    ioloop  = tornado.ioloop.IOLoop.instance()
    sender  = Sender(ioloop, server_list=options.server_list)
    fetcher = Fetcher(ioloop, start_url=options.start_url)

    def on_shutdown():
    #监听ctrl+c 以保证在退出时保存fetched
        logging.info("save fetched")
        with open("ed_fetched", "w") as f:
            for u in fetcher.fetch_finished:
                f.write(u + '\n')

        ioloop.stop()

    signal.signal(signal.SIGINT, lambda sig, frame:ioloop.add_callback_from_signal(on_shutdown))
    
    ioloop.add_callback(fetcher.run)
    ioloop.add_callback(sender.run)
    
    ioloop.start()
