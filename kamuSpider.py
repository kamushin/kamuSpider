import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen
from tornado.httpclient import AsyncHTTPClient

import logging
#from queue import Queue

#fetch_queue = Queue()

def checkUrlScheme(url):
    if not url.startswith('http://'):
        url = 'http://' + url
    
    return url


def send(url):
    '''
    把 url hash后传递给对应的服务器去抓取
    '''
    pass


class Crawler(tornado.web.RequestHandler):
    """接受其他服务器传递的需要抓取的URL, 并整理后传递给抓取器"""

    @tornado.gen.coroutine
    def get(self, url):

        url = checkUrlScheme(url)

        logging.info("get url: %s" % url)

        response = yield self.fetch(url)

        self.parse(response)

    @tornado.gen.coroutine
    def fetch(self, url):
        '''
        抓取器
        '''
        
        http_cilent = AsyncHTTPClient()

        response = yield http_cilent.fetch(url)

        logging.info("fetched url: %s" % url)

        return response

    
    def parse(self, response):
        '''
        解析URL, 保存结果, 传递新的URL
        '''
        logging.info(response.body)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    logging.info('Start up')
    app = tornado.web.Application([
            (r'/crawler/(.*)',Crawler),
        ], debug=True)

    app.listen(8887)
    tornado.ioloop.IOLoop.instance().start()
