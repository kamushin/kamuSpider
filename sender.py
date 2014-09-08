import tornado.gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.options import options
from queue import Queue
from cohash import Hash

import logging
import datetime
from util import Singleton
import fetcher

class Sender(metaclass=Singleton):
    '''
        URL分发器类
        兼顾用一致性hash实现的负载均衡
    '''


    def __init__(self, ioloop, server_list=[], replicas=20):
        super().__init__()
        
        self.ioloop = ioloop
        self.send_url_queue = Queue()
        self.sending = 0
        self.server_list = server_list
        if not self.server_list:
            raise ValueError("server_list is None.")

        self.replicas = replicas

        self.ring = Hash(self.server_list, replicas=self.replicas)

    def add_url(self, url):
        logging.debug("send url to queue %s" % url)

        self.send_url_queue.put(url)


    @tornado.gen.coroutine
    def send(self, server, url):
        '''
        把 url hash后传递给对应的服务器去抓取
        '''
        if server != options.local:
            1/0
            http_cilent = AsyncHTTPClient()
            
            target_url = 'http://'+ server+ '/crawler/'+ url

            logging.info("target_url: %s" % target_url)

            request = HTTPRequest(url=target_url.encode('utf-8'), connect_timeout=options.timeout, request_timeout=options.timeout)

            yield http_cilent.fetch(request)

        else:
            fetch = fetcher.Fetcher()
            fetch.fetch_queue.put(url)

    @tornado.gen.coroutine
    def do_work(self, url):
        logging.debug("sender do_work with url %s" % url)

        server = self.ring.get_node(url)

        try:
            yield self.send(server, url)
        except tornado.httpclient.HTTPError as e:
            import traceback
            traceback.print_exc()

            with open('httperrorwithServer.txt', "a") as f:
                f.write("Send Url: %s to Server:%s HTTPError: %s \n"% (url, server, e.code))

            logging.error("Send Url: %s to Server:%s HTTPError: %s \n"% (url, server, e.code))

        except:
            import traceback
            traceback.print_exc()
            logging.error("Send Url: %s to Server:%s Unknow Error\n"% (url, server))

        self.sending -= 1


    def run(self):
        '''
            Get url from send_url_queue to send to crawlers
        '''
        logging.error("sending: %s and %s urls waiting in queue" % (self.sending, self.send_url_queue.qsize()))

        while not self.send_url_queue.empty() and self.sending <= options.max_send_clients:
            url = self.send_url_queue.get()
            
            self.sending += 1
            
            self.ioloop.add_callback(self.do_work, url)

        self.ioloop.add_timeout(datetime.timedelta(seconds=1), self.run)

    
