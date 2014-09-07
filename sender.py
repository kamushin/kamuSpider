import tornado.gen
from cohash import Hash

from util import Singleton

class Sender(object):
    '''
        URL分发器类
        兼顾用一致性hash实现的负载均衡
    '''

    __metaclass__ = Singleton

    pass


@tornado.gen.coroutine
def send(fetch_queue, url):
    '''
    把 url hash后传递给对应的服务器去抓取
    '''
    fetch_queue.put(url)
    #http_cilent = AsyncHTTPClient()
    
    #hashed = hash(url) % len(server_list)

    #target_url = server_list[hashed]+'/crawler/'+url

    #logger.debug("target_url: %s" % target_url)

    #yield http_cilent.fetch(target_url)
