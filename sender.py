import tornado.gen

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
