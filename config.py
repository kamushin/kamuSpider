from tornado.options import options, define
from tornado.httpclient import AsyncHTTPClient

import yaml
import logging

def config():
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)
        for k,v in config.items():
            logging.info("%s:%s", k,v)
            define(k, default=v)

    # curl_httpclient is faster, it is said 

    max_clients = options.max_fetch_clients + options.max_send_clients
    AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=max_clients)
    #AsyncHTTPClient.configure(None, max_clients=max_clients)
