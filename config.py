from tornado.options import options, define

import yaml
import logging

def config():
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)
        for k,v in config.items():
            logging.info("%s:%s", k,v)
            define(k, default=v)
