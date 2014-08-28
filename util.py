import lxml.html as H
import urllib.parse as urlparse
import os
import logging

class HtmlAnalyzer(object):
    '''分析HTML代码,得到下一步link'''

    @staticmethod
    def extract_links(html, base_ref, tags=[]):
        '''
        提取出html中的link
        base_ref 把相对路径转换为绝对路径
        tags 需要提取的link所在的标签
        '''

        if not html.strip():
            return

        try:
            doc = H.document_fromstring(html)
        except:
            return 

        default_tags = ['a', 'iframe', 'frame']
        default_tags.extend(tags)
        default_tags = set(default_tags)
        ignore_ext = ['js', 'css', 'png', 'jpg', 'gif', 'bmp', 'svg', 'jpeg', 'exe', 'rar', 'zip']

        doc.make_links_absolute(base_ref)
        links_in_doc = doc.iterlinks()
        for link in links_in_doc:
            if link[0].tag in default_tags:
                link_ext = os.path.splitext(urlparse.urlsplit(link[2]).path)[-1][1:]
                if link_ext not in ignore_ext:
                    yield link[2]
