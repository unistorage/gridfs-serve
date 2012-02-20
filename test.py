# -*- coding: utf-8 -*-
__author__ = 'artem'
import urllib2
headers = {'Range':'bytes=1-100'}
req = urllib2.Request('http://127.0.0.1:5000/4f3389011eb37317dd000004', headers=headers)
resp = urllib2.urlopen(req)
print resp.headers
print resp.read()