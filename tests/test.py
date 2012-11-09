import time
import os.path
import unittest
from pprint import pprint
from StringIO import StringIO
from datetime import datetime, timedelta

from bson.objectid import ObjectId
from webtest import TestApp
from gridfs import GridFS

import settings
import utils
from app import app


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = TestApp(app)
        cls.db = utils.get_mongodb_connection()[settings.MONGO_DB_NAME]
        cls.fs = GridFS(cls.db)

    def put_file(self, path):
        f = open(path, 'rb')
        filename = os.path.basename(path)
        return self.fs.put(f.read(), filename=filename)

    def get_file(self, _id, headers={}):
        r = self.app.get('/%s' % _id, headers=headers)
        content = StringIO(r._app_iter[0])
        return content

    def test(self):
        file_path = './tests/jpg.jpg'
        file_id = self.put_file(file_path)
        
        r = self.app.get('/%s' % file_id)
        content = StringIO(r._app_iter[0])
        self.assertEquals(open(file_path).read(), content.read())
        
        self.app.get('/%s/' % file_id, status=404)

    def test_range(self):
        file_path = './tests/jpg.jpg'
        file_id = self.put_file(file_path)
        
        content = self.get_file(file_id, headers={'Range': 'bytes=0-399'})
        self.assertEquals(open(file_path).read(400), content.read())
        
        content = self.get_file(file_id, headers={'Range': 'bytes=400-449'})
        f = open(file_path)
        f.seek(400)
        self.assertEquals(f.read(50), content.read())

        content = self.get_file(file_id, headers={'Range': 'bytes=400-'})
        f = open(file_path)
        f.seek(400)
        self.assertEquals(f.read(), content.read())

    def test_conditional_get(self):
        file_path = './tests/jpg.jpg'
        file_id = self.put_file(file_path)
        
        content = self.app.get('/%s' % file_id)
        self.assertEquals(content.status_code, 200)
        etag = content.headers['ETag']

        time.sleep(1)

        modified_header = (datetime.now() - timedelta(milliseconds=500)) \
                .strftime('%a, %d %b %Y %H:%M:%S +0000')
        
        content = self.app.get('/%s' % file_id, headers={'If-Modified-Since': modified_header})
        self.assertEquals(content.status_code, 304)
        
        content = self.app.get('/%s' % file_id, headers={'If-None-Match': etag})
        self.assertEquals(content.status_code, 304)

    def test_404(self):
        r = self.app.get('/12345678912346789012345', status='*')
        self.assertEquals(r.status_code, 404)
