# coding: utf-8
import time
import os.path
import unittest
from StringIO import StringIO
from datetime import datetime, timedelta

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

        time.sleep(2)

        last_modified = time.strptime(content.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
        d = datetime.fromtimestamp(time.mktime(last_modified)) + timedelta(seconds=1)
        if_modified_header = d.strftime('%a, %d %b %Y %H:%M:%S GMT')

        content = self.app.get('/%s' % file_id, headers={'If-Modified-Since': if_modified_header})
        self.assertEquals(content.status_code, 304)

        content = self.app.get('/%s' % file_id, headers={'If-None-Match': etag})
        self.assertEquals(content.status_code, 304)

    def test_404(self):
        r = self.app.get('/12345678912346789012345', expect_errors=True)
        self.assertEquals(r.status_code, 404)

    def test_non_latin_filename(self):
        file_path = u'./tests/русское название.jpg'
        file_id = self.put_file(file_path)

        content = self.app.get('/%s' % file_id)
        self.assertEquals(content.status_code, 200)
        self.assertIn('russkoe nazvanie.jpg', content.headers['Content-Disposition'])

    def test_pending_404(self):
        file_path = './tests/jpg.jpg'
        file_id = self.put_file(file_path)
        self.db.fs.files.update({'_id': file_id}, {'$set': {'pending': True}})

        self.app.get('/%s' % file_id, status=404)

    def test_deleted_404(self):
        file_path = './tests/jpg.jpg'
        file_id = self.put_file(file_path)
        self.db.fs.files.update({'_id': file_id}, {'$set': {'deleted': True}})

        self.app.get('/%s' % file_id, status=404)
