import re
from random import choice

from pymongo import ReplicaSetConnection, Connection, ReadPreference
from bson.objectid import ObjectId
from bson.errors import InvalidId
from werkzeug.wsgi import wrap_file
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import abort, NotFound, HTTPException
from gridfs import GridFS

import settings


class GridFSServer(object):
    def __init__(self):
        self.url_map = Map([
            Rule('/<obj_id>', endpoint='get_file')
        ])

    def get_mongodb_connection(self):
        if settings.MONGO_REPLICATION_ON:
            read_preference_random = choice([ReadPreference.PRIMARY, ReadPreference.SECONDARY])
            return ReplicaSetConnection(settings.MONGO_REPLICA_SET_URI,
                        replicaset=settings.MONGO_REPLICA_SET_NAME)
        else:
            return Connection(settings.MONGO_HOST, settings.MONGO_PORT)

    def on_get_file(self, request, obj_id):
        connection = self.get_mongodb_connection()
        fs = GridFS(connection[settings.MONGO_DB_NAME])
        try:
            obj_id = ObjectId(obj_id)
        except InvalidId:
            abort(404)

        if not fs.exists(obj_id):
            abort(404)
        
        fs_file = fs.get(obj_id)
        headers = {
            'Content-Disposition': 'inline; filename="%s";' % fs_file.filename
        }
        if request.range:
            range_header = re.findall(r'^bytes=(?P<start>\d+)-(?P<finish>\d+)$', str(request.range))
            if not range_header:
                abort(400)
            start, finish = map(int, range_header[0])
            if finish > fs_file.length:
                abort(416)
            fs_file.seek(start)
            return Response(fs_file.read(finish - start),
                    mimetype=fs_file.content_type, headers=headers, status=206)
        else:
            headers.update({
                'Content-Length': fs_file.length
            })
            return Response(wrap_file(request.environ, fs_file),
                    mimetype=fs_file.content_type, headers=headers)

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except NotFound, e:
            return e
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)


gridfs_serve = GridFSServer()

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    run_simple(settings.APP_HOST, settings.APP_PORT, gridfs_serve,
            use_debugger=settings.DEBUG, use_reloader=True)
