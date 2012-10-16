from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo import ReplicaSetConnection, Connection
from gridfs import GridFS
from werkzeug.wsgi import wrap_file, FileWrapper
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import abort, NotFound, HTTPException

import settings


class LimitedFileWrapper(FileWrapper):
    def __init__(self, file, start, end, buffer_size=8192):
        super(LimitedFileWrapper, self).__init__(file, buffer_size=buffer_size)
        self.file.seek(start)
        self._limit = end

    def next(self):
        buffer_size = min(self.buffer_size, self._limit - self.file.tell())
        data = self.file.read(buffer_size)

        if data:
            return data
        raise StopIteration()


class GridFSServer(object):
    def __init__(self):
        self.url_map = Map([
            Rule('/<obj_id>', endpoint='get_file')
        ])

    def get_mongodb_connection(self):
        if settings.MONGO_REPLICATION_ON:
            return ReplicaSetConnection(settings.MONGO_REPLICA_SET_URI,
                                        replicaset=settings.MONGO_REPLICA_SET_NAME)
        else:
            return Connection(settings.MONGO_HOST, settings.MONGO_PORT)

    def serve_whole_file(self, request, headers, fs_file):
        headers.update({
            'Content-Length': fs_file.length
        })
        return Response(wrap_file(request.environ, fs_file),
                        mimetype=fs_file.content_type, headers=headers)

    def serve_partial(self, request, headers, fs_file, start, end):
        headers.update({
            'Content-Length': end - start
        })
        return Response(LimitedFileWrapper(fs_file, start, end),
                        mimetype=fs_file.content_type, headers=headers, status=206)

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
            if not request.range.units == 'bytes':
                abort(400)

            ranges = request.range.ranges
            if len(ranges) > 1:
                return self.serve_whole_file(headers, fs_file)

            start, end = ranges[0]
            if end is None:
                end = fs_file.length
            if end > fs_file.length:
                abort(416)

            return self.serve_partial(request, headers, fs_file, start, end)
        else:
            return self.serve_whole_file(request, headers, fs_file)

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
