from bson.objectid import ObjectId
from bson.errors import InvalidId

from gridfs import GridFS
from werkzeug.wsgi import wrap_file
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import abort, NotFound, HTTPException

import settings
import utils


class GridFSServer(object):
    def __init__(self):
        self.url_map = Map([
            Rule('/<obj_id>', endpoint='get_file')
        ])

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
        return Response(utils.LimitedFileWrapper(fs_file, start, end),
                        mimetype=fs_file.content_type, headers=headers, status=206)

    def on_get_file(self, request, obj_id):
        with utils.MongoDBConnection() as connection:
            fs = GridFS(connection[settings.MONGO_DB_NAME])
            try:
                fs_file = fs.get(ObjectId(obj_id))
            except:
                abort(404)

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
                elif end > fs_file.length:
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
