# -*- coding: utf-8 -*-
from werkzeug.wsgi import wrap_file
from pymongo import ReplicaSetConnection
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import abort, NotFound, HTTPException
from gridfs import GridFS
from pymongo import ReadPreference
from bson.objectid import ObjectId
from bson.errors import InvalidId
import re
import settings


class GridFSServer(object):

    def __init__(self, conf):

        con = ReplicaSetConnection('%s:%s'%(conf['mongo_host'], conf['mongo_port']),
            replicaset=conf['replica_name'], read_preference = ReadPreference.SECONDARY)
        self.fs = GridFS(con[conf['db_name']])
        self.url_map = Map([
            Rule('/<objid>', endpoint='get_file')
        ])

    def on_get_file(self, request, objid):
        try:
            obj_id = ObjectId(objid)
        except InvalidId:
            abort(404)

        if not self.fs.exists(obj_id):
            abort(404)
        else:
            fs_file = self.fs.get(obj_id)
            m_type = fs_file.content_type
            try:

                if request.range:
                    range_header =  re.findall(r'^bytes=(?P<start>\d+)-(?P<finish>\d+)$', str(request.range))
                    if range_header:
                        start, finish = int(range_header[0][0]), int(range_header[0][1])
                        if finish>fs_file.length:
                            abort(416)
                        fs_file.seek(start)
                        fs_file = fs_file.read(finish-start)
                        print fs_file
                        return Response(fs_file, mimetype=m_type, status=206)
            except ValueError:
                abort(500)
            return Response(wrap_file(request.environ, fs_file), mimetype=m_type)

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except NotFound, e:
            abort(404)
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

def create_app(mongo_port=settings.MONGO_PORT,mongo_host=settings.MONGO_HOST, replica_name=settings.REPLICA_NAME, db_name=settings.DB_NAME):
    app = GridFSServer({
        'mongo_port': mongo_port,
        'mongo_host': mongo_host,
        'replica_name':replica_name,
        'db_name':db_name
    })

    return app
gridfs_serve = create_app()
if __name__ == '__main__':
    from werkzeug.serving import run_simple
    app = create_app()

    run_simple(settings.APP_HOST, settings.APP_PORT, app, use_debugger=settings.DEBUG, use_reloader=True)