from gridfs import GridFS
from werkzeug.wsgi import wrap_file
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import abort, NotFound, HTTPException

import settings
from utils import ObjectIdConverter, LimitedFileWrapper, MongoDBConnection


def serve_full_file_request(request, headers, file):
    headers.update({
        'Content-Length': file.length
    })
    return Response(wrap_file(request.environ, file),
                    mimetype=file.content_type, headers=headers)


def serve_partial_file_request(request, headers, file, start, end):
    headers.update({
        'Content-Length': end - start
    })
    return Response(LimitedFileWrapper(file, start, end),
                    mimetype=file.content_type, headers=headers, status=206)


def serve_request(request, _id=None):
    with MongoDBConnection() as connection:
        fs = GridFS(connection[settings.MONGO_DB_NAME])
        try:
            file = fs.get(_id)
        except:
            abort(404)

        headers = {
            'Content-Disposition': 'inline; filename="%s";' % file.filename
        }

        if not request.range:
            return serve_full_file_request(request, headers, file)
        else:
            if request.range.units != 'bytes':
                abort(400)

            ranges = request.range.ranges
            if len(ranges) > 1:
                return serve_full_file_request(request, headers, file)

            start, end = ranges[0]
            if end is None:
                end = file.length
            elif end > file.length:
                abort(416)

            return serve_partial_file_request(request, headers, file, start, end)


url_map = Map([
    Rule('/<ObjectId:_id>')
], converters={
    'ObjectId': ObjectIdConverter
})


@Request.application
def app(request):
    urls = url_map.bind_to_environ(request.environ)
    try:
        endpoint, args = urls.match()
        return serve_request(request, **args)
    except HTTPException, e:
        return e


if __name__ == '__main__':
    from werkzeug.serving import run_simple
    run_simple(settings.APP_HOST, settings.APP_PORT, app,
               use_debugger=settings.DEBUG, use_reloader=True)
