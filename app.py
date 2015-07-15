from email.utils import encode_rfc2231

from gridfs import GridFS
from werkzeug.wsgi import wrap_file, ClosingIterator
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import abort, HTTPException
from unidecode import unidecode

import settings
from utils import ObjectIdConverter, LimitedFileWrapper, get_mongodb_connection


def serve_full_file_request(request, headers, file, callbacks=[]):
    headers.update({
        'Content-Length': file.length,
        'Accept-Ranges': 'bytes',
    })
    file_iterator = wrap_file(request.environ, file)
    response = Response(ClosingIterator(file_iterator, callbacks=callbacks),
                        direct_passthrough=True, mimetype=file.content_type,
                        headers=headers)
    response.last_modified = file.uploadDate
    response.set_etag(file.md5)
    return response


def serve_partial_file_request(request, headers, file, start, end, callbacks=[]):
    # Note: byte positions are inclusive!
    headers.update({
        'Content-Length': end - start,
        'Content-Range': 'bytes %i-%i/%i' % (start, end - 1, file.length),
    })
    file_iterator = LimitedFileWrapper(file, start, end)
    return Response(ClosingIterator(file_iterator, callbacks=callbacks),
                    direct_passthrough=True, mimetype=file.content_type,
                    headers=headers, status=206)


def serve_request(request, connection, _id=None):
    fs = GridFS(connection[settings.MONGO_DB_NAME])
    try:
        file = fs.get(_id)
    except:
        abort(404)

    if getattr(file, 'pending', False):
        abort(404)

    if getattr(file, 'blocked', False):
        abort(404)

    if request.if_modified_since:
        if request.if_modified_since > file.uploadDate:
            return Response(status=304)
    if request.if_none_match:
        if request.if_none_match.contains(file.md5):
            return Response(status=304)

    filename = file.filename
    # Process non-latin filenames using technique described here:
    # http://greenbytes.de/tech/tc2231/#encoding-2231-fb
    rfc2231_filename = encode_rfc2231(filename.encode('utf-8'), 'UTF-8')
    transliterated_filename = unidecode(filename)
    content_disposition = 'inline; filename="%s"; filename*=%s;' % (
        transliterated_filename, rfc2231_filename
        )

    headers = {'Content-Disposition': content_disposition}

    callbacks = [connection.close]
    if not request.range:
        return serve_full_file_request(request, headers, file, callbacks=callbacks)

    if request.range.units != 'bytes':
        abort(400)

    ranges = request.range.ranges
    if len(ranges) > 1:
        return serve_full_file_request(request, headers, file, callbacks=callbacks)

    start, end = ranges[0]
    if end is None:
        end = file.length
    elif end > file.length:
        abort(416)

    return serve_partial_file_request(request, headers, file, start, end,
                                      callbacks=callbacks)


url_map = Map([
    Rule('/<ObjectId:_id>'),
], converters={
    'ObjectId': ObjectIdConverter,
})


@Request.application
def app(request):
    urls = url_map.bind_to_environ(request.environ)
    try:
        endpoint, args = urls.match()
    except HTTPException, e:
        return e

    connection = get_mongodb_connection()
    try:
        return serve_request(request, connection, **args)
    except HTTPException as e:
        return e
    except:
        connection.close()


if __name__ == '__main__':
    from werkzeug.serving import run_simple
    run_simple(settings.APP_HOST, settings.APP_PORT, app,
               use_debugger=settings.DEBUG, use_reloader=True)
