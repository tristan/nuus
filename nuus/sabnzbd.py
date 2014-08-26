from tornado.httpclient import HTTPClient, HTTPRequest
from tornado.httputil import url_concat
import requests

class SabnzbdClient(HTTPClient):
    def __init__(self, host, apikey):
        super(SabnzbdClient, self).__init__()
        if host.endswith('/'):
            self.host = host[:-1]
        else:
            self.host = host
        self.apikey = apikey

    def addurl(self, url):
        return self.fetch(url_concat('%s/api' % self.host, dict(mode='addurl', name=url, apikey=self.apikey)))

    def addfile(self, filename, contents):
        boundary = '-----------------zxczczc'
        body = """--%(boundary)s\r\nContent-Disposition: form-data; name="output"\r\n\r\njson\r\n--%(boundary)s\r\nContent-Disposition: form-data; name="mode"\r\n\r\naddfile\r\n--%(boundary)s\r\nContent-Disposition: form-data; name="nzbname"\r\n\r\n%(filename)s\r\n--%(boundary)s\r\nContent-Disposition: form-data; name="nzbfile"; filename="%(filename)s.nzb"\r\nContent-Type: application/x-nzb\r\n\r\n%(contents)s\r\n--%(boundary)s\r\nContent-Disposition: form-data; name="apikey"\r\n\r\n%(apikey)s\r\n--%(boundary)s--\r\n""" % dict(
            boundary=boundary,
            filename=filename,
            contents=contents,
            apikey=self.apikey)
        req = HTTPRequest(
            '%s/api' % self.host,
            method='POST',
            request_timeout=60,
            headers={
                'Content-Type': 'multipart/form-data; charset=UTF-8; boundary=%s' % boundary,
                'Content-Length': str(len(body))
            },
            body=body)
        print('sending file %s.nzb of length %s' % (filename, len(body)))
        return self.fetch(req)
