#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import socket
import json


__all__ = ['Query', 'Socket']


class Query(object):
    def __init__(self, conn, resource):
        self._conn = conn
        self._resource = resource
        self._columns = []
        self._filters = []
        self._lqstring = ""

    def call(self):
        try:
            data = bytes(str(self), 'utf-8')
        except TypeError:
            data = str(self)
        return self._conn.call(data)

    __call__ = call

    def __str__(self):
        request = 'GET %s' % (self._resource)
        if self._columns and any(self._columns):
            request += '\nColumns: %s' % (' '.join(self._columns))
        request += self._lqstring
        request += '\nOutputFormat: json\nColumnHeaders: on\n'
        return request

    def columns(self, *args):
        self._columns = args
        return self

    def filter(self, filter_str):
        self._filters.append(filter_str)
        self._lqstring += f"\nFilter: {filter_str}"
        return self

    def negate(self):
        self._lqstring += f"\nNegate:"
        return self
    
    def oring(self,num):
        self._lqstring += f"\nOr: {num}"
        return self

    def anding(self,num):
        self._lqstring += f"\nAnd: {num}"
        return self

class Socket(object):
    def __init__(self, peer):
        self.peer = peer

    def __getattr__(self, name):
        return Query(self, name)

    def call(self, request):
        try:
            if len(self.peer) == 2:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(self.peer)
            s.send(request)
            s.shutdown(socket.SHUT_WR)
            rawdata = s.makefile().read()
            if not rawdata:
                return []
            data = json.loads(rawdata)
            return [dict(zip(data[0], value)) for value in data[1:]]
        finally:
            s.close()
