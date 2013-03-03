#
# Copyright (c) 2012, 2013 Nate Diller.
# All rights reserved.
# This component and the accompanying materials are made available
# under the terms of the "Eclipse Public License v1.0"
# which accompanies this distribution, and is available
# at the URL "http://www.eclipse.org/legal/epl-v10.html".
#
# Initial Contributors:
# Nate Diller - Initial contribution.
#
# Contributors:
# 
# Description:
#
#

import os
import sys
import BaseHTTPServer
import SocketServer
import urllib

from urlparse import urlparse
from xml.dom import minidom

from list_records import *


def multistatus(valuegen):
	yield '<?xml version="1.0" encoding="utf-8"?>\r\n'
	yield '<D:multistatus xmlns:D="DAV:">\r\n'
	for d in valuegen:
		yield d
	yield '</D:multistatus>\r\n'


def prop_encode_dir(props):
	for prop in props:
		if prop == "resourcetype":
			yield "<D:resourcetype><D:collection/></D:resourcetype>\r\n"
		elif prop == "getcontentlength":
			yield "<D:getcontentlength>0</D:getcontentlength>\r\n"
		else:
			yield "<D:%s/>\r\n" % prop


def prop_encode_file(rec, props):
	for prop in props:
		if prop == "getcontentlength":
			yield "<D:%s>%s</D:%s>\r\n" % (prop, str(rec.file_length), prop)
		elif prop == "getetag":
			yield "<D:%s>%s</D:%s>\r\n" % (prop, rec.file_hash, prop)
		else:
			yield "<D:%s/>\r\n" % prop


proptemplate = "<D:response>\r\n\
<D:href>%s</D:href>\r\n\
<D:propstat><D:status>HTTP/1.1 200 OK</D:status>\r\n\
<D:prop>\r\n\
%s</D:prop></D:propstat>\r\n\
</D:response>\r\n"


def encode_dir(path):
	yield '<tr><td><a href="/pub%s/">%s/</a></td></tr>\r\n' % (urllib.quote(path), path)


def encode_file(rec):
	yield "<tr>"
	p = rec.modepath
	yield '<td><a href="/pub%s">%s</a></td>' % (urllib.quote(p), p)
	yield "<td><pre>   %12d  </pre></td>" % rec.file_length
	yield "<td><pre>%s</pre></td>" % rec.file_hash
	yield "</tr>\r\n"


first_row = "<tr><th>Archive pathname</th>\
<td><pre>           size  </pre></td>\
<td><pre>md5</pre></td></tr>\r\n"


root_html = '<html><body><table>\r\n\
<tr><th>Repository pathname</th></tr>\r\n\
<tr><td><a href="/pub/">/pub/</a></td><td><pre>Browse the archive tree</pre></td></tr>\r\n\
<tr><td>/obj/</td><td><pre>Fetch a file object directly</pre></td></tr>\r\n\
</table></body></html>\r\n\r\n'


def list_props(xmldata):
	dom = minidom.parseString(xmldata)
	propfindnode, = dom.getElementsByTagNameNS("DAV:", "propfind")
	for pnode in propfindnode.childNodes:
		if pnode.nodeType == pnode.ELEMENT_NODE:
			for node in pnode.childNodes:
				if node.nodeType == node.ELEMENT_NODE:
					yield node.localName


class provider_handler(BaseHTTPServer.BaseHTTPRequestHandler):
	def recv_props(self):
		bytes = 2**18
		if "Content-length" in self.headers:
			bytes = int(self.headers["Content-length"])
		data = self.rfile.read(bytes)
		props = []
		if data:
			try:
				props.extend(list_props(data))
			except Exception, err:
				self.log_message("error parsing xml %s", err)
		if not props:
			props = ["resourcetype", "getcontentlength"]
		return props

	def list_slash(self):
		propbody = ''.join(prop_encode_dir(self.recv_props()))
		yield proptemplate % ('/', propbody)
		if self.headers["Depth"] == '1':
			yield proptemplate % ("/pub/", propbody)

	def list_pub(self, ls):
		props = self.recv_props()
		dirpropbody = ''.join(prop_encode_dir(props))
		yield proptemplate % ("/pub/", dirpropbody)
		if self.headers["Depth"] == '1':
			for rec in ls.start_record.list_root():
				dir = root_component(rec.modepath)
				if dir != rec.modepath:
					yield proptemplate % ("/pub%s/" % urllib.quote(dir), dirpropbody)
				else:
					propbody = ''.join(prop_encode_file(rec, props))
					yield proptemplate % ("/pub%s" % urllib.quote(rec.modepath), propbody)

	def list_dir(self, ls, pubpath):
		props = self.recv_props()
		dirpropbody = ''.join(prop_encode_dir(props))
		yield proptemplate % ("/pub%s/" % urllib.quote(pubpath), dirpropbody)
		if self.headers["Depth"] == '1':
			for dir in ls.subdirs():
				yield proptemplate % ("/pub%s/" % urllib.quote(dir), dirpropbody)
			mentioned = []
			for rec in ls.list_files():
				if rec.modepath not in mentioned:
					propbody = ''.join(prop_encode_file(rec, props))
					yield proptemplate % ("/pub%s" % urllib.quote(rec.modepath), propbody)
					mentioned.append(rec.modepath)

	def do_PROPFIND(self):
		path = urllib.unquote(urlparse(self.path)[2])
		if not path.startswith("/pub/") and not path == "/pub":
			if path == '/':
				self.send_response(207)
				self.send_header("Content-Type", "text/xml")
				self.end_headers()
				for d in multistatus(self.list_slash()):
					self.wfile.write(d)
			else:
				self.send_error(404)
			return

		recfd = open(os.path.join(sys.argv[2], "archive.tsv"), 'rb')
		start_record = record_entry(recfd)
		path = path[4:]
		if not path:
			path = '/'
		ls = lookup_state(start_record, path)
		if not ls.search_record:
			self.send_error(404)
			return
		if path and not path.endswith('/'):
			record = ls.lookup_record()
			if record:
				self.send_response(207)
				self.send_header("Content-Type", "text/xml")
				self.end_headers()
				propbody = ''.join(prop_encode_file(record, self.recv_props()))
				self.wfile.write(proptemplate % ("/pub" + urllib.quote(record.modepath), propbody))
				return
		ls.not_file()
		path = path.rstrip('/')
		if not ls.search_record:
			if path:
				self.send_error(404)
				return
		self.send_response(207)
		self.send_header("Content-Type", "text/xml")
		self.end_headers()
		if path:
			for d in multistatus(self.list_dir(ls, path)):
				self.wfile.write(d)
		else:
			for d in multistatus(self.list_pub(ls)):
				self.wfile.write(d)

	def do_GET(self):
		path = urllib.unquote(urlparse(self.path)[2])
		if not path.rstrip('/'):
			self.send_response(200)
			self.send_header("Content-Type", "text/html")
			self.end_headers()
			self.wfile.write(root_html)
			return
		view, path = path[:4], path[4:]
		if view not in ("/pub", "/obj"):
			self.send_response(404)
			return

		if not path:
			path = '/'
		recfd = open(os.path.join(sys.argv[2], "archive.tsv"), 'rb')
		start_record = record_entry(recfd)
		ls = lookup_state(start_record, path)
		if not ls.search_record:
			self.send_response(404)
			return
		if view == "/obj" or not path.endswith('/'):
			record = ls.lookup_record()
			if record:
				self.send_response(200)
				self.send_header("Content-Length", record.file_length)
				self.end_headers()
				for buf in file_data(record):
					self.wfile.write(buf)
				return
			elif view == "/obj":
				self.send_response(404)
				return
		ls.not_file()
		path = path.rstrip('/')
		if not ls.search_record:
			if path:
				self.send_response(404)
				return
		self.send_response(200)
		self.send_header("Content-Type", "text/html")
		self.end_headers()
		self.wfile.write("<html><body><table>\r\n")
		self.wfile.write(first_row)
		if not path:
			for rec in start_record.list_root():
				dir = root_component(rec.modepath)
				if dir != rec.modepath:
					self.wfile.write(''.join(encode_dir(dir)))
				else:
					self.wfile.write(''.join(encode_file(rec)))
			return
		for dir in ls.subdirs():
			self.wfile.write(''.join(encode_dir(dir)))
		for rec in ls.list_files():
			self.wfile.write(''.join(encode_file(rec)))
		self.wfile.write("</table></body></html>\r\n\r\n")

	def do_HEAD(self):
		path = urllib.unquote(urlparse(self.path)[2])
		if not path.rstrip('/'):
			self.send_response(200)
			self.send_header("Content-Type", "text/html")
			return
		view, path = path[:4], path[4:]
		if view not in ("/pub", "/obj"):
			self.send_response(404)
			return

		if not path:
			path = '/'
		recfd = open(os.path.join(sys.argv[2], "archive.tsv"), 'rb')
		start_record = record_entry(recfd)
		ls = lookup_state(start_record, path)
		if not ls.search_record:
			self.send_response(404)
			return
		if view == "/obj" or not path.endswith('/'):
			record = ls.lookup_record()
			if record:
				self.send_response(200)
				self.send_header("Content-Length", record.file_length)
				return
			elif view == "/obj":
				self.send_response(404)
				return
		ls.not_file()
		path = path.rstrip('/')
		if not ls.search_record:
			if path:
				self.send_response(404)
				return
		self.send_response(200)
		self.send_header("Content-Type", "text/html")

	def do_OPTIONS(self):
		self.send_response(200)
		self.send_header("Allow", "OPTIONS, HEAD, GET, PROPFIND")
		self.send_header("Content-Length", '0')
		self.send_header('DAV', '1')
		self.end_headers()

	def log_message(self, format, *args):
		log_fd.write("%s [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))
		log_fd.flush()


def file_data(rec):
	seg = open(os.path.join(sys.argv[2], "%s.bin" % rec.segment_name), 'rb')
	seg.seek(rec.segment_offset)
	bytes = rec.file_length
	while bytes:
		buf = seg.read(min(bytes, 4096))
		yield buf
		bytes -= len(buf)
	seg.close()


class provider_server(BaseHTTPServer.HTTPServer, SocketServer.ThreadingMixIn):
	def process_request(self, *args):
		try:
			BaseHTTPServer.HTTPServer.process_request(self, *args)
		except Exception, err:
			log_fd.write("Exception %s\n" % err)
			log_fd.flush()


if __name__ == "__main__":
	import time
	if len(sys.argv) > 3:
		print "provider.py port /path/to/disk-[uuid]/ar-[uuid]"
		sys.exit()

	global log_fd
	t = time.strftime("%Y%m%d%H%M%S", time.localtime())
	disk, port = os.path.dirname(os.path.abspath(sys.argv[2])), int(sys.argv[1])
	log_fd = open(os.path.join(disk, "logs", "%s-%d.log" % (t, port)), 'a')

	server = provider_server(('', port), provider_handler)
	server.serve_forever()


