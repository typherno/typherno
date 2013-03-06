#
# Copyright (c) 2011, 2013 Nate Diller.
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

import asyncore
import hashlib	# v2.5

from BaseHTTPServer import BaseHTTPRequestHandler
from cStringIO import StringIO

from email import utils		# v2.4
from datetime import datetime	# v2.3
from traceback import print_stack, format_exc	# v2.4

from typherno_common import *


def tsv_line(fields):
	return "%s\n" % '\t'.join(fields)


def uptime_str(start):
	uptime = datetime.now() - start
	days, seconds = uptime.days, uptime.seconds

	if days > 1:
		return "%d days  " % days

	hours = seconds / 3600 + days * 24
	if hours > 1:
		return "%d hours " % hours

	minutes = seconds / 60
	if minutes > 1:
		return "%d minutes" % minutes

	return "%d seconds" % seconds



class connection_state:
	def __init__(self, host, port):
		self.host = host
		self.port = port

		self.role = ''
		self.info = None
		self.uuid = ''
		self.status = None

		self.starttime = datetime.now()

	def unpack_body(self, map, body):
		if len(body) % 2:
			body.append('')
		while body:
			map[body.pop(0)] = body.pop(0)

	def recv_info(self, seq):
		if len(seq) < 3:
			return

		uuid, role, info, body = seq[0], seq[1], seq[2], seq[3:]
		if info != "Info":
			return

		self.role, self.uuid = role, uuid

		self.info = {}
		self.unpack_body(self.info, body)

	def recv_status(self, seq):
		status, body = seq[0], seq[1:]
		if status != "Status":
			return

		if self.status is None:
			self.status = {}
		self.unpack_body(self.status, body)

	def recv_seq(self, seq):
		if not self.role:
			self.recv_info(seq)
		else:
			self.recv_status(seq)

	def raw_cap(self):
		return int(self.info["Raw Capacity"])

	def avail_cap(self):
		if not self.status:
			return 0
		return int(self.status["Available Capacity"])

	def relative_local_host(self, local_host, query_host):
		if self.host == query_host:
			return "127.0.0.1"
		if self.host == "127.0.0.1":
			return local_host
		return self.host

	def reporter_fields(self, local_host, query_host, da):
		yield "%s:%s" % (self.relative_local_host(local_host, query_host), self.port)
		yield uptime_str(self.starttime)
		yield da
		if self.role:
			yield self.role
		if self.uuid:
			yield self.uuid

	def subscriber_fields(self):
		yield self.uuid
		yield uptime_str(self.starttime)
		yield mb_str(self.info["Raw Capacity"])
		if "Provider" in self.info:
			yield self.info["Provider"]



class disk_state:
	def __init__(self, uuid):
		self.uuid = uuid

		self.starttime = datetime.now()
		self.subscriptions = []

	def add_subscriber(self, handle):
		self.subscriptions.append(handle)

	def update_subscriber(self, handle):
		self.subscriptions.remove(handle)
		self.subscriptions.append(handle)

	def drop_subscriber(self, handle):
		self.subscriptions.remove(handle)

	def latest_raw_cap(self):
		latest = self.subscriptions[-1]
		return latest.raw_cap()

	def uptime_fields(self, local_host, query_host):
		yield self.uuid
		yield uptime_str(self.starttime)
		yield str(len(self.subscriptions))
		latest = self.subscriptions[-1]
		yield mb_str(latest.raw_cap() - latest.avail_cap())
		yield mb_str(latest.raw_cap(), 4)
		host = latest.host
		if host == query_host:
			yield "127.0.0.1"
		elif host == "127.0.0.1":
			yield local_host
		else:
			yield host


def root_component(path):
	p = path[path.find('/')+1:]
	if '/' in p:
		return p[:p.find('/')]
	return p


def path_components(path):
	components = path.split('/')
	for i in range(1, len(components)):
		yield '/'.join(components[:i+1])


class archive_map:
	def __init__(self, source, name, piece_length):
		self.files = [source.allocate_fd() for i in range(2)]
		self.records, self.pieces = self.files

		self.mapname = name

		self.records.open("archive", ".tsv")
		self.pieces.open("archive", "-pieces.bin", piece_length)

		self.segment_counter = iter(xrange(1, 2**30))

		self.handlers = []
		self.disks = []
		self.disk_map = {}

		self.last_record_ptr = ''
		self.last_record_length = 0
		self.record_offset = 0

		self.last_root = []
		self.last_parent = ''
		self.last_parent_mentions = ''

		self.last_dir_record = {}
		self.last_dir_mention = {}

		self.starttime = datetime.now()
		self.virgin = True

	def last_record_len(self):
		return self.last_record_length

	def root_link(self, modepath):
		if not self.last_root:
			return '-'
		p = root_component(modepath)
		if self.last_root[0][0] != p:
			link = self.last_root[0][1]
			if link == self.last_record_ptr:
				return '^'
			return link
		if len(self.last_root) > 1:
			return self.last_root[1][1]
		return '-'

	def parent_link(self, parent):
		link = self.last_dir_record.get(parent, '-')
		if link == self.last_record_ptr:
			return '^'
		return link

	def mention_fields(self, dir):
		limit = self.last_dir_mention.get(dir, '-')
		yield ''
		prev = ''
		for pdir in path_components(dir):
			ptr = self.last_dir_mention.get(pdir, '-')
			if ptr == prev:
				yield ''
			else:
				yield ptr
				if ptr == limit:
					break
				prev = ptr

	def mentions(self, parent, parent_link):
		if parent == self.last_parent:
			return self.last_parent_mentions

		self.last_parent = parent
		mentions = '/'.join(self.mention_fields(parent))
		self.last_parent_mentions = mentions

		if mentions.endswith('/' + parent_link):
			return mentions.rstrip(parent_link)
		return mentions

	def record_link(self, modepath):
		if not modepath.startswith('/'):
			return '-'
		root_link = self.root_link(modepath)
		parent = modepath[:modepath.rfind('/')]
		parent_link = self.parent_link(parent)
		mentions = self.mentions(parent, parent_link)
		return ' '.join([root_link, parent_link, mentions])

	def record_fields(self, seg, length, digest, modepath):
		yield "%x" % self.last_record_len()
		yield seg.segname
		yield "%x" % seg.bin_offset
		yield str(length)
		yield digest
		yield self.record_link(modepath)
		yield modepath

	def next_record(self, seg, length, digest, modepath=None):
		if modepath is None:
			modepath = seg.modepath
		rec = "%s\n" % '\t'.join(self.record_fields(seg, length, digest, modepath))
		self.new_record(rec, modepath)
		return rec

	def new_record(self, record, modepath):
		record_ptr = "%x:%x" % (self.record_offset, len(record))

		self.record_offset += len(record)
		self.last_record_length = len(record)
		self.last_record_ptr = record_ptr
		if not modepath.startswith('/'):
			modepath = ''
			self.last_parent = ''
			return

		root = root_component(modepath)
		if self.last_root and self.last_root[0][0] == root:
			self.last_root[0][1] = record_ptr
		else:
			self.last_root.insert(0, [root, record_ptr])
			if len(self.last_root) > 2:
				self.last_root.pop()

		parent = modepath[:modepath.rfind('/')]
		if not parent:
			self.last_parent = ''
			return

		self.last_dir_record[parent] = record_ptr
		for pdir in path_components(parent):
			self.last_dir_mention[pdir] = record_ptr

	def put_disk(self, uuid):
		disk = self.disk_map[uuid]
		if not disk.subscriptions:
			self.disks.remove(disk)
			del self.disk_map[uuid]

	def get_disk(self, uuid):
		if uuid in self.disk_map:
			return self.disk_map[uuid]

		disk = disk_state(uuid)
		self.disks.append(disk)

		self.disk_map[uuid] = disk
		return disk

	def disk_map_put(self, uuid, subscriber):
		disk = self.disk_map[uuid]
		disk.drop_subscriber(subscriber)
		self.put_disk(uuid)

	def disk_lines(self, local_host, query_host):
		for disk in self.disks:
			yield '\t'.join(disk.uptime_fields(local_host, query_host))

	def upload_lines(self, local_host, query_host):
		for da in self.handlers:
			if da.upload_ready():
				yield '\t'.join(da.upload_fields(local_host, query_host))

	def segment_lines(self):
		for da in self.handlers:
			for seg in da.segments:
				if seg.segname:
					yield '\t'.join(seg.uptime_fields())

	def reporter_lines(self, local_host, query_host):
		for da in self.handlers:
			for r in da.data_peers:
				if r.role != "Subscriber":
					yield '\t'.join(r.reporter_fields(local_host, query_host, da.peer_uuid))

	def accumulator_lines(self, local_host, query_host):
		for da in self.handlers:
			yield '\t'.join(da.accumulator_fields(local_host, query_host))

	def uptime_fields(self, fs_uuid, repl, min_repl):
		yield fs_uuid
		yield uptime_str(self.starttime)
		yield "%s/%sx" % (repl, min_repl)
		yield "%14d" % sum(ar.bytes_done for ar in self.handlers)
		yield str(sum(ar.segments_done for ar in self.handlers))

		upldg = sum(1 for ar in self.handlers for s in ar.segments if s.modepath)
		cmsnd = sum(1 for ar in self.handlers for s in ar.segments if s.segname)
		yield "%d/%d" % (upldg, cmsnd)
		acqrd = sum(1 for ar in self.handlers for s in ar.segments if s.connection)
		maxsg = sum(int(ar.peer_info["Maximum Concurrent Segments"]) for ar in self.handlers)
		yield "%d/%dw" % (acqrd, maxsg)

		written = sum(ar.bytes_done * len(ar.data_peers) for ar in self.handlers)
		raw_cap = sum(disk.latest_raw_cap() for disk in self.disks)
		yield '/'.join((mb_str(written), mb_str(raw_cap, 4)))



class segment_descriptor:
	def __init__(self, channel, source):
		self.channel = channel

		self.files = [source.allocate_fd() for i in range(4)]
		self.bin, self.pieces, self.reporters, self.entries = self.files

		self.mapname = ''
		self.commissioned = False
		self.modepath = ''
		self.attrs = ''
		self.meter_len = 0
		self.meter_md5 = hashlib.md5()
		self.reporter = None

	def fs_request_segment(self, reporter, segment_request_queue):
		if self.reporter:
			raise RuntimeError("Cannot request segment for %s, busy with %s" % (repr(reporter), repr(self.reporter)))

		yield ctl_msg("Request Segment")
		self.reporter = reporter
		segment_request_queue.append(self)

	def fs_commission_segment(self, cookie):
		assert not self.commissioned
		yield cmd_msg(self.channel, *self.commission_fields(cookie))
		self.commissioned = True

	def fs_entry(self, modepath):
		if self.modepath:
			raise RuntimeError("Cannot write entry %s, busy with %s" % (modepath, self.modepath))

		yield raw_msg(modepath, ENTRY, self.channel)
		self.modepath = modepath

	def fs_entry_abort(self, put=False):
		if self.modepath:
			yield cmd_msg(self.channel, *self.record_fields(commit=False))
			self.modepath = ''
			self.meter_len = 0
			self.meter_md5 = hashlib.md5()
		if put:
			self.put()

	def fs_entry_commit(self, cookie=None):
		assert self.modepath
		yield cmd_msg(self.channel, *self.record_fields(cookie))
		self.modepath = ''
		self.meter_len = 0
		self.meter_md5 = hashlib.md5()

	def fs_renew_segment(self, reporter, queue):
		assert self.mapname
		if self.commissioned:
			yield cmd_msg(self.channel, *self.retire_fields())
			self.commissioned = False
		self.mapname = ''

		for d in self.fs_request_segment(reporter, queue):
			yield d

	def fd_reporter(self, cookie):
		assert self.reporter
		yield (self.reporters, tsv_line(self.reporter_fields()), cookie)
		self.reporter = None

	def fd_attribute(self, attrs):
		self.attrs = ''.join(attrs)
		yield (self.entries, '\t'.join(self.entry_fields()))

	def fd_write(self, datachunks):
		assert not isinstance(datachunks, str)
		for data in datachunks:
			assert isinstance(data, str)
			if not self.meter_len and data:
				if not self.attrs:
					self.attrs = '-'
					yield (self.entries, '\t'.join(self.entry_fields()))
			yield (self.bin, data)
			yield (self.pieces, ''.join(self.pieces.digest(data)))
			self.meter_len += len(data)
			self.meter_md5.update(data)

	def fd_abort(self):
		if self.attrs:
			yield (self.entries, tsv_line(self.abort_fields()))
			self.attrs = ''

	def fd_commit(self):
		if self.attrs:
			yield (self.entries, tsv_line(self.commit_fields()))
			self.attrs = ''

	def fd_commit_sync(self, cookie):
		if not self.attrs:
			self.attrs = '-'
			yield (self.entries, '\t'.join(self.entry_fields()))
		yield (self.bin, '', cookie)
		yield (self.pieces, '', cookie)
		yield (self.entries, tsv_line(self.commit_fields()), cookie)
		self.attrs = ''

	def fd_sync_put(self, cookie):
		yield (self.bin, '', cookie)
		yield (self.pieces, '', cookie)
		yield (self.entries, '', cookie)
		self.put()

	def fd_retire(self, cookie):
		yield (self.bin, '', cookie)
		yield (self.pieces, self.pieces.final_digest(), cookie)
		yield (self.entries, '', cookie)
		for fd in self.files:
			yield (fd,)

	def commit_fields(self):
		yield ''
		yield time_stamp()
		yield str(self.meter_len)
		yield self.meter_md5.hexdigest()

	def abort_fields(self):
		yield ''
		yield time_stamp()
		yield str(self.meter_len)
		yield '-'

	def entry_fields(self):
		yield "%x" % self.bin.position
		yield self.modepath
		yield self.attrs

	def reporter_fields(self):
		reporter = self.reporter
		yield time_stamp()
		yield str(self.bin.position)
		yield reporter.peer_role
		yield "%s:%d" % (reporter.remote_host, reporter.remote_port)
		if reporter.peer_uuid:
			yield reporter.peer_uuid

	def retire_fields(self):
		for fd in self.files:
			yield "%s   %12d  %s" % (fd.hash.hexdigest(), fd.position, fd.extension)
		yield "Retire Segment"

	def record_fields(self, cookie=None, commit=True):
		if cookie:
			yield str(cookie)
		if commit:
			yield self.meter_md5.hexdigest()
		yield str(self.meter_len)
		yield "Entry Record"

	def commission_fields(self, cookie):
		yield str(cookie)
		yield self.mapname
		yield "Commission Segment"



class file_descriptor:
	def __init__(self, channel):
		self.channel = channel
		self.name = ''
		self.extension = ''
		self.position = 0

	def __repr__(self):
		return "%d:%s%s@%d" % (self.channel, self.name, self.extension, self.position)

	def open(self, prefix, filename, piece_length=None):
		if self.name:
			raise RuntimeError("Open %s%s failed: already open %s%s" % (prefix, filename, self.name, self.extension))

		self.name = prefix
		self.extension = filename
		self.piece_length = piece_length

		self.reset_piece()

		self.position = 0
		self.hash = hashlib.sha256()

	def reset_piece(self):
		self.current_piece = hashlib.sha1()
		self.boundary = self.piece_length

	def digest(self, data):
		while self.boundary <= len(data):
			piece, data = data[:self.boundary], data[self.boundary:]
			self.current_piece.update(piece)

			yield self.current_piece.digest()
			self.reset_piece()

		self.boundary -= len(data)
		self.current_piece.update(data)

	def final_digest(self):
		if self.boundary == self.piece_length:
			return ''
		return self.current_piece.digest()

	def provision(self):
		assert self.name
		path = "%s%s" % (self.name, self.extension)
		return raw_msg(path, PROVISION, self.channel, self.position)

	def data(self, data, cookie=None):
		assert self.name
		self.hash.update(data)
		self.position += len(data)
		if cookie:
			return raw_msg(data, DATA_ACK, self.channel, int(cookie))
		if data:
			return raw_msg(data, DATA, self.channel)

	def reclaim(self):
		assert self.name
		return raw_msg('', RECLAIM, self.channel)

	def close(self):
		assert self.name
		self.name = ''



class subscription_queue(list):
	def __init__(self, minimum_replication_factor):
		list.__init__(self)

		self.min_repl = minimum_replication_factor

		self.fd_pool = [file_descriptor(ch) for ch in range(2, 255)]
		self.fd_active = []
		self.pub_queue = []
		self.ack_cookies = iter(xrange(2, 2**30))
		self.ack_junction = {}

		self.fs_uuid = ''
		self.fs_name = ''
		self.fs_path_max = 0
		self.info_msg = ''
		self.starttime = datetime.now()

	def replication_factor(self):
		return len(self)

	def allocate_fd(self):
		return self.fd_pool.pop(0)

	def fill_info(self, fs_uuid, fs_name, fs_path_max):
		self.fs_uuid = fs_uuid
		self.fs_name = fs_name
		self.fs_path_max = fs_path_max

		info = (fs_uuid, "Archive Tracker", "Info", \
			fs_uuid, "File System", \
			fs_name, "File System Name", \
			str(fs_path_max), "Path Max", \
			str(self.min_repl), "Minimum Replication Factor", )
		self.info_msg = ctl_msg(*info)

	def stat_seq_fields(self):
		yield "Status"
		yield str(self.replication_factor())
		yield "Replication Factor"

	def status(self):
		return ctl_msg(*self.stat_seq_fields())

	def resolve_ack(self, cookie, handler):
		if cookie not in self.ack_junction:
			raise RuntimeError("ack unknown ... receiver canceled?")
		receiver, incomplete = self.ack_junction[cookie]
		if handler not in incomplete:
			raise RuntimeError("ack from unxepected handler %s, needed %s" % (repr(handler), repr(incomplete)))

		incomplete.remove(handler)
		if incomplete:
			return
		self.ack_junction.pop(cookie)
		receiver.done_ack()

	def add_ack_receiver(self, cookie, publisher):
		if cookie in self.ack_junction:
			receiver, targets = self.ack_junction[cookie]
			assert publisher is receiver
			targets.extend(self)
		else:
			self.ack_junction[cookie] = [publisher, list(self)]

	def file_msg(self, publisher, name, data, cookie=None):
		yield raw_msg(name, PROVISION, 1, 0)
		if cookie:
			yield raw_msg(data, DATA_ACK, 1, cookie)
			self.add_ack_receiver(cookie, publisher)
		else:
			yield raw_msg(data, DATA, 1)
		yield raw_msg('', RECLAIM, 1)

	def fd_msg(self, publisher, fd, data=None, cookie=None):
		if data is None:
			yield fd.reclaim()
			fd.close()
			self.fd_active.remove(fd)
			self.fd_pool.append(fd)
		elif data or cookie:
			assert isinstance(data, str)
			if fd not in self.fd_active:
				yield fd.provision()
				self.fd_active.append(fd)
			yield fd.data(data, cookie)
			if cookie:
				self.add_ack_receiver(cookie, publisher)

	def handle_msg(self, publisher, target, data=None, cookie=None):
		if isinstance(target, str):
			for m in self.file_msg(publisher, target, data, cookie):
				yield m
		else:
			for m in self.fd_msg(publisher, target, data, cookie):
				yield m

	def try_to_publish(self):
		if not self.pub_queue or self.replication_factor() < self.min_repl:
			return
		for subscriber in self:
			if subscriber.writable():
				return
		publisher = self.pub_queue.pop(0)
		issue = publisher.publish_content()

		release = ''.join(d for msg in issue for d in self.handle_msg(publisher, *msg))
		for subscriber in self:
			subscriber.out_buffer = ''.join([subscriber.out_buffer, release])

	def queue_issue(self, publisher):
		assert publisher not in self.pub_queue
		self.pub_queue.append(publisher)
		self.try_to_publish()

	def subscribe(self, subscriber):
		for s in self:
			if subscriber.peer_uuid == s.peer_uuid:
				raise RuntimeError("Disk already subscribed")
		self.append(subscriber)
		for fd in self.fd_active:
			yield fd.provision()
		yield ctl_msg(*self.stat_seq_fields())

	def unsubscribe(self, subscriber):
		self.remove(subscriber)
		self.try_to_publish()

	def subscriber_lines(self):
		for s in self:
			yield '\t'.join(s.subscriber_fields())

	def provider_lines(self):
		for s in self:
			if s.peer_status["Provider"]:
				yield '\t'.join(s.subscriber_fields())

	def archive_fields(self):
		yield "v0"
		yield self.fs_name
		yield self.fs_uuid
		yield "super"



class accumulator_queue(subscription_queue):
	def __init__(self, minimum_replication_factor, seg_width, seg_length, write_max):
		subscription_queue.__init__(self, minimum_replication_factor)

		self.write_max = write_max
		self.segment_length = seg_length
		self.segment_width = seg_width

		self.fs_queue = None

	def set_fs_queue(handler):
		self.fs_queue = handler

	def fill_ar_info(self, ar_uuid, *fs_args):
		self.fill_info(*fs_args)

		info = (ar_uuid, "Accumulator", "Info", \
			self.fs_uuid, "File System", \
			self.fs_name, "File System Name", \
			str(self.fs_path_max), "Path Max", \
			str(self.write_max), "Write Max", \
			str(self.segment_length), "Segment Length", \
			str(self.segment_width), "Maximum Concurrent Segments", \
			str(self.min_repl), "Minimum Replication Factor", )
		self.info_msg = ctl_msg(*info)

	def stat_seq_fields(self):
		yield "Status"
		yield str(self.fs_queue.available_segments())
		yield "Segments Available"
		yield str(self.replication_factor())
		yield "Replication Factor"



class msg_handler(asyncore.dispatcher_with_send):
	def __init__(self, host, port, *asyncore_args):
		asyncore.dispatcher_with_send.__init__(self, *asyncore_args)

		self.remote_host = host
		self.remote_port = port

		self.peer_role = ''
		self.peer_uuid = ''
		self.peer_info = {}
		self.head_bytes = ''
		self.bytes_expected = None
		self.chunks = []

	def protocol_error(self, err):
		print format_exc()
		raise

	def connect(self):
		asyncore.dispatcher_with_send.connect(self, (self.remote_host, self.remote_port))

	def handle_connect(self):
		self.out(HANDSHAKE)

	def got_info(self):
		pass

	def recv_info(self):
		seq = [s for s in self.unpack_seq()]
		if len(seq) < 3:
			self.protocol_error("Info: invalid text %r" % seq)
		head, body = seq[:3], seq[3:]
		if head[2] != "Info" or not head[1]:
			self.protocol_error("Info: incomplete header %r" % head)
		if len(body) % 2:
			self.protocol_error("Info: unidentified value %s" % pairs[-1])
		self.peer_role, self.peer_uuid = head[1], head[0]
		while body:
			self.peer_info[body.pop(0)] = body.pop(0)

		self.got_info()
		if CTL in self.handlers:
			try:
				self.handlers[CTL](self)
			except Exception, err:
				self.protocol_error(err)

	def msg_router(self):
		call_id, = struct.unpack("!B", self.chunks[0][0])	# peek
		if call_id not in msg_format:
			self.protocol_error("unknown msg ID - %d" % call_id)
		fmt = msg_format[call_id]
		head = self.chomp(struct.calcsize(fmt))
		if not self.peer_role:
			if call_id != CTL:
				self.protocol_error("call_id %r instead of CTL Info" % call_id)
			self.recv_info()
			return
		args = struct.unpack(fmt, head)[1:]
		if call_id not in self.handlers:
			self.protocol_error("unhandled msg ID %d - %r" % (call_id, args))
		try:
			self.handlers[call_id](self, *args)
		except Exception, err:
			self.protocol_error(err)

	def got_handshake(self):
		pass

	def recv_handshake(self):
		self.head_bytes += self.recv(len(HANDSHAKE) - len(self.head_bytes))
		if len(self.head_bytes) < len(HANDSHAKE):
			return
		try:
			rev, flags = verify_handshake(self.head_bytes)
		except RuntimeError, err:
			self.protocol_error(err)
		self.head_bytes = ''
		self.bytes_expected = 0

		self.got_handshake()

	def do_chomp(self, chunks, bytes):
		if not bytes:
			return ''
		soup = []
		while bytes and len(chunks[0]) <= bytes:
			bytes -= len(chunks[0])
			soup.append(chunks.pop(0))
		if bytes:
			chunk = chunks[0]
			a, chunks[0] = chunk[:bytes], chunk[bytes:]
			soup.append(a)

		if len(soup) > 1:
			return ''.join(soup)
		return soup[0]

	def chomp(self, bytes):
		return self.do_chomp(self.chunks, bytes)

	def gobble(self):
		chunks = self.chunks
		if len(chunks) > 1:
			return ''.join(chunks)
		return chunks[0]

	def unpack_seq(self):
		chunks = list(self.chunks)
		while chunks:
			bytes, = struct.unpack("!B", self.do_chomp(chunks, 1))
			yield self.do_chomp(chunks, bytes)

	def try_handle_read(self):
		if self.bytes_expected is None:
			self.recv_handshake()
			return

		h = len(self.head_bytes)
		if h < 4:
			self.head_bytes += self.recv(4 - h)
			if len(self.head_bytes) == 4:
				self.bytes_expected, = struct.unpack("!L", self.head_bytes)
			return

		if self.bytes_expected > AR_WRITE_MAX * 2:
			self.protocol_error("packet too big %d bytes" % self.bytes_expected)

		if self.bytes_expected:
			chunk = self.recv(self.bytes_expected)
			self.bytes_expected -= len(chunk)
			self.chunks.append(chunk)

		if not self.bytes_expected:
			self.head_bytes = ''
			if self.chunks:
				self.msg_router()
				self.chunks = []

	def handle_read(self):
		try:
			self.try_handle_read()
		except Exception, err:
			self.protocol_error(err)

	def outl_v(self, chunks):
		msgs = [d for d in chunks]
		print "outl-%s" % repr(msgs)[:80]
		self.out_buffer = ''.join([self.out_buffer] + msgs)

	def outl(self, chunks):
		msgs = [d for d in chunks]
		self.out_buffer = ''.join([self.out_buffer] + msgs)

	def out(self, data):
		self.out_buffer += data

	def out_v(self, data):
		print "out-%s" % repr(data)[:80]
		self.out(data)

	def handle_close(self):
		self.close()



http_OK = "HTTP/1.1 200 OK\r\n"

http_header = """Server: Typherno/0.1 (Python)\r
Content-Type: text/%s\r
Content-Length: %d\r
Date: %s\r
\r
"""

http_404 = """<html>
<head><title>404 Not Found</title></head>
<body><h1>Not Found</h1><p>The requested URL %s was not found on this server.</p></body>
</html>
"""

class http_handler(asyncore.dispatcher_with_send, BaseHTTPRequestHandler):
	def __init__(self, host, port, *asyncore_args):
		asyncore.dispatcher_with_send.__init__(self, *asyncore_args)

		self.remote_host = host
		self.remote_port = port
		self.bytes = ''

	def reply_content(self, http, content):
		type = "plain"
		if content.startswith("<html>"):
			type = "html"
		header = http_header % (type, len(content), utils.formatdate(usegmt=True))
		self.out_buffer = ''.join([http, header, content])

	def reply_OK(self, content):
		self.reply_content(http_OK, content)

	def reply_lines(self, lines):
		self.reply_OK("\r\n".join([line for line in lines] + ['', '']))

	def handle_connect(self):
		pass

	def handle_read(self):
		self.bytes += self.recv(4096)
		if not self.bytes.endswith("\r\n\r\n"):
			return
		self.rfile = StringIO(self.bytes)
		self.rfile.seek(0)
		self.raw_requestline = self.rfile.readline()
		self.parse_request()

		self.http_request()

	def handle_write(self):
		asyncore.dispatcher_with_send.handle_write(self)
		if not self.out_buffer:
			self.handle_close()

	def handle_close(self):
		self.close()

