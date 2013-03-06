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
import socket
import sys

import hashlib	# v2.5
import uuid	# v2.5

from datetime import datetime	# v2.3
from traceback import print_stack, format_exc	# v2.4

from typherno_common import *
from libaccumulator import *



class subscriber_handler(msg_handler):
	def __init__(self, *sockargs):
		msg_handler.__init__(self, *sockargs)

	def got_handshake(self):
		assert subscription.info_msg
		self.out(subscription.info_msg)

	def cmd_router(self, channel):
		seq = [s for s in self.unpack_seq()]
		cmd = seq.pop()
		if cmd not in self.cmd_handlers:
			self.protocol_error("unhandled ch%d %s" % (channel, repr(seq)))

		seq.reverse()
		try:
			self.cmd_handlers[cmd](self, channel, *seq)
		except Exception, err:
			self.protocol_error(err)

	def handle_write(self):		# hot
		msg_handler.handle_write(self)

		if self.peer_role == "Subscriber" and not self.out_buffer:
			subscription.try_to_publish()

	def handle_close(self):
		if self in subscription:
			subscription.unsubscribe(self)

		msg_handler.handle_close(self)

		if self.peer_role == "Subscriber":
			if self.out_buffer and not subscription.replication_factor():
				raise RuntimeError("Lost sync")



class segment_state:
	def __init__(self):
		self.connection = None
		self.segname = ''
		self.starttime = None
		self.bin_offset = 0
		self.modepath = ''

	def retire(self, body):
		yield ("%s-head.tsv" % self.segname, body)
		rec = tracker.next_record(self, len(body), hashlib.md5(body).hexdigest(), "h /")
		yield (tracker.records, rec)
		yield (tracker.pieces, ''.join(tracker.pieces.digest(rec)))
		self.segname = ''
		self.bin_offset = 0

	def commission(self, name):
		self.segname = name
		self.starttime = datetime.now()
		rec = tracker.next_record(self, 0, '-', "s /")
		cookie = subscription.ack_cookies.next()
		yield (tracker.records, rec, cookie)
		yield (tracker.pieces, ''.join(tracker.pieces.digest(rec)))

	def entry(self, length, digest):
		rec = tracker.next_record(self, length, digest)
		yield (tracker.records, rec)
		yield (tracker.pieces, ''.join(tracker.pieces.digest(rec)))
		self.modepath = ''
		self.bin_offset += length

	def entry_ack(self, length, digest):
		rec = tracker.next_record(self, length, digest)
		cookie = subscription.ack_cookies.next()
		yield (tracker.records, rec, cookie)
		yield (tracker.pieces, ''.join(tracker.pieces.digest(rec)))
		self.modepath = ''
		self.bin_offset += length

	def release(self):
		self.connection = None

	def acquire(self, connection):
		self.connection = connection

	def archive_fields(self):
		yield "v0"
		yield subscription.fs_name
		yield subscription.fs_uuid
		yield "head"
		yield "segment"
		yield self.segname

	def uptime_fields(self):
		yield self.segname
		yield uptime_str(self.starttime)
		yield str(self.bin_offset)
		if self.connection:
			da = self.connection
			yield da.uuid
			yield "%s:%s" % (da.host, da.port)
			yield self.modepath
		else:
			yield "<ready>"



class metadata_publisher(subscriber_handler):
	def __init__(self, *sockargs):
		subscriber_handler.__init__(self, *sockargs)

		self.peer_status = {}
		self.starttime = datetime.now()

		self.bytes_done = 0
		self.segments_done = 0

		self.issue = None
		self.reply = None

		self.channel_map = {}

		self.data_peers = []
		self.segments = []

		self.log_file = None

	def __repr__(self):
		return "%s:%d" % (self.remote_host, self.remote_port)

	def __str__(self):
		return repr(self)

	def _print(self, out):
		out = "%s %r~$ %s" % (time_stamp(), self, out)
		if self.log_file:
			self.log_file.write("%s\n" % out)
			self.log_file.flush()
		else:
			print out

	def raw_cap(self):
		return int(self.peer_info["Raw Capacity"])

	def avail_cap(self):
		if not self.peer_status:
			return 0
		return int(self.peer_status["Available Capacity"])

	def da_subscribers(self):
		for p in self.data_peers:
			if p.role == "Subscriber":
				yield p

	def da_repl(self):
		return len([s for s in self.da_subscribers()])

	def caplist_field(self):
		caplist = [s.avail_cap() for s in self.da_subscribers()]
		caplist.sort()
		min_repl = int(self.peer_info["Minimum Replication Factor"])
		if len(caplist) > min_repl:
			return "%s-%s" % (mb_str(caplist[0]), mb_str(caplist[-min_repl]))
		if len(caplist) == min_repl:
			return mb_str(caplist[0])
		return "0MB"

	def relative_local_host(self, local_host, query_host):
		port = self.peer_info["TCP Port"]
		if self.remote_host == query_host:
			return ':'.join(("127.0.0.1", port))
		if self.remote_host == "127.0.0.1":
			return ':'.join((local_host, port))
		return ':'.join((self.remote_host, port))

	def subscriber_lines(self):
		for s in self.da_subscribers():
			yield '\t'.join(s.subscriber_fields())

	def upload_ready(self):
		if self.da_repl() < int(self.peer_info["Minimum Replication Factor"]):
			return False

		acqrd = sum(1 for s in self.segments if s.connection)
		return acqrd < int(self.peer_info["Maximum Concurrent Segments"])

	def upload_fields(self, local_host, query_host):
		info = self.peer_info

		yield self.relative_local_host(local_host, query_host)
		acqrd = sum(1 for s in self.segments if s.connection)
		yield "%d/%sw" % (acqrd, info["Maximum Concurrent Segments"])
		yield "%s/%sx" % (self.da_repl(), info["Minimum Replication Factor"])
		yield self.caplist_field()
		yield "%dMB" % (int(info["Segment Length"]) / 2**20)
		yield self.peer_uuid

	def accumulator_fields(self, local_host, query_host):
		info = self.peer_info

		yield self.peer_uuid
		yield uptime_str(self.starttime)
		yield "%d/%sx" % (self.da_repl(), info["Minimum Replication Factor"])
		yield "%14d" % self.bytes_done
		yield str(self.segments_done)
		upldg = sum(1 for s in self.segments if s.modepath)
		cmsnd = sum(1 for s in self.segments if s.segname)
		yield "%d/%d" % (upldg, cmsnd)
		acqrd = sum(1 for s in self.segments if s.connection)
		yield "%d/%sw" % (acqrd, info["Maximum Concurrent Segments"])
		yield self.caplist_field()
		yield "%dMB" % (int(info["Segment Length"]) / 2**20)
		yield self.relative_local_host(local_host, query_host)

	def subscriber_fields(self):
		yield self.peer_uuid
		yield uptime_str(self.starttime)
		yield mb_str(self.peer_info["Raw Capacity"])
		if self.peer_status["Provider"]:
			yield self.peer_status["Provider"]

	def done_ack(self):
		if self.reply:
			self.out(self.reply)
			self.reply = None

	def publish_content(self):
		issue = self.issue
		self.issue = None
		for d in issue:
			yield d

	def readable(self):
		return not self.issue and not self.reply

	def super_fields(self):
		yield " - typherno archive -"
		yield '/'.join(subscription.archive_fields())
		yield "(%s)" % time_stamp()
		yield "piece-length: %d" % FS_PIECE_LENGTH
		yield "path-max: %d" % FS_PATH_MAX
		yield "software-version: 1.2-pre0"
		yield ''
		yield ''

	def super(self):
		yield ("archive-super.tsv", '\n'.join(self.super_fields()))

	def snapshot_fields(self, seg, digests):
		yield " - typherno archive -"
		yield '/'.join(seg.archive_fields())
		yield "(%s)" % time_stamp()
		yield "piece-length: %s" % self.peer_info["Piece Length"]
		yield ''
		yield " - segment digests -"
		for digest in digests:
			yield digest
		yield ''
		yield " - featuring disks -"
		for s in self.da_subscribers():
			yield s.uuid
		yield ''
		yield ''


def empty_head(seg, piece_length):
	yield " - typherno archive -"
	yield '/'.join(seg.archive_fields())
	yield "(%s)" % time_stamp()
	yield "piece-length: %s" % piece_length
	yield "\n - segment digests -\n\n - featuring disks -\n\n"


def fs_abort_all(prior, segments, piece_length):
	if prior:
		for m in prior:
			yield m
	for seg in segments:
		if seg.connection:
			body = '\n'.join(empty_head(seg, piece_length))
			for m in seg.retire(body):
				yield m


class metadata_handler(metadata_publisher):
	def got_info(self):
		self._print("%s %s: %s" % (self.peer_uuid, self.peer_role, repr(self.peer_info)))
		if self.peer_role == "Subscriber":
			disk = tracker.get_disk(self.peer_uuid)
			disk.add_subscriber(self)

			self.outl(subscription.subscribe(self))
		elif self.peer_role == "Accumulator Reporter":
			for ch in range(int(self.peer_info["Maximum Concurrent Segments"])):
				seg = segment_state()
				self.segments.append(seg)
				self.channel_map[ch + 1] = seg
			tracker.handlers.append(self)

	def disconnected(self, ch):
		cxn = self.channel_map[ch]
		if cxn.role == "Subscriber":
			tracker.disk_map_put(cxn.uuid, cxn)
		self._print("(%d) Disconnected" % ch)
		self.data_peers.remove(cxn)

	def connected_to(self, ch, host, port):
		assert ch not in self.channel_map
		self._print("(%d) Connected to %s:%s" % (ch, host, port))
		cxn = connection_state(host, port)
		self.channel_map[ch] = cxn
		self.data_peers.append(cxn)

	def log(self, ch, level, content):
		self._print("(%d) --%s-- %s" % (ch, level, content))

	def release_segment(self, ch):
		seg = self.channel_map[ch]
		self._print("(%d) %s released from %s:%s" % (ch, seg.segname, seg.connection.host, seg.connection.port))
		seg.release()

	def acquire_segment(self, ch, cxn_ch):
		seg = self.channel_map[ch]
		cxn = self.channel_map[int(cxn_ch)]
		fs = tracker
		if fs.virgin:
			self.issue = self.super()
			subscription.queue_issue(self)
			fs.virgin = False
		name = "<seg>"
		if seg.segname:
			name = seg.segname
		self._print("(%d) %s acquired by %s:%s (%s)" % (ch, name, cxn.host, cxn.port, cxn_ch))
		seg.acquire(cxn)

	def entry_record(self, ch, length, digest=None, cookie=None):
		seg = self.channel_map[ch]
		length = int(length)
		if cookie:
			self.issue = seg.entry_ack(length, digest)
			self.reply = ctl_msg(cookie, "Ack")
			subscription.queue_issue(self)
		elif digest:
			self.issue = seg.entry(length, digest)
			subscription.queue_issue(self)
		else:
			self._print("(%d) Cancel record %s at %s: %s skipped" % (ch, seg.segname, mb_str(seg.bin_offset), mb_str(length)))
			seg.modepath = ''
			seg.bin_offset += length
		self.bytes_done += length

	def retire_segment(self, ch, *digests):
		seg = self.channel_map[ch]
		self._print("(%d) Retiring segment %s: %s" % (ch, seg.segname, mb_str(seg.bin_offset)))
		self.segments_done += 1

		body = '\n'.join(self.snapshot_fields(seg, digests))
		self.issue = seg.retire(body)
		subscription.queue_issue(self)

	def commission_segment(self, ch, segname, cookie):
		seg = self.channel_map[ch]
		self._print("(%d) Commissioning segment %s" % (ch, segname))

		self.issue = seg.commission(segname)
		self.reply = ctl_msg(cookie, "Ack")
		subscription.queue_issue(self)

	cmd_handlers = { \
		"Acquire Segment":	acquire_segment, \
		"Release Segment":	release_segment, \

		"Commission Segment":	commission_segment, \
		"Entry Record":		entry_record, \
		"Retire Segment":	retire_segment, \

		"Connected to":		connected_to, \
		"Disconnected":		disconnected, \

		"Log":			log, }

	def status_seq(self, seq):
		self._print("(ctl) Status: %s" % repr(seq))
		while seq:
			self.peer_status[seq.pop(0)] = seq.pop(0)
		tracker.disk_map[self.peer_uuid].update_subscriber(self)

	def request_segment(self):
		segname = "%03d" % tracker.segment_counter.next()
		self._print("(ctl) New segment: %s" % segname)

		self.out(ctl_msg(segname, "Segment Name"))

	def ctl_router(self):
		seq = [s for s in self.unpack_seq()]
		if seq[0] == "Status":
			return self.status_seq(seq[1:])
		if seq[0] == "Request Segment":
			return self.request_segment()
		if len(seq) > 1 and seq[1] == "Ack":
			return subscription.resolve_ack(int(seq[0]), self)
		if len(seq) > 2 and seq[2] == "Info":
			return

		self._print("unhandled CTL %s" % repr(seq))

	def entry(self, ch):
		modepath = self.gobble()
		if len(modepath) > subscription.fs_path_max:
			raise RuntimeError("Entry: path too long")
		if modepath.startswith('.'):
			raise RuntimeError("Entry: illegal relative path name")

		self._print("(%d) path: %s" % (ch, modepath))
		self.channel_map[ch].modepath = modepath

	def log_seq(self, ch):
		cxn = self.channel_map[ch]
		seq = [s for s in self.unpack_seq()]
		self._print("(%d) --CTL-- %s" % (ch, str(seq)))
		if cxn.role:
			cxn.recv_status(seq)
			tracker.disk_map[cxn.uuid].update_subscriber(cxn)
		else:
			cxn.recv_info(seq)
			if cxn.role == "Subscriber":
				disk = tracker.get_disk(cxn.uuid)
				disk.add_subscriber(cxn)

	handlers = { \
		CTL:		ctl_router, \
		SET:		metadata_publisher.cmd_router, \

		ENTRY:		entry, \
		DATA:		log_seq, }

	def handle_write(self):
		try:
			metadata_publisher.handle_write(self)
		except Exception, err:
			self._print(err)

	def handle_close(self):
		fs = tracker
		self._print("Closing %s" % repr(self))

		if self.peer_role == "Accumulator Reporter":
			for s in self.da_subscribers():
				fs.disk_map_put(s.uuid, s)
			count = int(self.peer_info["Maximum Concurrent Segments"])
			segs = [self.channel_map[i+1] for i in range(count)]
			prior = self.issue
			self.issue = fs_abort_all(prior, segs, self.peer_info["Piece Length"])
			if not prior:
				subscription.queue_issue(self)
		elif self.peer_role == "Subscriber":
			fs.disk_map_put(self.peer_uuid, self)

		if self in fs.handlers:
			fs.handlers.remove(self)

		metadata_publisher.handle_close(self)



class tracker_status_handler(http_handler):
	def segments(self):
		self.reply_lines(tracker.segment_lines())

	def reporters(self):
		local_host = self.getsockname()[0]
		self.reply_lines(tracker.reporter_lines(local_host, self.remote_host))

	def disks(self):
		local_host = self.getsockname()[0]
		self.reply_lines(tracker.disk_lines(local_host, self.remote_host))

	def upload(self):
		local_host = self.getsockname()[0]
		self.reply_lines(tracker.upload_lines(local_host, self.remote_host))

	def accumulators(self):
		self.reply_lines(self.accumulator_lines())

	def providers(self):
		self.reply_lines(subscription.provider_lines())

	def status(self):
		self.reply_lines(self.status_lines())

	URLs = { \
		"/segments":		segments, \
		"/reporters":		reporters, \
		"/disks":		disks, \
		"/upload":		upload, \
		"/accumulators":	accumulators, \
		"/providers":		providers, \
		"/status":		status, }

	def subscribers(self, uuid):
		if not uuid or uuid == subscription.fs_uuid:
			return self.reply_lines(subscription.subscriber_lines())
		for h in tracker.handlers:
			if h.peer_uuid == uuid:
				return self.reply_lines(h.subscriber_lines())
		self.reply_OK("No such accumulator %s\r\n" % uuid)

	def http_request(self):
		reqpath = self.path.lower()
		if reqpath in self.URLs:
			return self.URLs[reqpath](self)
		if reqpath == '/':
			return self.status()
		if reqpath.startswith("/subscribers"):
			query = reqpath[1:]
			if '/' not in query:
				return self.reply_OK("Use /Subscribers/[accumulator_uuid]\r\n")
			dir, uuid = query.split('/')
			return self.subscribers(uuid)
		self.reply_content("HTTP/1.1 404 Not Found\r\n", http_404 % self.path)

	def status_lines(self):
		local_host = self.getsockname()[0]
		yield "--- Accumulators ---"
		for l in self.accumulator_lines():
			yield l
		yield "\r\n--- Disks ---"
		for l in tracker.disk_lines(local_host, self.remote_host):
			yield l
		yield "\r\n--- Segments ---"
		for l in tracker.segment_lines():
			yield l
		yield "\r\n--- Reporters ---"
		for l in tracker.reporter_lines(local_host, self.remote_host):
			yield l

	def accumulator_lines(self):
		local_host = self.getsockname()[0]
		s = subscription
		repl = s.replication_factor()
		yield '\t'.join(tracker.uptime_fields(s.fs_uuid, repl, s.min_repl))
		for l in tracker.accumulator_lines(local_host, self.remote_host):
			yield l



class segment(segment_descriptor):
	def put(self):
		subscription.fs_queue.out(cmd_msg(self.channel, "Release Segment"))
		self.reporter = None
		if self not in subscription.fs_queue.segment_request_queue:
			subscription.fs_queue.sd_put(self)

	def get(self, reporter_ch):
		subscription.fs_queue.out(cmd_msg(self.channel, str(reporter_ch), "Acquire Segment"))

	def log(self, content):
		subscription.fs_queue.out(cmd_msg(self.channel, content, "SEG", "Log"))

	def open(self, name, piece_length):
		if self.mapname:
			raise RuntimeError("Cannot open %s, already %s" % (name, self.mapname))
		self.mapname = name

		self.bin.open(name, ".bin")
		self.pieces.open(name, "-pieces.bin", piece_length)
		self.reporters.open(name, "-reporters.tsv")
		self.entries.open(name, "-entries.tsv")

		if self.reporter:
			self.reporter.done_ack()	# from self.request_segname(), self.renew()
		else:
			subscription.fs_queue.sd_put(self)

	def request_segname(self, reporter):
		queue = subscription.fs_queue.segment_request_queue
		yield (subscription.fs_queue, self.fs_request_segment(reporter, queue))
		yield publisher_handler.barrier		# self.open() calls reporter.done_ack() later

	def entry(self, modepath):
		assert self.mapname
		if not self.commissioned:
			cookie = subscription.ack_cookies.next()
			yield (subscription.fs_queue, self.fs_commission_segment(cookie), cookie)
			yield publisher_handler.barrier
		if self.reporter:
			cookie = subscription.ack_cookies.next()
			yield (subscription, self.fd_reporter(cookie), cookie)
			yield publisher_handler.barrier

		yield (subscription.fs_queue, self.fs_entry(modepath))

	def attribute(self, attrs):
		assert self.mapname
		yield (subscription, self.fd_attribute(attrs))

	def write(self, datachunks):
		assert self.mapname
		yield (subscription, self.fd_write(datachunks))

	def abort(self):
		assert self.mapname
		yield (subscription, self.fd_abort())
		yield (subscription.fs_queue, self.fs_entry_abort())

	def commit(self):
		assert self.mapname
		yield (subscription, self.fd_commit())
		yield (subscription.fs_queue, self.fs_entry_commit())

	def commit_sync(self):
		assert self.mapname
		cookie = subscription.ack_cookies.next()
		yield (subscription, self.fd_commit_sync(cookie), cookie)
		yield publisher_handler.barrier
		cookie = subscription.ack_cookies.next()
		yield (subscription.fs_queue, self.fs_entry_commit(cookie), cookie)
		yield publisher_handler.barrier

	def renew(self, reporter):
		assert self.mapname
		cookie = subscription.ack_cookies.next()
		yield (subscription, self.fd_retire(cookie), cookie)
		yield publisher_handler.barrier
		queue = subscription.fs_queue.segment_request_queue
		yield (subscription.fs_queue, self.fs_renew_segment(reporter, queue))
		yield publisher_handler.barrier		# self.open() calls reporter.done_ack() later

	def reclaim_sync(self):
		assert self.mapname
		for m in self.abort():
			yield m
		cookie = subscription.ack_cookies.next()
		yield (subscription, self.fd_sync_put(cookie), cookie)
		yield publisher_handler.barrier



class connection:
	def __init__(self, channel):
		self.channel = channel

	def connected_to(self, host, port):
		subscription.fs_queue.out(cmd_msg(self.channel, str(port), host, "Connected to"))

	def log_data(self, data):
		subscription.fs_queue.out(raw_msg(data, DATA, self.channel))

	def log(self, level, content):
		subscription.fs_queue.out(cmd_msg(self.channel, content, level, "Log"))

	def disconnected(self):
		subscription.fs_queue.out(cmd_msg(self.channel, "Disconnected"))



class tracker_queue_handler(msg_handler):
	def __init__(self, uuid, host, port):
		msg_handler.__init__(self, host, port)
		self.uuid = uuid

		self.segment_request_queue = []
		self.pub_queue = []
		self.sd_cache = []

		width = subscription.segment_width
		self.sd_pool = [segment(ch + 1, subscription) for ch in range(width)]
		self.cd_pool = [connection(ch + 1) for ch in range(width, 254)]

		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, AR_QUEUE_SENDBUF)

	def got_handshake(self):
		info = (self.uuid, "Accumulator Reporter", "Info", \
			str(subscription.ar_port), "TCP Port", \
			str(FS_PIECE_LENGTH), "Piece Length", \
			str(subscription.segment_length), "Segment Length", \
			str(subscription.segment_width), "Maximum Concurrent Segments", \
			str(subscription.min_repl), "Minimum Replication Factor", )

		self.out(ctl_msg(*info))

	def got_info(self):
		assert self.peer_role == "Archive Tracker"
		subscription.fill_ar_info(self.uuid, self.peer_uuid, \
			self.peer_info["File System Name"], self.peer_info["Path Max"])

	def ctl(self):
		seq = [s for s in self.unpack_seq()]
		if seq[1] == "Segment Name":
			sd = self.segment_request_queue.pop(0)
			sd.open(seq[0], FS_PIECE_LENGTH)
		if seq[1] == "Ack":
			subscription.resolve_ack(int(seq[0]), self)

	handlers = { CTL: ctl }

	def available_segments(self):
		return len(self.sd_cache) + len(self.sd_pool)

	def sd_put(self, sd):
		assert sd not in self.sd_cache
		assert sd not in self.sd_pool
		assert sd not in self.segment_request_queue
		assert not sd.meter_len
		assert not sd.reporter

		if sd.mapname:
			self.sd_cache.append(sd)
		else:
			self.sd_pool.append(sd)

	def sd_get(self):
		if self.sd_cache:
			return self.sd_cache.pop(0)
		if self.sd_pool:
			return self.sd_pool.pop(0)

	def queue_issue(self, publisher):
		assert publisher not in self.pub_queue
		self.pub_queue.append(publisher)

	def writable(self):	# hot
		return self.out_buffer or self.pub_queue

	def handle_write(self):
		issues = [publisher.publish_content() for publisher in self.pub_queue]
		self.pub_queue = []
		self.outl(d for msg in issues for d in msg)

		msg_handler.handle_write(self)

	def handle_close(self):
		sys.exit()



class publisher_handler(subscriber_handler):
	def __init__(self, *handler_args):
		subscriber_handler.__init__(self, *handler_args)

		self.issue_seq = []
		self.issue = None
		self.reply = None

	sentinel = 'S'
	barrier = (None, sentinel)

	def dispatch(self):
		if self.issue_seq and not self.issue:
			msg = self.issue_seq.pop(0)
			recipient, self.issue = msg[:2]
			if not recipient is None:
				recipient.queue_issue(self)
				if recipient is subscription.fs_queue and len(msg) > 2:
					subscription.ack_junction[msg[2]] = [self, [recipient]]

		if self.reply and not self.issue:
			self.out(self.reply)
			self.reply = None

	def done_ack(self):
		assert self.issue is self.sentinel or self.issue is None
		self.issue = None
		self.dispatch()

	def publish_content(self):
		for d in self.issue:
			yield d
		self.issue = None
		self.dispatch()

	def next_issue(self, issues):
		self.issue_seq.extend(issues)
		self.dispatch()

	def readable(self):	# hot
		return not self.issue and not self.reply and subscription.fs_uuid



def fd_abort_all(segments):
	for sd in segments:
		for m in sd.fd_abort():
			yield m

def fs_abort_put_all(segments):
	for sd in segments:
		for m in sd.fs_entry_abort(put=True):
			yield m

def abort_put_all(segments):
	yield (subscription, fd_abort_all(segments))
	yield (subscription.fs_queue, fs_abort_put_all(segments))


class accumulator_handler(publisher_handler):
	def __init__(self, *handler_args):
		publisher_handler.__init__(self, *handler_args)

		self.channel_map = {}
		self.cd = subscription.fs_queue.cd_pool.pop(0)
		self.cd.connected_to(self.remote_host, self.remote_port)

	def got_handshake(self):
		publisher_handler.got_handshake(self)

		self.cd.log("INFO", "Accumulator got handshake")

	def got_info(self):
		if self.peer_role == "Subscriber":
			self.outl(subscription.subscribe(self))
		else:
			self.out(ctl_msg(*subscription.stat_seq_fields()))

	def provision(self, ch):
		assert ch not in self.channel_map
		sd = subscription.fs_queue.sd_get()
		if not sd:
			self.cd.log("INFO", "Reporter provision (%d) failed -- busy" % ch)
			self.out(cmd_msg(ch, "Busy"))
			return

		sd.get(self.cd.channel)
		self.channel_map[ch] = sd
		self.cd.log("INFO", "Reporter provision (%d)" % ch)
		self.out(cmd_msg(ch, "Ok"))

	def size(self, ch, size):
		sd = self.channel_map[ch]
		pos = sd.bin.position
		size = int(size)
		if not sd.mapname:
			sd.log("(%s) Activate segment for %s file" % (sd.mapname, mb_str(size)))
			self.next_issue(sd.request_segname(self))
		elif pos and pos + size > subscription.segment_length:
			sd.log("(%s) Renew %s segment for %s file" % (sd.mapname, mb_str(pos), mb_str(size)))
			self.next_issue(sd.renew(self))
		else:
			sd.log("(%s) Ready at %s for %s file" % (sd.mapname, mb_str(pos), mb_str(size)))

	def entry(self, ch):
		sd = self.channel_map[ch]
		modepath = self.gobble()
		self.next_issue(sd.entry(modepath))

	def attribute(self, ch, attrs):
		sd = self.channel_map[ch]
		self.next_issue(sd.attribute(attrs))

	def write(self, ch):
		sd = self.channel_map[ch]
		self.next_issue(sd.write(self.chunks))

	def abort(self, ch):
		sd = self.channel_map[ch]
		self.next_issue(sd.abort())

	def commit(self, ch, cookie=None):
		sd = self.channel_map[ch]
		if cookie:
			self.next_issue(sd.commit_sync())
			self.reply = ctl_msg(cookie, "Ack")
		else:
			self.next_issue(sd.commit())

	def reclaim(self, ch):
		sd = self.channel_map.pop(ch)
		self.cd.log("INFO", "Reporter reclaim (%d)" % ch)
		if sd.mapname:
			self.next_issue(sd.reclaim_sync())
			self.reply = cmd_msg(ch, "Ok")
		else:
			sd.put()
			self.out(cmd_msg(ch, "Ok"))

	cmd_handlers = { \
		"Provision":		provision, \
		"File Size":		size, \
		"File Attribute":	attribute, \
		"Abort":		abort, \
		"Commit":		commit, \
		"Reclaim":		reclaim, }

	def ctl(self):
		self.cd.log_data(self.gobble())
		seq = [s for s in self.unpack_seq()]
		if seq[1] == "Ack":
			subscription.resolve_ack(int(seq[0]), self)

	handlers = { \
		CTL:		ctl, \
		SET:		publisher_handler.cmd_router, \

		ENTRY:		entry, \
		DATA:		write, }

	def protocol_error(self, err):
		self.cd.log("ERROR", '\n' + format_exc().strip()[-252:])
		self.handle_close()

	def handle_close(self):
		self.reply = None
		segs = self.channel_map.values()
		if segs:
			self.next_issue(abort_put_all(segs))
		self.cd.disconnected()
		subscription.fs_queue.cd_pool.append(self.cd)

		publisher_handler.handle_close(self)



class accumulator_server(asyncore.dispatcher):
	def __init__(self, handler, host, port):
		asyncore.dispatcher.__init__(self)
		self.handler = handler

		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()

		self.bind((host, port))
		self.listen(2)

	def handle_accept(self):
		sock, (host, port) = self.accept()

		h = self.handler(host, port, sock)
		h.handle_connect()


def fs_main(min_repl, fs_uuid, fs_name):
	global subscription, tracker

	subscription = subscription_queue(min_repl)
	tracker = archive_map(subscription, fs_name, FS_PIECE_LENGTH)

	subscription.fill_info(fs_uuid, fs_name, FS_PATH_MAX)

	s = y = None
	try:
		s = accumulator_server(metadata_handler, '', 9825)
		y = accumulator_server(tracker_status_handler, '', 4352)
		asyncore.loop()
	finally:
		s and s.close()
		y and y.close()
		print "Done listening"


def da_main(min_repl, da_uuid, fs_host, seg_width, seg_size):
	global subscription

	for p in range(9826, 9842):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		err = s.connect_ex(("127.0.0.1", p))
		if err:
			port = p
			break
		s.close()

	subscription = accumulator_queue(min_repl, seg_width, seg_size, AR_WRITE_MAX)
	subscription.fs_queue = tracker_queue_handler(da_uuid, fs_host, 9825)

	s = None
	try:
		subscription.fs_queue.connect()

		s = accumulator_server(accumulator_handler, '', port)

		subscription.ar_port = port
		asyncore.loop()
	finally:
		s and s.close()
		subscription.fs_queue.close()


def do_da(cmd, repl, host, seg_width, seg_megs):
	repl = int(repl.strip('x'))
	if '@' in host:
		da_uuid, host = host.split('@')
		da_uuid = str(uuid.UUID(da_uuid))
	else:
		da_uuid = str(uuid.uuid4())

	seg_width = int(seg_width.strip('w'))
	seg_bytes = int(seg_megs.strip('mMBb')) * 2**20
	if seg_width > AR_SEGMENT_CAP:
		print "Using maximum concurrency %dw" % SEGMENT_CAP
		seg_width = AR_SEGMENT_CAP

	try:
		da_main(repl, da_uuid, host, seg_width, seg_bytes)
	except:
		pass


def do_fs(cmd, repl, fs_name, fs_uuid=''):
	repl = int(repl.strip('x'))
	if fs_uuid:
		fs_uuid = str(uuid.UUID(fs_uuid))
	else:
		fs_uuid = str(uuid.uuid4())
		print "using UUID %s" % fs_uuid

	try:
		fs_main(repl, fs_uuid, fs_name)
	except Exception, err:
		raise
		print err


if __name__ == "__main__":
	args = sys.argv
	if len(args) < 3:
		print "ar.py x fsname [uuid] -OR-\nar.py x [uuid@]host w MB"
	elif len(args) < 5:
		do_fs(*args)
	else:
		do_da(*args)

