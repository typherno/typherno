#
# Copyright (c) 2012 Nate Diller.
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


def parent_dir(target):
	if '/' not in target[1:]:
		return '/'
	return target[:target.rfind('/')]


def root_component(target):
	if '/' not in target[1:]:
		return target
	return target[:target.find('/', 1)]


class record_entry:
	def __init__(self, fd, offset=None, length=0):
		self.fd = fd
		self.record_offset = offset
		self.record_length = length

		self.prev_length = 0
		self.segment_name = ''
		self.segment_offset = 0
		self.file_length = 0
		self.file_hash = ''
		self.root_ptr = ''
		self.sibling_ptr = ''
		self.dir_mentions = ''
		self.modepath = ''

	def __str__(self):
		return "%10d ---> %s  %s" % (self.record_offset, ''.join(self.file_fields()), self.modepath)

	def file_fields(self):
		yield self.segment_name
		yield "@%d" % self.segment_offset
		yield "%14d" % self.file_length
		yield "  %s" % self.file_hash

	def boot(self):
		self.fd.seek(0, 2)
		flen = self.fd.tell()
		self.fd.seek(max(-flen, -8192), 2)
		off = self.fd.tell()
		buf = self.fd.read(8192)
		b = buf.rfind('\n')
		a = buf.rfind('\n', 0, b)
		self.record_offset = off + a + 1
		self.record_length = b - a
		return buf[a+1:b+1]

	def fetch(self):
		if self.record_offset is None:
			fields = self.boot().split('\t')
		else:
			self.fd.seek(self.record_offset)
			fields = self.fd.read(self.record_length).split('\t')

		self.prev_length = int(fields[0], 16)
		self.segment_name = fields[1]
		self.segment_offset = int(fields[2], 16)
		self.file_length = int(fields[3])
		self.file_hash = fields[4]
		if fields[5] != '-':
			pointers = fields[5].split(' ')
			self.root_ptr, self.sibling_ptr, self.dir_mentions = pointers
			if self.dir_mentions.endswith('/'):
				assert self.sibling_ptr != '^'
				self.dir_mentions += self.sibling_ptr
		self.modepath = fields[6].strip()

	def prev(self):
		plen = self.prev_length
		if not plen:
			return None
		return record_entry(self.fd, self.record_offset - plen, plen)

	def prev_mention(self, target):
		patha = target.split('/')[1:]
		pathb = self.modepath.split('/')[1:]
		mentions = self.dir_mentions.split('/')[1:]
		ptr = '-'
		for i in range(min(len(patha), len(pathb), len(mentions))):
			if patha[i] != pathb[i]:
				break
			if mentions[i]:
				ptr = mentions[i]

		if ptr == '-':
			return None
		poff, plen = ptr.split(':')
		return record_entry(self.fd, int(poff, 16), int(plen, 16))

	def prev_sibling(self):
		if self.sibling_ptr == '-':
			return None
		if self.sibling_ptr == '^':
			return self.prev()
		poff, plen = self.sibling_ptr.split(':')
		return record_entry(self.fd, int(poff, 16), int(plen, 16))

	def prev_root(self):
		if self.root_ptr == '-':
			return None
		if self.root_ptr == '^':
			return self.prev()
		poff, plen = self.root_ptr.split(':')
		return record_entry(self.fd, int(poff, 16), int(plen, 16))

	def list_root(self):
		rec = self
		mentioned = []
		while(rec):
			rec.fetch()
			if not rec.modepath.startswith('/'):
				rec = rec.prev()
			else:
				p = root_component(rec.modepath)[1:]
				if p not in mentioned:
					yield rec
					mentioned.append(p)
				rec = rec.prev_root()

	def enumerate(self):
		rec = self
		mentioned = []
		while(rec):
			rec.fetch()
			if rec.modepath.startswith('/'):
				p = parent_dir(rec.modepath)[1:]
				if p not in mentioned:
					yield "/%s" % p
					mentioned.append(p)
			rec = rec.prev()


class lookup_state:
	def __init__(self, start_record, target):
		self.start_record = start_record
		self.could_be_file = True
		could_be_file = True
		if target.endswith('/'):
			could_be_file = False
			target = target.rstrip('/')
		if not target:
			target = '/'
		self.target = target
		rootname = self.rootname = root_component(target)
		parent = self.parent = parent_dir(target)

		rec = start_record
		if self.target != '/':
			while rec:
				rec.fetch()
				if rec.modepath.startswith(rootname):
					break
				if rec.modepath.startswith('/'):
					rec = rec.prev_root()
				else:
					rec = rec.prev()
		self.search_record = rec
		if could_be_file:
			while rec:
				rec.fetch()
				if rec.modepath.startswith(target):
					break
				if parent_dir(rec.modepath) == parent:
					break
				rec = rec.prev_mention(target)
			self.search_record = rec
		else:
			self.not_file()

	def not_file(self):
		if not self.could_be_file:
			return
		target = self.target
		if target == '/':
			return
		rec = self.search_record
		while rec:
			rec.fetch()
			if rec.modepath.startswith(target + '/'):
				break
			rec = rec.prev_mention(target)
		self.search_record = rec
		self.could_be_file = False

	def lookup_record(self):
		if not self.could_be_file:
			return None
		target = self.target
		parent = self.parent
		rec = self.search_record
		while(rec):
			rec.fetch()
			if parent_dir(rec.modepath) == parent:
				break
			rec = rec.prev_mention(parent)
		while(rec):
			rec.fetch()
			if rec.modepath == target:
				return rec
			rec = rec.prev_sibling()
		self.not_file()

	def list_files(self):
		target = self.target
		rec = self.search_record
		while(rec):
			rec.fetch()
			if parent_dir(rec.modepath) == target:
				yield rec
				rec = rec.prev_sibling()
			else:
				rec = rec.prev_mention(target)

	def subtree(self):
		target = self.target
		rec = self.search_record
		mentioned = []
		while(rec):
			rec.fetch()
			if rec.modepath.startswith(target):
				dir = parent_dir(rec.modepath)
				if dir not in mentioned:
					yield dir
					mentioned.append(dir)
			rec = rec.prev_mention(target)

	def subdirs(self):
		target = self.target
		i = len(target) + 1
		mentioned = []
		for dir in self.subtree():
			cname = dir[i:]
			if '/' in cname:
				cname = cname[:cname.find('/')]
			if cname and cname not in mentioned:
				yield '/'.join((target, cname))
				mentioned.append(cname)


def readdir(start_record, target='/'):
	target = target.rstrip('/')
	if target:
		ls = lookup_state(start_record, target)
		for dir in ls.subdirs():
			yield (dir, '')
		for rec in ls.list_files():
			yield (rec.modepath, ''.join(rec.file_fields()))
	else:
		for rec in start_record.list_root():
			dir = root_component(rec.modepath)
			if dir != rec.modepath:
				yield (dir, '')
			else:
				yield (rec.modepath, ''.join(rec.file_fields()))


if __name__ == "__main__":
	import sys
	fd = open("archive.tsv", 'rb')

	start_record = record_entry(fd)
	for p, r in readdir(start_record, *sys.argv[1:]):
		if r:
			sys.stdout.write("%s\n %s\n" % (p, r))
		else:
			sys.stdout.write("%s\n" % p)


