#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: ziyuanliu
# @Date:   2014-11-28 15:21:22
# @Last Modified by:   ziyuanliu
# @Last Modified time: 2014-12-04 17:31:26

import argparse
import time
import fnmatch, re
import Queue
import logging
import shutil

from json import loads
from os import makedirs, remove, removedirs
from collections import defaultdict
from os.path import join, expanduser, exists, isdir, abspath, split, getsize
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from threading import Thread

DEFAULT_RULES_FN = '.nrules'
DEFAULT_TASK_FN = "tasks"
DEFAULT_SETTING_FN = ".pid"
DEFAULT_NYZR_DIR = '.nyzr'

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
def make_dir(dp):
	if not exists(dp):
		makedirs(dp)
	return dp

class File(object):
	"""docstring for File"""
	def __init__(self, fp):
		super(File, self).__init__()
		self.fp = expanduser(fp)
		self.filename = split(self.fp)[1]
		self.size = self._get_stable_size()
		self.cmds = {"move":self.move_to_dir,
			"delete":self.delete,
			"copy":self.copy_to_dir}

	def _get_stable_size(self):
		block_time = 0.5
		size = float(getsize(self.fp))
		while True:
			time.sleep(block_time)
			if size == float(getsize(self.fp)):
				logging.debug("file size stablized to %f"%size)
				break
			else:
				size = float(getsize(self.fp))
				block_time*=1.25
		return size

	def move_to_dir(self,directory):
		directory = expanduser(directory)
		if self.exists() and make_dir(directory):
			new_fp = join(directory,self.filename)
			logging.info("moving %s to %s"%(self.fp,new_fp))
			shutil.move(self.fp,new_fp)

	def copy_to_dir(self,directory):
		directory = expanduser(directory)
		if self.exists() and make_dir(directory):
			new_fp = join(directory,self.filename)
			logging.info("copying %s to %s"%(self.fp,new_fp))
			shutil.copy2(self.fp,new_fp)

	def delete(self):
		if self.exists():
			logging.info("deleting %s"%(self.fp))
			remove(self.fp)

	def exists(self):
		return exists(self.fp)
		
class NyzrManager(object):
	"""NyzrManager manages the queue and the basic terminal controls"""
	def __init__(self, task_num=10):
		super(NyzrManager, self).__init__()
		self.task_limit = task_num
		self.task_num = 0
		self.restart()

	def run():
		while True:
			pass

	def restart(self):
		pass

	def shutdown(self):
		pass

	def read_rules(path=None):
		if not path:
			path = join(join('~/',DEFAULT_NYZR_DIR),DEFAULT_RULES_FN)

		if not exists(path):
			raise Warning("Rules file: [%s] not found!")

		with open(path,'r') as f:
			content = f.read()

		js = loads(content)
		into_rules(js)

class Task(Thread):
	"""docstring for Task"""
	def __init__(self,fp):
		super(Task, self).__init__()
		self.fp = fp
		self.rmanager = RuleManager()

	def run(self):
		logging.info("starting operation")
		self.file = File(self.fp)

		#are any rules valid?
		self.operations = self.rmanager.apply_rules(self.fp)
		if not self.operations:
			return

		opMgr = OperationManager()
		for op in self.operations:
			ops = opMgr.operations[op]
			for i,o in enumerate(ops):
				cmd, args = o.split('#')
				
				if len(args)>0:
					self.file.cmds[cmd.lower()](args)
				else:
					self.file.cmds[cmd.lower()]()
		return 

class nyzr_handler(FileSystemEventHandler):
	def __init__(self,*args, **kwargs):
		super(nyzr_handler,self).__init__(*args,**kwargs)
		
	def add_to_operation_queue(self,src_path):
		t = Task(src_path)
		t.start()

	def on_moved(self, event):
		super(nyzr_handler, self).on_moved(event)

		what = 'directory' if event.is_directory else 'file'
		# print "Moved %s: from %s to %s" % (what, event.src_path, event.dest_path)
		src_dir,src_fn = split(event.src_path)
		dest_dir,dest_fn = split(event.dest_path)
		print dest_fn+'.crdownload',src_fn
		if src_dir==dest_dir and dest_fn+'.crdownload'==src_fn:
			#deal with crdownload files 
			self.add_to_operation_queue(event.dest_path)

	def on_created(self, event):
		super(nyzr_handler, self).on_created(event)

		what = 'directory' if event.is_directory else 'file'
		(directory,basename) = split(event.src_path)
		if basename[0]=='.' or '.crdownload' in basename:
			#igoring dotfiles and crdownload files
			return

		logging.info("Created %s: %s" % (what, event.src_path))
		self.add_to_operation_queue(event.src_path)

		# self.queue.put_nowait(t)

		

	def on_deleted(self, event):
		super(nyzr_handler, self).on_deleted(event)

		what = 'directory' if event.is_directory else 'file'
		print "Deleted %s: %s" % (what, event.src_path)


	def on_modified(self, event):
		super(nyzr_handler, self).on_modified(event)

		what = 'directory' if event.is_directory else 'file'
		print "Modified %s: %s" % (what, event.src_path)

class FilterManager(object):
	"""Singleton pattern to manage all the filters"""
	_instance = None
	def __new__(cls,*args,**kwargs):
		if not cls._instance:
			cls._instance = super(FilterManager,cls).__new__(cls,*args,**kwargs)
			cls._instance.filters={}
		return cls._instance

	def add_filters(self, name, value):
		filters = []
		for val in value:
			
			filters.append('#'.join((val,value[val])))
		self.filters[name]=filters

	@classmethod
	def filter_matched(cls,filtername,filepath):
		(dirname,basename) = split(filepath)
		try:
			filters = cls._instance.filters[filtername]
			for subfilter in filters:
				(filter_name,content) = subfilter.split('#')
				if filter_name == 'regex':
					reobj = re.compile(content)
					if not reobj.match(basename):
						return False
					else:
						logging.info("regex matched for %s"%basename)
				elif filter_name == "extension":
					if not fnmatch.fnmatch(basename,content):
						return False
					else:
						logging.info("extension matched: %s %s"%(basename,content))

				if filter_name == "size":
					file_size = float(getsize(filepath))/1000**2
					logging.info("file size [%f] "%(file_size))
					if '<' in content[0]:
						if float(content[1:])<file_size:
							return False
						else:
							logging.info("file size [%f] matched %s"%(file_size,content))
					elif float(content[1:])>=file_size:
						return False
					else:
						logging.info("file size [%f] matched %s"%(file_size,content))


				if filter_name == 'origin':
					pass
				
				return True

		except Exception, e:
			raise e
			return False
			
	def __str__(self):
		return " ".join(["%s: %s"%(s,self.filters[s]) for s in self.filters.keys()])

class OperationManager(object):
	"""Singleton pattern to manage all the operations"""
	_instance = None
	def __new__(cls,*args,**kwargs):
		if not cls._instance:
			cls._instance = super(OperationManager,cls).__new__(cls,*args,**kwargs)
			cls._instance.operations={}

		return cls._instance

	def add_operations(self, name, value):
		operations = []
		for d in value:
			for val in d:
				key = val
				operations.append('#'.join((key,d[key])))
		self.operations[name] = operations

	def __str__(self):
		return " ".join(["%s: %s"%(s,self.operations[s]) for s in self.operations.keys()])
				
class Rule(object):
	"""docstring for Rules"""
	def __init__(self,directory, filters=None, operations=None):
		super(Rule, self).__init__()
		self.directory = directory
		self.filters = filters
		self.operations = operations

	def does_apply(self,fp):
		#does the rules for this directory apply for this filename
		for f in self.filters:
			if not FilterManager.filter_matched(f,fp):
				return False
		return True
	
	def __str__(self):
		return "dir: %s filters: %s operations: %s"%(self.directory," ".join(self.filters)," ".join(self.operations))

class RuleManager(object):
	"""Singleton pattern to manage all the rules"""
	_instance = None
	def __new__(cls,*args,**kwargs):
		if not cls._instance:
			cls._instance = super(RuleManager,cls).__new__(cls,*args,**kwargs)
			cls._instance.rules = defaultdict(list)
			cls._instance.queue = Queue.LifoQueue()
		return cls._instance

	def add_rule(self,rule):
		self.rules[abspath(expanduser(rule.directory))].append(rule)

	def apply_rules(self,pathname):
		(dirname,basename) = split(pathname)
		for directory in self.rules.keys():
			absp = abspath(expanduser(directory))
			if absp==dirname:
				for rule in self.rules[directory]:
					if rule.does_apply(pathname):
						logging.info("does apply, going to apply operations: %s"%rule)
						return rule.operations						
		logging.info("did not fit any rules")		
		return None

	def __str__(self):
		return " ".join(["%s"%(", ".join([str(f) for f in self.rules[s]])) for s in self.rules.keys()])

def into_rules(dct):
	manager = RuleManager()
	fltMgr = FilterManager()
	opMgr = OperationManager()
	rlMgr = RuleManager()
	
	flts = dct['filters']
	for flt in flts.keys():
		fltMgr.add_filters(flt,flts[flt])

	ops = dct['operations']
	for op in ops.keys():
		opMgr.add_operations(op,ops[op])

	for directory in dct['directories']:		
		name = directory['directory_path']
		for rule in directory['rules']:
			filters = rule['filters']
			for n in name:
				operations = rule['operations']
				rlMgr.add_rule(Rule(n,filters,operations))



def read_rules(path=None):
	if not path:
		path = join(join('~/',DEFAULT_NYZR_DIR),DEFAULT_RULES_FN)

	if not exists(path):
		raise Warning("Rules file: [%s] not found!")

	with open(path,'r') as f:
		content = f.read()

	js = loads(content)
	into_rules(js)
	# print js



def make_default_dir():
	dir_path = join('~/',DEFAULT_NYZR_DIR)
	return make_dir(dir_path)

def initialize_program():
	#overwrite
	make_default_dir()

def write_pid(pid):
	dir_path = make_default_dir()
	pid_path = join(dir_path,DEFAULT_SETTING_FN)
	if not exists(pid_path):
		f = open(pid_path,'w')
	else:
		f = open(pid_path, 'a')
	f.write(pid)
	f.close()

def read_pids():
	dir_path = make_default_dir()
	pid_path = join(dir_path,DEFAULT_SETTING_FN)
	if not exists(pid_path):
		return []
	with open(pid_path,'r') as f:
		return f.readlines()


if __name__ == '__main__':
	#first read the rules file 
	rules_dir = expanduser('~')
	rules_location = join(rules_dir,DEFAULT_RULES_FN)

	read_rules('.nrules')
	nyzr = nyzr_handler()
	observer = Observer()
	path = expanduser("~/Downloads/")

	observer.schedule(nyzr, path, recursive=True)
	observer.start()

	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()



