#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: ziyuanliu
# @Date:   2014-11-28 15:21:22
# @Last Modified by:   ziyuanliu
# @Last Modified time: 2014-11-28 23:49:06

import argparse
import time
import fnmatch, re

from json import loads
from os import makedirs
from collections import defaultdict
from os.path import join, expanduser, exists, isdir, abspath, split, getsize
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

DEFAULT_RULES_FN = '.nrules'
DEFAULT_TASK_FN = "tasks"
DEFAULT_SETTING_FN = ".pid"
DEFAULT_NYZR_DIR = '.nyzr'

class Task(object):
	"""docstring for Task"""
	def __init__(self):
		super(Task, self).__init__()

class nyzr_handler(FileSystemEventHandler):
	def __init__(self,*args, **kwargs):
		super(nyzr_handler,self).__init__(*args,**kwargs)
		self.rmanager = RuleManager()

	def on_moved(self, event):
		super(nyzr_handler, self).on_moved(event)

		what = 'directory' if event.is_directory else 'file'
		# print "Moved %s: from %s to %s" % (what, event.src_path, event.dest_path)

	def on_created(self, event):
		super(nyzr_handler, self).on_created(event)

		what = 'directory' if event.is_directory else 'file'
		print "Created %s: %s" % (what, event.src_path)
		#wait for file to stablize 
		size = float(getsize(event.src_path))
		while True:
			time.sleep(0.5)
			if size == float(getsize(event.src_path)):
				break
			else:
				size = float(getsize(event.src_path))
				print "still stablizing %d"%size

		self.rmanager.apply_rules(event.src_path)

	def on_deleted(self, event):
		super(nyzr_handler, self).on_deleted(event)

		what = 'directory' if event.is_directory else 'file'
		# print "Deleted %s: %s" % (what, event.src_path)


	def on_modified(self, event):
		super(nyzr_handler, self).on_modified(event)

		what = 'directory' if event.is_directory else 'file'
		# print "Modified %s: %s" % (what, event.src_path)

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
						print "regex matched for %s"%basename
				elif filter_name == "extension":
					if not fnmatch.fnmatch(basename,content):
						return False
					else:
						print "extension matched: %s %s"%(basename,content)

				if filter_name == "size":
					file_size = float(getsize(filepath))/1000**2
					print "file size [%f] "%(file_size)
					if '<' in content[0]:
						if float(content[1:])<file_size:
							return False
						else:
							print "file size [%f] matched %s"%(file_size,content)
					elif float(content[1:])>=file_size:
						return False
					else:
						print "file size [%f] matched %s"%(file_size,content)


				if filter_name == 'origin':
					pass
				print "filter matched: ",filtername
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
		print "Rule matched for",f
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
		return cls._instance

	def add_rule(self,rule):
		self.rules[abspath(expanduser(rule.directory))].append(rule)

	def apply_rules(self,pathname):
		(dirname,basename) = split(pathname)
		print "checking rules for",dirname,":", basename
		for directory in self.rules.keys():
			absp = abspath(expanduser(directory))
			if absp==dirname:
				for rule in self.rules[directory]:
					if rule.does_apply(pathname):
						#create task from operations
						return
						
					



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





def make_dir():
	dir_path = join('~/',DEFAULT_NYZR_DIR)
	if not exists(dir_path):
		makedirs(dir_path)
	return dir_path

def initialize_program():
	#overwrite
	make_dir()

def write_pid(pid):
	dir_path = make_dir()
	pid_path = join(dir_path,DEFAULT_SETTING_FN)
	if not exists(pid_path):
		f = open(pid_path,'w')
	else:
		f = open(pid_path, 'a')
	f.write(pid)
	f.close()

def read_pids():
	dir_path = make_dir()
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
	print path
	observer.schedule(nyzr, path, recursive=True)
	observer.start()

	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()



