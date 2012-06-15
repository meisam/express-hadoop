#!/usr/bin/python

import sys
import getopt
import sqlite3
import string

def usage():
    print "Usage: txt2sqlite.py [--help] [--output sqliteFile [--name tableName]] [--keys keys [--types types]]"
    print """Example: cat table| txt2sqlite.py --output file --name table --keys 'key1,key2,...' """

def list2createSQL(keys, types, table):
	sql = "create table " + table +" ("
	for k, t in zip(keys, types):
		sql = sql + k + " " + t + ", "
	#sql = "'''" + sql + "PRIMARY KEY(" + primary + "))" + "'''"
	sql = sql.rstrip(', ')
	sql = sql + ")"
	#sql = "create table stocks(date text, trans text, symbol text, qty real, price real)"
	#print sql
	return sql

def list2insertSQL(keys, table):
	sql = "insert into " + table + " values "
	dummy = ["?" for k in keys]
	sql = sql + str(tuple(dummy))
	return string.replace(sql,"'","")
	

def main():
	if len(sys.argv) < 3:
		print "not enough arguments"
		usage()
		sys.exit()

	try:
		opts, args = getopt.getopt(sys.argv[1:], "ho:k:t:n:", ["help", "output", "keys" "types", "name"])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	cycle=5000
	nocreate = True
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt in ("-k", "--keys"):
			keys = arg.split()
			rawkeys = arg
		elif opt in ("-t", "--types"):
			types = arg.split(',')
			nocreate = False
		elif opt in ("-o", "--output"):
			db = arg
		elif opt in ("-n", "--name"):
			table = arg
		else:
			print "unhandled option"
			usage()
			sys.exit()
	
	conn = sqlite3.connect(db)
	c = conn.cursor()

	if not nocreate:
		c.execute(list2createSQL(keys, types, table))
	else:
		sql_insert = list2insertSQL(keys, table)
		progress=0
		while 1:
			txt_line = sys.stdin.readline()
			if not txt_line:
				break
			c.execute(sql_insert, tuple(txt_line.split()))	
			progress = progress + 1
			if progress >= cycle:
				conn.commit()
				progress = 0
		
	conn.commit()
	c.close()


if __name__ == "__main__":
	main()
