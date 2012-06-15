#!/usr/bin/python

import sys
import getopt
import sqlite3
import string

def usage():
    print "Usage: hdfs_sqlite2stat.py [--help] --file db --name [--all] jobid [--remote] [--local]"

def sql_stat_local(table, c):
	sql = "select op, sum(bytes), sum(duration), sum(1) from " + table + " where src=dst group by op";
	c.execute(sql);
	print "op\tbytes\tduration\toccurance"
	print_tuple_result(c)

def sql_stat_remote(table,c ):
	sql = "select op, sum(bytes), sum(duration), sum(1) from " + table + " where src!=dst group by op";
	c.execute(sql);
	print "op\tbytes\tduration\toccurance"
	print_tuple_result(c)

def sql_get_all_tables(c):
	sql = "SELECT name FROM sqlite_master WHERE type='table'"
	c.execute(sql)
	res = list()
	for row in c:
		res.append(row[0])
	return res

def gen_print_result(c, printer):
	for row in c:
		printer(row)

def print_tuple2list(t):
	for elem in t:
		sys.stdout.write(str(elem))
		sys.stdout.write("\t")
	sys.stdout.write("\n")

def print_tuple_result(c):
	gen_print_result(c, print_tuple2list)

def main():
	if len(sys.argv) < 5:
		print "not enough arguments"
		usage()
		sys.exit()

	try:
		opts, args = getopt.getopt(sys.argv[1:], "hf:n:rla", ["help", "file", "name" "remote", "local", "all"])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt in ("-f", "--file"):
			db = arg
		elif opt in ("-n", "--name"):
			tables = list()
			tables.append(arg)

	conn = sqlite3.connect(db)
	c = conn.cursor()

	#initial
	calcRemote = calcLocal = False
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			_dummy = 1;	
		elif opt in ("-a", "--all"):
			tables = sql_get_all_tables(c)
		elif opt in ("-f", "--file"):
			_dummy = 1;
		elif opt in ("-n", "--name"):
			_dummy = 1;
		elif opt in ("-r", "--remote"):
			calcRemote = True
		elif opt in ("-l", "--local"):
			calcLocal = True
		else:
			print "unhandled option"
			usage()
			sys.exit()
		
	for table in tables:
		print "@@" + table
		if calcRemote:
			print "@@@@calcRemote"
			sql_stat_remote(table,c);
		if calcLocal:
			print "@@@@calcLocal"
			sql_stat_local(table,c);

	conn.commit()
	c.close()


if __name__ == "__main__":
	main()
