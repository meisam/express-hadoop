#!/usr/bin/python

import sys
import getopt

def printList2Line(l):
	for entry in l:
		sys.stdout.write(entry);
		sys.stdout.write("\t");
	sys.stdout.write("\n");

def usage():
    print "Usage: hdfs_traffic2table.py [--help] [--printKey] [--printValue]"
    print """Example: cat datanode.log|grep HDFS|grep attempt| hdfs_traffic2table.py """


def main():
	if len(sys.argv) < 1:
		print "not enough arguments"
		usage()
		sys.exit()

	try:
		opts, args = getopt.getopt(sys.argv[1:], "hkv", ["help", "printKey", "printValue"])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	first_line = False
	print_value = False
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
			sys.exit()
		elif opt in ("-k", "--printKey"):
			first_line = True
		elif opt in ("-v", "--printValue"):
			print_value = True
		else:
			print "unhandled option"
			usage()
			sys.exit()

	while 1:
		log_line = sys.stdin.readline()            
		if not log_line:                       
			break
	
		#for word in log_line.split():
		#	print word
	
		log_segments = log_line.split()
		date_segment = log_segments[0];
		time_segment = log_segments[1];
		cliID_segment = log_segments[13];
		
		r = {}
		r['yy'] = date_segment.split('-')[0];
		r['mm'] = date_segment.split('-')[1];
		r['dd'] = date_segment.split('-')[2];
		
		r['hour'] = time_segment.split(':')[0];
		r['min'] = time_segment.split(':')[1];
		r['sec'] = time_segment.split(':')[2].split(',')[0];
		r['ms'] = time_segment.split(':')[2].split(',')[1];
	
		r['src'] = log_segments[5].lstrip('/').split(':')[0];
		r['dst'] = log_segments[7].lstrip('/').split(':')[0];
	
		r['bytes'] = log_segments[9].rstrip(",");
		r['op'] = log_segments[11].split(",")[0];
		r['offset'] = log_segments[15].split(",")[0];
		r['blockid'] = log_segments[19].split(",")[0];
		r['duration'] = log_segments[21].split(",")[0];
	
		r['jobid'] = cliID_segment.split('_')[2] + "_" + cliID_segment.split('_')[3]
		r['task'] = cliID_segment.split('_')[4]
		r['taskid'] = cliID_segment.split('_')[5]
		
		if first_line:
			printList2Line(r.keys())
			first_line = False
		
		if not print_value:
			break

		printList2Line(r.values())

if __name__ == "__main__":
	main()
