#!/usr/bin/env python
## put info for a single prefix into a single file (for later processing)
## input argument: date for which we have ris dump files
import glob
import re
import os
import sys
import subprocess
import radix
import arrow
import cPickle as pickle

# TODO record file corruption
# TODO record record reading problems
# TODO record peer activity for a given file
# TODO record peer state changes

DATE=sys.argv[1]
adate=arrow.get( DATE )
GLOB="/mnt/ris/*/%s/bview.%s.gz" % ( adate.format('YYYY.MM'), adate.format('YYYYMMDD.HHmm') )
BGPDUMP='/usr/bin/bgpdump' # make sure this is version 1.5+
OUTDIR='./data/%s' % ( adate.format('YYYY.MM.DD.HH') )
os.system("mkdir -p %s" % (OUTDIR) )

as2pfx = {}
r = radix.Radix()

for fname in glob.glob( GLOB ):
    print >>sys.stderr, "processing file: %s" % ( fname )
    command = '%s -t change -v -m %s' % ( BGPDUMP, fname )
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    data = {} # holds per prefix stuff
    for line in proc.stdout.readlines():
            liner = line.rstrip('\n')
            fields = liner.split('|')
            pfx = fields[5]
            path = fields[6]
            path_p = path.split()
            if len(path_p) > 0:
                origin_as = path_p[-1]
                data.setdefault( pfx , [] )
                data[ pfx ].append( liner )
                as2pfx.setdefault( origin_as, set() )
                as2pfx[ origin_as ].add( pfx )
                rnode = r.search_exact( pfx )
                if rnode:
                    # add to the existing node
                    # 'o' is 'origin'
                    rnode.data['o'].add( origin_as )
                else:
                    # add a new node
                    rnode = r.add( pfx )
                    rnode.data['o'] = set( [ origin_as ] )
            else:
                print >>sys.stderr, "EEPS no path: %s" % liner
    # create dirs
    pfx_list = data.keys()
    subdirs = set()
    print >>sys.stderr, "creating dirs"
    for p in pfx_list:
        pieces = re.split('[\.\:]', p)
        subdirs.add( pieces[0] )
    for subdir in subdirs:
        DIR = "/".join([OUTDIR,subdir])
        os.system("mkdir -p %s" % DIR )
    ## done
    print >>sys.stderr, "creating files"
    for pfx, lines in data.iteritems():
        pfx_file = pfx.replace('/','_')
        # make smaller dirs
        pieces = re.split('[\.\:]', pfx)
        subdir = pieces[0]
        DIR = "/".join([OUTDIR,subdir])
        with open("/".join([DIR,pfx_file]),'a') as outfa:
            for line in lines:
                print >>outfa, line

print >>sys.stderr, "finished parsing bviews"

# now pickle-save relevant data structs
as2pfx_file = "/".join([OUTDIR,"as2pfx.pickle"])
print >>sys.stderr, "saving as2pfx to %s" % as2pfx_file
pickle.dump( as2pfx, open( as2pfx_file, "wb" ) )

radix_file =  "/".join([OUTDIR,"radix.pickle"])
print >>sys.stderr, "saving radix to %s" % radix_file
pickle.dump( r,      open( radix_file, "wb" ) )
