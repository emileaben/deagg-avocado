#!/usr/bin/env python
## put info for a single prefix into a single file (for later processing)
## input argument: date for which we have ris dump files
import glob
import os
import sys
import cPickle as pickle
import subprocess
import ipaddr
import arrow
import re

"TABLE_DUMP2|1476406525|B|193.242.98.143|35699|62.152.29.0/24|35699 174 8544 8544 8544 8544|IGP|193.242.98.143|0|0||NAG||"

class PfxStore:
    def __init__( self, basedir ):
        print >>sys.stderr, "loading! %s" % ( basedir )
        self.basedir = basedir
        self.radix   = pickle.load( open('%s/radix.pickle' % basedir  ,'r') )
        self.as2pfx  = pickle.load( open('%s/as2pfx.pickle' % basedir ,'r') )
        print >>sys.stderr, "loaded!"

    def routes_for_pfx( self, pfx ):
        '''
        load routes for a particular prefix into a data structure. keyed by peer_ip/peer_asn
        '''
        d = {}
        pfx_file = pfx.replace('/','_')
        pieces = re.split('[\.\:]', pfx)
        subdir = pieces[0]
        DIR = "/".join([ self.basedir ,subdir])
        with open("/".join([DIR,pfx_file]),'r') as inf:
            for line in inf:
                line = line.rstrip('\n')
                parts = line.split('|')
                parts = parts[1:] # pop table dump
                ts = int(parts.pop(0))
                parts = parts[1:] # pop 'B'
                peer_ip = parts.pop(0)
                peer_asn = parts.pop(0)
                peer_id = (peer_ip,peer_asn)
                parts.pop(0) # pop the prefix, which we know already
                d[ peer_id ] = {'ts': ts, 'route': parts} # what's left
        return d

    def analyse_pairs( self, pairs ):
        all_sim  = 0
        no_sim   = 0
        some_sim = 0
        for pair in pairs:
            r1 = self.routes_for_pfx( pair[0] )
            r2 = self.routes_for_pfx( pair[1] )
            peers = set(r1.keys()) | set(r2.keys())
            ts_diffs = []
            sim = 0
            dissim = 0
            for peer in peers:
                ## how similar are they!?
                if peer in r1 and peer in r2:
                    route1 = '|'.join( r1[ peer ]['route'] )
                    route2 = '|'.join( r2[ peer ]['route'] )
                    ts_diffs.append( abs( r1[ peer ]['ts'] - r2[ peer ]['ts'] ) )
                    if route1 == route2:
                        sim += 1
                        #similar!
                    else:
                        #dissimilar
                        dissim += 1
                else:
                    #dissimilar #TODO# count separately
                    dissim += 1
            if   sim == 0:
                no_sim += 1
            elif dissim == 0:
                all_sim += 1
            else:
                some_sim += 1
        plen = len( pairs )
        return "pairs:%d sim/some/no: %.1f%%/%.1f%%/%.1f%%" % ( 
            plen,
            100.0 * all_sim  / plen,
            100.0 * some_sim / plen,
            100.0 * no_sim   / plen
        )

    def findpairs( self, pfxset ):
        '''
        from a set of prefixes, finds the 'other half' of a prefix
        returns a list of tuples of prefix pairs
        '''
        pairs = []
        pfxset_copy = pfxset.copy()
        while len( pfxset_copy ) > 0:
            try:
                p1 = pfxset_copy.pop()
                # find the other half
                p1_net,p1_mask = p1.split('/')
                p1_mask = int( p1_mask )
                if p1_mask == 0:
                    continue
                this_half = None
                other_half = None
                if ':' in p1:
                    this_half = ipaddr.IPv6Address( p1_net )
                    other_half = ipaddr.IPv6Address( int(this_half) ^ 2 ** ( 128 - p1_mask ) )
                else:
                    this_half = ipaddr.IPv4Address( p1_net )
                    other_half = ipaddr.IPv4Address( int(this_half) ^ 2 ** ( 32  - p1_mask ) )
                other_half = "%s/%s" % ( other_half, p1_mask )
                if other_half in pfxset:
                    pairs.append( sorted([ p1, other_half ]) )
                    pfxset_copy.remove( other_half )
            except KeyError:
                break
        return pairs


DATE=sys.argv[1]
adate=arrow.get( DATE )
DATADIR='./data/%s' % ( adate.format('YYYY.MM.DD.HH') )

p = PfxStore( DATADIR )

for asn,pfxset in p.as2pfx.iteritems():
    pairs = p.findpairs( pfxset )
    if len( pairs ) > 0:
        pa_txt = p.analyse_pairs( pairs )
        print "%s\t%s" % ( asn, pa_txt )

