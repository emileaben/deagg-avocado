#!/usr/bin/env python
## put info for a single prefix into a single file (for later processing)
## input argument: date for which we have ris dump files
import glob
import numpy as np
import os
import sys
import json
import cPickle as pickle
import radix
import subprocess
import ipaddr
import arrow
import re
import hashlib

"TABLE_DUMP2|1476406525|B|193.242.98.143|35699|62.152.29.0/24|35699 174 8544 8544 8544 8544|IGP|193.242.98.143|0|0||NAG||"

class PfxStore:
    def __init__( self, basedir ):
        print >>sys.stderr, "loading! %s" % ( basedir )
        self.basedir = basedir
        self.radix   = pickle.load( open('%s/radix.pickle' % basedir  ,'r') )
        self.as2pfx  = pickle.load( open('%s/as2pfx.pickle' % basedir ,'r') )
        ## ignore_peers: these are the route-collector peers that are to be ignored
        self.ignore_peers = radix.Radix()
        self.has_ignore_peers = False # to avoid a lot of radix lookups
        print >>sys.stderr, "loaded!"

    def set_ignore_peers( self, pfx_list ):
        for p in pfx_list:
            self.ignore_peers.add( p )
        self.has_ignore_peers = True # need to do a lot of radix lookups now

    def peer_count( self, pfx ):
        '''
        get the peer count for a prefix
        '''
        pfx_file = pfx.replace('/','_')
        pieces = re.split('[\.\:]', pfx)
        subdir = pieces[0]
        DIR = "/".join([ self.basedir ,subdir])
        return sum(1 for line in open("/".join([DIR,pfx_file]),'r'))

    def routes_for_pfx( self, pfx ):
        '''
        load routes for a particular prefix into a data structure. keyed by peer_ip/peer_asn
        ignores routes in self.ignore_peers if that is set
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
                if self.has_ignore_peers == True:
                    ignore_match = self.ignore_peers.search_best( peer_ip )
                    if ignore_match:
                        #print >>sys.stderr, "ignoring peer_ip: %s" % ( peer_ip )
                        continue
                peer_asn = parts.pop(0)
                peer_id = "%s|%s" % (peer_ip,peer_asn)
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

def route_signature( p, r ):
    '''
    create a signature of how a route is propagates (as seen from peers)
    sort and md5?
    '''
    sorted_peers = sorted( r.keys() )
    sig = ''
    for peer in sorted_peers:
        sig += "%s|%s" % ( peer, r[peer]['route'] )
    return hashlib.md5( sig ).hexdigest()

def route_signature_path_only( p, r ):
    '''
    create a signature of how a route is propagates (as seen from peers)
    sort and md5?
    '''
    sorted_peers = sorted( r.keys() )
    sig = ''
    for peer in sorted_peers:
        #sig += "%s|%s" % ( peer, r[peer]['route'] )
        # only consider the first attribute, which is path:
        sig += "%s|%s" % ( peer, r[peer]['route'][0] )
    return hashlib.md5( sig ).hexdigest()

def find_groups( p, pfxset, consider_only_path=False ):
    '''
    find groups of similarly routed prefixes
    '''
    groups = {}
    #print >>sys.stderr, "finding groups, consider_only_paths:%s" % ( consider_only_path )
    for pfx in pfxset:
        r = p.routes_for_pfx( pfx )
        # create a signature
        rsig = None
        if consider_only_path == False:
            rsig = route_signature( p, r )
        else:
            rsig = route_signature_path_only( p, r )
        # try to find the signature
        if rsig in groups:
            # add the pfx
            groups[ rsig ]['pfx_set'].add( pfx )
        else:
            # it's a new one
            groups[ rsig ] = {
                'pfx_set': set([ pfx ]),
                'route_set': r
            }
            if consider_only_path == True:
                # this chops off all other attributes for the route that is stored per peer
                for peer in groups[ rsig ]['route_set']:
                    groups[ rsig ]['route_set'][ peer ]['route'] = [ groups[ rsig ]['route_set'][ peer ]['route'][0] ]
    return groups

def ip_other_half( pfx ):
    pfx_net, pfx_mask = pfx.split('/')
    pfx_mask = int( pfx_mask )
    if ':' in pfx:
        this_half = ipaddr.IPv6Address( pfx_net )
        other_half = ipaddr.IPv6Address( int(this_half) ^ 2 ** ( 128 - pfx_mask ) )
    else:
        this_half = ipaddr.IPv4Address( pfx_net )
        other_half = ipaddr.IPv4Address( int(this_half) ^ 2 ** ( 32  - pfx_mask ) )
    return "%s/%s" % ( other_half, pfx_mask )

def ip_1bit_less_specific( pfx ):
    '''
    find the 1bit less specific supernet of a prefix
    '''
    pfx_net, pfx_mask = pfx.split('/')
    pfx_mask = int( pfx_mask )
    super_mask = pfx_mask - 1
    super_addr = None
    if ':' in pfx:
        this_net   = ipaddr.IPv6Address( pfx_net )
        shift_mask = 128-super_mask
        super_addr = ipaddr.IPv6Address( ( int(this_net) >> shift_mask ) << shift_mask )
    else:
        this_net   = ipaddr.IPv4Address( pfx_net )
        shift_mask = 32-super_mask
        super_addr = ipaddr.IPv4Address( ( int(this_net) >> shift_mask ) << shift_mask )
    return "%s/%s" % ( super_addr, super_mask )

def try_pfx_merge( p, pfx_set ):
    '''
    we have a set of prefixes with similar routing, now analyse them
    '''
    ## bymask: groups prefixes by pfx length so we can analyse longest prefix first
    bymask = {}
    for pfx in pfx_set:
        pnet,pmask = pfx.split('/')
        pmask = int( pmask )
        if pmask == 0: # don't count default! #TODO maybe remove earlier?
            continue
        bymask.setdefault( pmask, set() )
        bymask[ pmask ].add( pfx )
    ## start at longest prefixes, and work up until you don't find prefixes anymore
    mask = max( bymask.keys() )
    merged_pfx_set = set()
    while len( bymask.keys() ) > 0:
        if mask in bymask:
            while len( bymask[ mask ] ) > 0:
                ## find the other side
                p1 = bymask[ mask ].pop()
                other_half = ip_other_half( p1 )
                if other_half in bymask[ mask ]:
                    bymask[ mask ].remove( other_half )
                    one_bit_less_specific = ip_1bit_less_specific( p1 )
                    bymask.setdefault( mask-1, set() )
                    bymask[ mask-1 ].add( one_bit_less_specific )
                else:
                   merged_pfx_set.add( p1 )
            del bymask[ mask ]
        # walk to one-bit less specific
        mask -= 1
    return merged_pfx_set

def discard_aggregates_with_diff_routing_policy( groups, this_sig, merged_pfx_set ):
    '''
    compare the results of aggregation with the other groups of prefixes, and remove the prefixes that are
    already in other groups of routing identity
    '''
    res_pfx_set = merged_pfx_set.copy() # result pfx set (may be a redundant copy, python boffins may know)
    for sig,g in groups.iteritems():
        if sig == this_sig:
            continue
        group_pfx_set = g['pfx_set']
        for pfx in group_pfx_set:
            if pfx in res_pfx_set:
                res_pfx_set.remove( pfx )
    return res_pfx_set

def pfxset_lengths( pfxset ):
    return map( lambda x: int( x.split('/')[1] ), pfxset )

def pfxset_avg_len( pfxset ):
    return 1.0*sum( map( lambda x: int( x.split('/')[1] ), pfxset ) ) / len(pfxset)

def read_config():
    conf = {}
    conf_file = "config.json"
    try:
        with open('config.json','r') as inf:
            conf = json.load( inf )
    except:
        print >>sys.stderr, "error reading conf file: %s" % conf_file
        raise
    return conf

if __name__ == "__main__":
    conf = read_config()
    if not 'date' in conf:
        print >>sys.stderr, "no 'date' in config.json, exiting"
        sys.exit(0)
    DATE=conf['date']
    adate=arrow.get( DATE )
    DATADIR='./data/%s' % ( adate.format('YYYY.MM.DD.HH') )
    p = PfxStore( DATADIR )

    # ignore peers has the pfxes that the peers to be ignored are in
    ### this is for sensitivity analysis
    # expects a list
    if 'ignore_peers' in conf:
        p.set_ignore_peers( conf['ignore_peers'] )
    CONSIDER_ONLY_PATH=False
    if 'consider_only_path' in conf:
        CONSIDER_ONLY_PATH=conf['consider_only_path']
    ## FOR BRAZIL
    #IGNORE_PEERS = ['187.16.216.0/21','2001:12f8::/64']
    ## TODO: FOR South Africa
    # print the config to stdout, so we know what was used
    print "### %s" % json.dumps( conf )

    for asn,all_pfxset in p.as2pfx.iteritems():
        all_pfxset_af = {
            4: set(),
            6: set()
        }
        for pfx in all_pfxset:
            if ':' in pfx:
                all_pfxset_af[6].add( pfx )
            else:
                all_pfxset_af[4].add( pfx )
        j = { # struct that holds it together
            'asn': asn,
            'all_pfx_cnt': len( all_pfxset )
        }
        for af in (4,6):
            global_pfxset = set()
            j['all_pfx_v%d_cnt' % af] = len( all_pfxset_af[ af ] )
            for pfx in all_pfxset_af[af]:
                # remove low visibility pfxes
                if p.peer_count( pfx ) >= 20:
                    global_pfxset.add( pfx )
            j['global_pfx_v%d_cnt' % af ] = len( global_pfxset )
            groups = find_groups( p, global_pfxset, consider_only_path=CONSIDER_ONLY_PATH )
            j['group_v%d_cnt' % af] = len( groups )
            aggr_pfx_set = set()
            ######## next line didn't result in many prefixes removed from the set. either it's a rare case, or there is a bug in the code
            ### aggr_pfx_no_up_diff_routing_set = set() ## removes the prefixes that were already present with different routing policy
            for sig,g in groups.iteritems():
                ja = {} # struct to dump to json for the 'atom'/'group'
                ja['atom_id'] = "%s-%s-%s" % ( asn, af, sig )
                #ja['route_set'] = g['route_set'] ## THIS IS BIG
                group_pfx_set = g['pfx_set']
                ja['pfx_list'] = list( group_pfx_set )
                if len( group_pfx_set ) > 0: # 0 can happen for filtering out non-globally routed prefixes
                    ## now find if we can aggregate these in a useful manner!
                    merged_pfx_set = try_pfx_merge( p, group_pfx_set )
                    ja['aggr_pfx_list'] =  list( merged_pfx_set )
                    if len( merged_pfx_set ) == 0:
                        raise ValueError("merge to 0?!, for %s" % (group_pfx_set) )
                    aggr_pfx_set |= merged_pfx_set
                ## see if any of the merged_pfx_set was already present for a different routing policy
                ##REMOVED## aggr_pfx_no_up_diff_routing_set |= discard_aggregates_with_diff_routing_policy( groups, sig, merged_pfx_set )
                print json.dumps( ja )
            j['aggr_pfx_v%d_cnt' % af] = len( aggr_pfx_set )
            ##REMOVED j['aggr_no_te_pfx_v%d_cnt' % af] = len( aggr_pfx_no_up_diff_routing_set )
        print json.dumps( j )

