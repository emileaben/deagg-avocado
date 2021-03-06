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

def find_groups( p, pfxset ):
    '''
    find groups of similarly routed prefixes
    '''
    groups = {}
    for pfx in pfxset:
        r = p.routes_for_pfx( pfx )
        # create a signature
        rsig = route_signature( p, r )
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

RFIELD2VAL = {
    0: 'path',
    1: 'origin',
    2: 'nexthop',
    4: 'med',
    5: 'comm'
}

def find_group_relation( g1, g2 ):
    '''
    classify the relationship of a group of prefixes
    this is established if any pair between them is:
      - overlapping the other:  'overlap'
      - adjacent to the other:  'adjacent'
    '''

def compare_groups( g1, g2 ):
    r = {} # result data structure
    # get the route sets and peer sets
    rs1 = g1['route_set']
    rs2 = g2['route_set']
    peer_set_ints = set( rs1.keys() ) & set( rs2.keys() )
    peer_set_union = set( rs1.keys() ) | set( rs2.keys() )
    peer_set_cnt = len( peer_set_union )
    r1_cnt = len( rs1.keys() )
    r2_cnt = len( rs2.keys() )
    r['peer_cnt_union'] = peer_set_cnt
    # can go into which peers differ here. always the same?
    if peer_set_cnt != r1_cnt or peer_set_cnt != r2_cnt:
        r['set_diff'] = True
        r['peer_cnt_intersect'] = len( peer_set_ints )
        r['r1_cnt'] = r1_cnt
        r['r2_cnt'] = r1_cnt
    else:
        r['set_diff'] = False
    same_peer_count = 0
    fdiff_counts = {}
    for p in peer_set_ints:
        #r1 = '|'.join( rs1[ p ]['route'] )
        #r2 = '|'.join( rs2[ p ]['route'] )
        r1 = rs1[ p ]['route']
        r2 = rs2[ p ]['route']
        field_diffs = set()
        for idx,val1 in enumerate( r1 ):
            val2 = r2[ idx ]
            if val1 != val2:
                field_str = str( idx )
                if idx in RFIELD2VAL:
                    field_str = RFIELD2VAL[ idx ]
                else:
                    print >>sys.stderr, "unrec field (pos %s, peer: %s), but different!: %s %s %s %s" % ( idx, p, val1, val2, r1, r2 )
                field_diffs.add( field_str )
        fdiff_str = ','.join( sorted( field_diffs ) )
        fdiff_counts.setdefault( fdiff_str, 0 )
        fdiff_counts[ fdiff_str ] += 1
    r['peer_diff'] = fdiff_counts
    #return "field_diff_counts: %s" % fdiff_counts
    return r

if __name__ == "__main__":
    DATE=sys.argv[1]
    adate=arrow.get( DATE )
    DATADIR='./data/%s' % ( adate.format('YYYY.MM.DD.HH') )
    ASN=sys.argv[2]

    p = PfxStore( DATADIR )

    all_pfxset = p.as2pfx[ ASN ]
    all_pfxset_af = {
        4: set(),
        6: set()
    }
    for pfx in all_pfxset:
        if ':' in pfx:
            all_pfxset_af[6].add( pfx )
        else:
            all_pfxset_af[4].add( pfx )
    for af in (4,6):
        global_pfxset = set()
        for pfx in all_pfxset_af[af]:
            # remove low visibility pfxes
            if p.peer_count( pfx ) >= 20:
                global_pfxset.add( pfx )
        groups = find_groups( p, global_pfxset )
        aggr_pfx_set = set()
        ######## next line didn't result in many prefixes removed from the set. either it's a rare case, or there is a bug in the code
        ### aggr_pfx_no_up_diff_routing_set = set() ## removes the prefixes that were already present with different routing policy
        gr_list = list( groups.iteritems() )
        for idx1, gr_tuple1 in enumerate( gr_list ):
            sig1,g1 = gr_tuple1
            for idx2, gr_tuple2 in enumerate( gr_list[ idx1+1: ] ):
                #group_rel = find_group_relation( g1, g2 )
                sig2,g2 = gr_tuple2
                output_txt = compare_groups( g1, g2 )
                print json.dumps([ list(g1['pfx_set']), list(g2['pfx_set']), output_txt])
            ''' 
                #ja['route_set'] = g['route_set'] ## THIS IS BIG
                group_pfx_set = g['pfx_set']
                ja['pfx_cnt'] = len(group_pfx_set)
                if len( group_pfx_set ) > 0: # 0 can happen for filtering out non-globally routed prefixes
                    ja['pfx_avg_len'] = pfxset_avg_len( group_pfx_set )
                    ja['pfx_med_len'] = np.percentile(pfxset_lengths( group_pfx_set ), 50)
                    ## now find if we can aggregate these in a useful manner!
                    merged_pfx_set = try_pfx_merge( p, group_pfx_set )
                    ja['aggr_pfx_cnt'] =  len( merged_pfx_set )
                    if len( merged_pfx_set ) == 0:
                        raise ValueError("merge to 0?!, for %s" % (group_pfx_set) )
                    ja['aggr_pfx_avg_len'] = pfxset_avg_len( merged_pfx_set )
                    ja['aggr_pfx_med_len'] = np.percentile(pfxset_lengths( merged_pfx_set ), 50)
                    aggr_pfx_set |= merged_pfx_set
                ## see if any of the merged_pfx_set was already present for a different routing policy
                ##REMOVED## aggr_pfx_no_up_diff_routing_set |= discard_aggregates_with_diff_routing_policy( groups, sig, merged_pfx_set )
                print json.dumps( ja )
            j['aggr_pfx_v%d_cnt' % af] = len( aggr_pfx_set )
            ##REMOVED j['aggr_no_te_pfx_v%d_cnt' % af] = len( aggr_pfx_no_up_diff_routing_set )
            '''

