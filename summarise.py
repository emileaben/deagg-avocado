#!/usr/bin/env python
import json
import sys
import heapq
'''
{"group_v6_cnt": 0, "aggr_pfx_v6_cnt": 0, "group_v4_cnt": 1, "all_pfx_v4_cnt": 1, "global_pfx_v4_cnt": 1, "global_pfx_v6_cnt": 0, "aggr_pfx_v4_cnt": 1, "all_pfx_cnt": 1, "asn": "11874", "all_pfx_v6_cnt": 0}
{"group_v6_cnt": 0, "aggr_pfx_v6_cnt": 0, "group_v4_cnt": 0, "all_pfx_v4_cnt": 50, "global_pfx_v4_cnt": 0, "global_pfx_v6_cnt": 0, "aggr_pfx_v4_cnt": 0, "all_pfx_cnt": 50, "asn": "38341", "all_pfx_v6_cnt": 0}
{"group_v6_cnt": 0, "aggr_pfx_v6_cnt": 0, "group_v4_cnt": 2, "all_pfx_v4_cnt": 2, "global_pfx_v4_cnt": 2, "global_pfx_v6_cnt": 0, "aggr_pfx_v4_cnt": 2, "all_pfx_cnt": 2, "asn": "38340", "all_pfx_v6_cnt": 0}
{"group_v6_cnt": 1, "aggr_pfx_v6_cnt": 1, "group_v4_cnt": 0, "all_pfx_v4_cnt": 0, "global_pfx_v4_cnt": 0, "global_pfx_v6_cnt": 1, "aggr_pfx_v4_cnt": 0, "all_pfx_cnt": 1, "asn": "{45271}", "all_pfx_v6_cnt": 1}
{"aggr_pfx_med_len": 24.0, "pfx_cnt": 1, "aggr_pfx_cnt": 1, "aggr_pfx_avg_len": 24.0, "atom_id": "11544-4-4f4ed9aaa8c96e900f81db41f57fc447", "pfx_med_len": 24.0, "pfx_avg_len": 24.0}
'''

out = {}
for af in (4,6):
    out[af] = {
        'global_pfx_cnt': 0,
        'aggr_pfx_cnt': 0,
        'aggr_no_te_pfx_cnt': 0
    }

asn_cnt = 0

h_avg_shift = []
#h_len = 0

with open( sys.argv[1] ) as inf:
    for line in inf:
        d = json.loads( line )
        if not 'asn' in d:
            ## it's a group
            heapq.heappush(h_avg_shift, ( int(1000*(d['aggr_pfx_avg_len'] - d['pfx_avg_len'])), d ) )
            #h_len += 1
            #if h_len > 100: # cull list
            #    h_avg_shift = h_avg_shift[0:10]
            #    h_len = 0
        else:
            asn_cnt += 1
            for af in (4,6):
                out[af]['global_pfx_cnt'] += d['global_pfx_v%d_cnt' % af ]
                out[af]['aggr_pfx_cnt'] += d['aggr_pfx_v%d_cnt' % af ]
                #out[af]['aggr_no_te_pfx_cnt'] += d['aggr_no_te_pfx_v%d_cnt' % af ]

print "ASNs analysed: %d" % asn_cnt
for af in (4,6):
    print "IPv%d 'global' prefixes: %d" % (af, out[af]['global_pfx_cnt'])
    print "IPv%d 'global max aggregated' prefixes: %d" % (af, out[af]['aggr_pfx_cnt'])
    #print "IPv%d 'global max aggregated' (after TE-removal) prefixes: %d" % (af, out[af]['aggr_no_te_pfx_cnt'])
    print "IPv%d max_aggregate/global %.1f%%" % (af, 100.0* out[af]['aggr_pfx_cnt'] / out[af]['global_pfx_cnt'])
    #print "IPv%d max_aggregate_no_te/global %.1f%%" % (af, 100.0* out[af]['aggr_no_te_pfx_cnt'] / out[af]['global_pfx_cnt'])

### biggest shifts in avg_len
print "largest shifts in avg pfx size for atoms"
for h in heapq.nsmallest(25, h_avg_shift):
    print "shift:%s data: %s" % (-h[0]/1000.0, h[1])


