#!/usr/bin/env python
import json
import sys
'''
{"group_v6_cnt": 0, "aggr_pfx_v6_cnt": 0, "group_v4_cnt": 1, "all_pfx_v4_cnt": 1, "global_pfx_v4_cnt": 1, "global_pfx_v6_cnt": 0, "aggr_pfx_v4_cnt": 1, "all_pfx_cnt": 1, "asn": "11874", "all_pfx_v6_cnt": 0}
{"group_v6_cnt": 0, "aggr_pfx_v6_cnt": 0, "group_v4_cnt": 0, "all_pfx_v4_cnt": 50, "global_pfx_v4_cnt": 0, "global_pfx_v6_cnt": 0, "aggr_pfx_v4_cnt": 0, "all_pfx_cnt": 50, "asn": "38341", "all_pfx_v6_cnt": 0}
{"group_v6_cnt": 0, "aggr_pfx_v6_cnt": 0, "group_v4_cnt": 2, "all_pfx_v4_cnt": 2, "global_pfx_v4_cnt": 2, "global_pfx_v6_cnt": 0, "aggr_pfx_v4_cnt": 2, "all_pfx_cnt": 2, "asn": "38340", "all_pfx_v6_cnt": 0}
{"group_v6_cnt": 1, "aggr_pfx_v6_cnt": 1, "group_v4_cnt": 0, "all_pfx_v4_cnt": 0, "global_pfx_v4_cnt": 0, "global_pfx_v6_cnt": 1, "aggr_pfx_v4_cnt": 0, "all_pfx_cnt": 1, "asn": "{45271}", "all_pfx_v6_cnt": 1}
'''

out = {}
for af in (4,6):
    out[af] = {
        'global_pfx_cnt': 0,
        'aggr_pfx_cnt': 0
    }

line_cnt = 0

with open( sys.argv[1] ) as inf:
    for line in inf:
        line_cnt += 1
        d = json.loads( line )
        for af in (4,6):
            out[af]['global_pfx_cnt'] += d['global_pfx_v%d_cnt' % af ]
            out[af]['aggr_pfx_cnt'] += d['aggr_pfx_v%d_cnt' % af ]

print "ASNs analysed: %d" % line_cnt
for af in (4,6):
    print "IPv%d 'global' prefixes: %d" % (af, out[af]['global_pfx_cnt'])
    print "IPv%d 'global max aggregated' prefixes: %d" % (af, out[af]['aggr_pfx_cnt'])
    print "IPv%d max_aggregate/global %.1f%%" % (af, 100.0* out[af]['aggr_pfx_cnt'] / out[af]['global_pfx_cnt'])

