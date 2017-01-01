import json
from collections import defaultdict
import radix

asnData = []
atomData = defaultdict(list) 

for line in open("./results/out.analyse.2016-11-16.txt","r"):
    x = json.loads(line)
    if "asn" in x:
        asnData.append(x)
    else:
        id = x["atom_id"].split("-") # asn-af-hash
        x["af"] = id[1]
        atomData[id[0]].append(x)


te = []
security = []
naap = []
other = []
noDeagg = 0
nbTotal = len(asnData) 

for asn in asnData:
    if asn["global_pfx_v4_cnt"] == asn["aggr_pfx_v4_cnt"]:
        # Number of prefixes unchanged after aggregation
        noDeagg += 1
    elif asn["group_v4_cnt"]>1:
        # put all prefixes in a radix 
        rtree = radix.Radix()
        for atom in atomData[asn["asn"]]:
            for pfx in atom["pfx_list"]:
                rnode = rtree.add(pfx)
                rnode.data["atom_id"] = atom["atom_id"]
        
        nbCoveredDiff = 0
        for rnode in rtree:
            covering = rtree.search_covering(rnode.prefix)
            sameAtom = True
            for n in covering:
                if n.data["atom_id"] != rnode.data["atom_id"]:
                    sameAtom = False

            if not sameAtom:
                nbCoveredDiff += 1
        
        if nbCoveredDiff / float(asn["global_pfx_v4_cnt"]) > 0.5:
            # Contains more than 50% of prefixes covered by prefixes with
            # different paths
            te.append(asn)
        elif asn["global_pfx_v4_cnt"] > 0 and asn["aggr_pfx_v4_cnt"]/float(asn["global_pfx_v4_cnt"]) > 0.5:
            # Contains more than 50% of non-aggregatable prefixes
            naap.append(asn)
        else:
            #naap.append(asn)
            other.append(asn)

            # print asn
            # print atomData[asn["asn"]]

    elif asn["group_v4_cnt"] == 1:
        all24 = True
        onlyv6 = True
        for atom in atomData[asn["asn"]]:
            if atom["af"] == "4" :
                onlyv6 = False
                if atom["pfx_med_len"] != 24:
                    all24 = False

        if all24 and not onlyv6:
            security.append(asn)
        else:
            # Could be aggregated but not /24s
            other.append(asn)
    else:
        other.append(asn)

nbTE = len(te)
nbNAAP = len(naap)
nbSecurity = len(security)
nbOther = len(other)

# Print results
nbDeagg = float(nbTotal-noDeagg)
print " %s ASN in total" % (nbTotal)
print "\t%.2f%% (%s ASN) not deaggregating" % (100.0*noDeagg/nbTotal, noDeagg)
print "\t%.2f%% (%s ASN) deaggregating" % (100.0*nbDeagg/nbTotal, int(nbDeagg))
print "\t\t%.2f%% (%s ASN) TE" % (100.0*nbTE/nbDeagg, nbTE)
print "\t\t%.2f%% (%s ASN) Not aggregatable" % (100.0*nbNAAP/nbDeagg, nbNAAP)
print "\t\t%.2f%% (%s ASN) Security" % (100.0*nbSecurity/nbDeagg, nbSecurity)
print "\t\t%.2f%% (%s ASN) Other" % (100.0*nbOther/nbDeagg, nbOther)


