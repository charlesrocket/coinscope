def test_crossval():
    testset_stats, floodset_stats, inferred = process_experiment(manifest, logs)

    sid = logs[0].source_id

    global versions
    versions = good_versions(sid, manifest)
    gv = set([k for k in versions if versions[k][0]])
    print len(gv), 'nodes have good starting heights and version string'

    # if floodset_stats[g]['hasflood'] and
    gd = [g for g in floodset_stats if floodset_stats[g]['getdataflood'] and floodset_stats[g]['getdatamarker']]
    print len(gd), 'nodes responded to getdata and have flood'

    global good
    good = gv.intersection(gd)
    good = gd
    print len(good), 'nodes are "good"'

    dmap = dict((int(hid),ip) for hid,ip in manifest['nodes']) # Map handles to ip
    goodips = set([dmap[k][0] for k in good])
    global ipmap
    ipmap = defaultdict(set)
    for k,v in dmap.iteritems():
        ipmap[v[0]].add(k)

    import groundtruth
    peers = groundtruth.gtpeers(manifest['time'])
    for k in peers:
        try:
            handle = [h for h in dmap if dmap[h][0] == k and not testset_stats[h]['haswrongparent']][0] # FIXME: this only picks the first handle! but there are often dupes
        except IndexError:
            try:
                handle = [h for h in dmap if dmap[h][0] == k][0]
            except IndexError:
                continue
        except KeyError:
            continue
        print 'processing peer', k, handle
        stable,transient = peers[k]
        print 'stable', len(stable), 'transient', len(transient)
        sgood = set([s for s in stable if s in goodips])
        tgood = set([t for t in transient if t in goodips])
        print 'stable good', len(sgood), 'transient good', len(tgood)

        inferredgood = [dmap[h][0] for h in inferred[handle] if dmap[h][0] in goodips]
        print len(inferredgood), 'edges inferred'

        TP = set(inferredgood).intersection(tgood)
        FP = set(inferredgood).difference(tgood)
        FN = set(sgood).difference(inferredgood)
        print 'TP:', len(TP), 'FP:', len(FP), 'FN:', len(FN)
        print 'FP', FP
        print 'FN', FN
