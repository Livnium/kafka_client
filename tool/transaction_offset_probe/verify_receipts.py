#!/usr/bin/env python3
"""Validate retained official-codec receipts; never promote not88 into success."""
import hashlib,json,pathlib,sys,struct
p=pathlib.Path(sys.argv[1]); rows=[]
for line in p.read_text().splitlines():
    try: rows.append(json.loads(line))
    except json.JSONDecodeError: pass
assert rows[-1]['event']=='all-cases-pass'
starts=[r for r in rows if r['event']=='case-start']
assert len(starts)==rows[-1]['cases']
cases=[]
for case in starts:
    group=case['group']; events=[r for r in rows if r.get('group')==group]
    assert any(r['event']=='transaction-offsets-acknowledged-open' for r in events)
    assert any(r['event']=='case-pass' for r in events)
    fetches=[r for r in events if r['event']=='fetch']
    for r in fetches:
        for direction in ('request','response'):
            frame=bytes.fromhex(r[direction+'Hex'])
            assert len(frame)==r[direction+'Bytes']
            assert struct.unpack('>i',frame[:4])[0]==len(frame)-4
            assert hashlib.sha256(frame).hexdigest()==r[direction+'Sha256']
        assert r['groupError']=='NONE' and len(r['offsets'])==1
        assert not r['autoRetry'] and r['requestCount']==1
    baseline=[r for r in fetches if r['stage'] in ('baseline','open-v5-control','open-v7-control')]
    assert len(baseline)==3
    assert all(r['offsets'][0]['errorCode']==0 and r['offsets'][0]['offset']==10 for r in baseline)
    pending=[r for r in fetches if r['stage'].startswith('pending-stable-')]
    assert len(pending)==3
    assert all(r['version']==7 and r['requireStable'] and r['offsets'][0]['errorCode']==88 and r['offsets'][0]['offset']==-1 for r in pending)
    after=[r for r in fetches if r['stage'].startswith('post-resolution-read-retry-')]
    expected=20 if case['resolution']=='commit' else 10
    assert after[-1]['offsets'][0]['errorCode']==0 and after[-1]['offsets'][0]['offset']==expected
    assert all(r['offsets'][0]['errorCode']==88 for r in after[:-1])
    cases.append(dict(group=group,security_protocol=case['securityProtocol'],resolution=case['resolution'],pending88_receipts=len(pending),post_resolution88_receipts=len(after)-1,settled_offset=expected))
print(json.dumps(dict(outcome='genuine-pending-transaction88-observed',cases=cases,raw_receipt_file=str(p),raw_receipt_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),raw_frame_hashes_verified=True,scope='Synthetic offset metadata only; not application no-gap or exactly-once evidence'),indent=2))
