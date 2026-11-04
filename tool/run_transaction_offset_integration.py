#!/usr/bin/env python3
"""Guarded owned Kafka TLS/SASL real-transaction retry evidence runner."""
import hashlib,json,os,pathlib,shutil,signal,socket,struct,re,subprocess,sys,time
ROOT=pathlib.Path(__file__).resolve().parents[1]
MIN_MEMORY_KIB=1024*1024
MIN_DISK_BYTES=2*1024**3
MAX_DART_RSS_KIB=768*1024

def explicit_command(arguments):
    if not arguments or arguments[0]!='--': raise ValueError('Supply -- followed by explicit cached Dart test snapshot command')
    command=arguments[1:]
    snapshots=[i for i,a in enumerate(command) if a.endswith('.snapshot')]
    if len(snapshots)!=1 or '--disable-dart-dev' not in command or not any(a.startswith('--packages=') for a in command):
        raise ValueError('Only explicit --disable-dart-dev cached snapshot execution is supported; no resolver invocation')
    index=snapshots[0]
    if index<2 or not any(a.endswith('group_stable_offsets_transaction_integration_test.dart') for a in command[index+1:]):
        raise ValueError('Explicit transaction integration test target required')
    return command

class WireReader:
    def __init__(self,data): self.data=data;self.at=0
    def read(self,n):
        value=self.data[self.at:self.at+n];self.at+=n
        assert len(value)==n,'Truncated receipt'
        return value
    def number(self,fmt): return struct.unpack(fmt,self.read(struct.calcsize(fmt)))[0]
    def var(self):
        n=0
        for i in range(5):
            b=self.number('>B');n|=(b&127)<<(7*i)
            if b<128: return n
        raise AssertionError('Invalid compact count')
    def compact(self):
        n=self.var();return None if n==0 else self.read(n-1).decode()
    def tags(self):
        for _ in range(self.var()): self.var();self.read(self.var())
    def end(self): assert self.at==len(self.data),'Trailing wire bytes'

def decode_wire(row):
    frames={k:bytes.fromhex(row[k+'Hex']) for k in ('request','response')}
    for frame in frames.values(): assert int.from_bytes(frame[:4],'big')==len(frame)-4
    r=WireReader(frames['request'][4:])
    assert r.number('>h')==9 and r.number('>h')==7
    correlation=r.number('>i');size=r.number('>h')
    if size>=0:r.read(size)
    r.tags();group=r.compact();assert r.var()==2;topic=r.compact();assert r.var()==2
    assert r.number('>i')==0;r.tags();assert r.number('>B')==1;r.tags();r.end()
    r=WireReader(frames['response'][4:]);assert r.number('>i')==correlation
    r.tags();r.number('>i');assert r.var()==2 and r.compact()==topic and r.var()==2
    assert r.number('>i')==0
    offset=r.number('>q');r.number('>i');r.compact();error=r.number('>h');r.tags();r.tags()
    assert r.number('>h')==0;r.tags();r.end()
    assert row['correlation']==correlation and row['errors']==[error] and row['offsets']==[offset]
    assert row['version']==7 and row['requireStable'] is True
    return group,error,offset,topic

def owned_processes(pgid):
    processes=[]
    for entry in pathlib.Path('/proc').glob('[0-9]*'):
        try:
            pid=int(entry.name)
            if os.getpgid(pid)!=pgid: continue
            status=entry.joinpath('status').read_text()
            if re.search(r'^State:\s+Z',status,re.M):continue
            exe=entry.joinpath('cmdline').read_bytes().split(b'\0')[0].decode(errors='replace')
            name=pathlib.Path(exe).name
            rss=re.search(r'^VmRSS:\s+(\d+)',status,re.M)
            processes.append(dict(pid=pid,kind='dart' if name in ('dart','dartaotruntime') else 'java' if name=='java' else 'other',rss_kib=int(rss.group(1)) if rss else 0))
        except (ProcessLookupError,FileNotFoundError): pass
    return processes

def reusable_ports(environment):
    keys={'KAFKA_SECURE_TLS_PORT':19192,'KAFKA_SECURE_CONTROLLER_PORT':19193,'KAFKA_SECURE_SASL_PORT':19194,'KAFKA_SECURE_ADMIN_PORT':19195,'KAFKA_SECURE_SCRAM_PORT':19196}
    results={}
    for key,default in keys.items():
        port=int(environment.get(key,default))
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            try:probe.bind(('127.0.0.1',port));results[str(port)]=True
            except OSError:results[str(port)]=False
    return results

def resources(path):
    values={line.split(':')[0]:int(line.split()[1]) for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')}
    return dict(memory_available_kib=values['MemAvailable'],disk_free_bytes=shutil.disk_usage(path).free)

def check_resources(sample):
    if sample['memory_available_kib']<MIN_MEMORY_KIB: raise RuntimeError('Available memory below 1 GiB floor')
    if sample['disk_free_bytes']<MIN_DISK_BYTES: raise RuntimeError('Free disk below 2 GiB floor')

def verify_events(text):
    text=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', text)
    assert re.search(r'^\d+:\d+ \+12: All tests passed!$',text,re.M), 'Expected reporter 12passed/0skipped'
    assert not re.search(r'^\d+:\d+ .*~\d+',text,re.M), 'Skipped tests cannot pass'
    rows=[]
    for line in text.splitlines():
        if 'TRANSACTION_EVIDENCE ' not in line: continue
        rows.append(json.loads(line.split('TRANSACTION_EVIDENCE ',1)[1]))
    cases=[r for r in rows if r['event']=='dart-transaction-case-pass']
    expected={(mode,scenario) for mode in ('SSL','SASL_SSL') for scenario in ('commit','abort','attempts','deadline','cancel','close')}
    assert {(r['mode'],r['scenario']) for r in cases}==expected, 'Incomplete real transaction matrix'
    assert len(cases)==len(expected), 'Duplicated case result'
    wire=[r for r in rows if r['event']=='dart-operation-wire-response']
    assert wire, 'No raw operation receipts'
    active=None
    helper_open=set()
    case_groups=set()
    helper_baselines={}
    helper_pending={}
    for row in rows:
        if row['event']=='official-java-helper':
            helper=row['receipt']
            if helper['event']=='fetch' and helper['version']==7 and helper['requireStable']:
                receipt=dict(version=7,requireStable=True,correlation=helper['correlation'],requestHex=helper['requestHex'],responseHex=helper['responseHex'],errors=[p['errorCode'] for p in helper['offsets']],offsets=[p['offset'] for p in helper['offsets']])
                group,error,offset,topic=decode_wire(receipt)
                assert group==helper['group']
                if helper['stage']=='baseline':
                    assert error==0 and offset==10
                    helper_baselines[group]=topic
                    helper_pending[group]=0
                if helper['stage'].startswith('pending-stable-'):
                    assert error==88 and offset==-1
                    helper_pending[group]=helper_pending.get(group,0)+1
            if helper['event']=='awaiting-resolution-command':
                assert helper['group'] in helper_baselines and helper_pending[helper['group']]==3
                helper_open.add(helper['group'])
            if helper['event'] in ('transaction-commit-acknowledged','transaction-abort-acknowledged'):
                assert active is not None and active['released'] and helper['group']==active['group'], 'Resolution before explicit second88 gate'
                assert helper['event']==('transaction-commit-acknowledged' if active['scenario']=='commit' else 'transaction-abort-acknowledged'), 'Wrong transaction resolution acknowledgment'
        if row['event']=='dart-transaction-case-start':
            assert active is None and row['group'] in helper_open and helper_baselines[row['group']]==row['topic']
            assert row['group'] not in case_groups, 'Each case must have a distinct group ID'
            case_groups.add(row['group'])
            active=dict(mode=row['mode'],scenario=row['scenario'],group=row['group'],topic=row['topic'],wire=[],released=False,closeArm=None,closeArmCounts=None,expectedCloseEvents=0)
        elif row['event']=='dart-operation-wire-response':
            assert active is not None
            decoded=decode_wire(row)
            assert decoded[0]==active['group'] and decoded[3]==active['topic'], 'Wrong operation group/topic'
            active['wire'].append(row)
        elif row['event']=='intentional-client-close-armed':
            assert active is not None and active['scenario']=='close' and active['closeArm'] is None
            assert not active['released'] and sum(88 in r['errors'] for r in active['wire'])>=2
            assert row['pendingResponses']==0 and row['requests']==row['responses']>=2
            active['closeArm']=row['connectionId']
            active['closeArmCounts']=(row['requests'],row['responses'])
        elif row['event']=='expected-client-close-transport-event':
            assert active is not None and active['scenario']=='close' and active['closeArm']==row['connectionId']
            assert active['expectedCloseEvents']==0 and row['expectedCloseEvents']==1
            assert row['leg']=='downstream' and row['callback']=='downstream-stream-onError'
            assert row['errorType']=='SocketException' and row['errno']==32
            assert row['second88Flushed'] is True and row['intentionalClientClose'] is True
            assert row['pendingResponses']==0 and row['requestAfterCloseArm'] is False
            assert row['requests']==row['responses']>=2
            assert (row['requests'],row['responses'])==active['closeArmCounts'], 'Traffic after client-close arm'
            active['expectedCloseEvents']+=1
        elif row['event']=='unexpected-relay-transport-event':
            raise AssertionError('Unexpected relay transport error cannot pass')
        elif row['event']=='transaction-resolution-command':
            assert active is not None and not active['released'] and row['group']==active['group']
            assert row['resolution']==('commit' if active['scenario']=='commit' else 'abort'), 'Wrong resolution command'
            count=sum(88 in r['errors'] for r in active['wire'])
            assert count==row['operation88Responses']>=2, 'Release before second88'
            assert len({r['correlation'] for r in active['wire']})==len(active['wire'])
            assert all(r['errors']==[88] for r in active['wire']), 'Success before transaction release'
            active['released']=True
        elif row['event']=='dart-transaction-case-pass':
            assert active is not None and active['released'] and (active['mode'],active['scenario'])==(row['mode'],row['scenario'])
            if active['scenario']=='close':assert active['closeArm'] is not None, 'Close scenario requires explicit arm'
            observed=active['wire']
            assert len(observed)==row['operationFetchRequests']
            errors88=[r for r in observed if 88 in r['errors']]
            assert len(errors88)==row['operation88Responses']>=2
            assert len({r['correlation'] for r in errors88})==len(errors88), 'Retry correlations must be distinct'
            if active['scenario'] in ('commit','abort'):
                assert observed[0]['errors']==observed[1]['errors']==[88]
                assert observed[-1]['errors']==[0] and observed[-1]['offsets']==[20 if active['scenario']=='commit' else 10]
            else:
                assert len(errors88)==len(observed)
                if active['scenario']=='attempts':assert len(observed)==3
            active=None
    assert active is None, 'Unfinished case cannot pass'
    assert len(case_groups)==12, 'Expected twelve distinct group IDs'
    for r in wire:
        assert r['version']==7 and r['requireStable'], 'Stable v7 request required'
        for direction in ('request','response'):
            frame=bytes.fromhex(r[direction+'Hex'])
            assert int.from_bytes(frame[:4],'big')==len(frame)-4
            r[direction+'Sha256']=hashlib.sha256(frame).hexdigest()
    assert all(r['operation88Responses']>=2 for r in cases), 'No88 result cannot be promoted to success'
    return dict(cases=cases,wire_receipts=wire,relay_lifecycle_events=[r for r in rows if r['event'] in ('intentional-client-close-armed','expected-client-close-transport-event')],official_helper_events=[r for r in rows if r['event']=='official-java-helper'])

def main():
    command=explicit_command(sys.argv[1:])
    os.umask(0o077)
    run_root=pathlib.Path(os.environ.get('KAFKA_TRANSACTION_RUN_ROOT',str(ROOT/'.dart_tool/transaction-offset-integration'))).resolve()
    run_root.mkdir(parents=True,exist_ok=True)
    run=run_root/f'run-{time.time_ns()}'
    run.mkdir()
    output=run/'sanitized-runner-output.txt'
    result=dict(exit_code=1,resource_floor_breach=None,peak_owned_dart_rss_kib=0,resource_samples=[],private_fixture_removed=False,scope='Synthetic offset metadata; not no-gap, failover, security certification, or exactly-once evidence',routing='Owned fixture TLS termination on both verified hops; only FindCoordinator v2 endpoint is rewritten')
    proc=None
    saved_handlers={s:signal.getsignal(s) for s in (signal.SIGINT,signal.SIGTERM)}
    def interrupted(signum,_frame):raise KeyboardInterrupt(f'Signal {signum}')
    for sig in saved_handlers:signal.signal(sig,interrupted)
    try:
        first=resources(run);check_resources(first);result['resources_before']=first
        env=dict(os.environ,KAFKA_TRANSACTION_FIXTURE='1',KAFKA_SECURE_RUN_ROOT=str(run))
        result['explicit_test_command']=command
        command=['bash',str(ROOT/'tool/run_secure_broker_integration.sh'),*command]
        with output.open('w') as log:
            proc=subprocess.Popen(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            deadline=time.monotonic()+600
            while proc.poll() is None:
                sample=resources(run)
                sample['monotonic_seconds']=time.monotonic()
                sample['owned_processes']=owned_processes(proc.pid)
                sample['owned_dart_rss_kib']=sum(p['rss_kib'] for p in sample['owned_processes'] if p['kind']=='dart')
                result['resource_samples'].append(sample)
                result['peak_owned_dart_rss_kib']=max(result['peak_owned_dart_rss_kib'],sample['owned_dart_rss_kib'])
                if sample['owned_dart_rss_kib']>MAX_DART_RSS_KIB: raise RuntimeError('Owned Dart RSS exceeded 768 MiB cap')
                try: check_resources(sample)
                except RuntimeError as error:
                    result['resource_floor_breach']=str(error);raise
                if time.monotonic()>deadline: raise TimeoutError('Owned fixture exceeded ten minute safety limit')
                time.sleep(.5)
            result['test_exit_code']=proc.returncode
        text=output.read_text()
        # Broker runner scrubs exact ephemeral values before returning this output.
        if '-----BEGIN PRIVATE KEY-----' in text: raise RuntimeError('Secret scan failed')
        if proc.returncode!=0: raise RuntimeError('Owned fixture failed; retain sanitized failed receipts')
        parsed=verify_events(text)
        (run/'verified-receipts.json').write_text(json.dumps(parsed,indent=2)+'\n')
        result['exit_code']=0
    except BaseException as error:
        result['failure']=f'{type(error).__name__}: {error}'
    finally:
        for sig in saved_handlers:signal.signal(sig,signal.SIG_IGN)
        if proc is not None and proc.poll() is None:
            # Only the process group we created; shell cleanup owns ephemeral keys.
            os.killpg(proc.pid,signal.SIGTERM)
            try: proc.wait(timeout=30)
            except subprocess.TimeoutExpired: os.killpg(proc.pid,signal.SIGKILL);proc.wait()
        result['owned_runner_stopped']=proc is None or proc.poll() is not None
        remaining=owned_processes(proc.pid) if proc is not None else []
        if remaining:
            result['cleanup_recovered_orphan_pids']=[p['pid'] for p in remaining]
            os.killpg(proc.pid,signal.SIGKILL)
            for _ in range(30):
                if not owned_processes(proc.pid):break
                time.sleep(.1)
            result['exit_code']=1
        result['owned_processes_remaining']=owned_processes(proc.pid) if proc is not None else []
        if result['owned_processes_remaining']:result['exit_code']=1
        # Only this invocation's fresh owned directory, after owned process stop.
        for private in run.glob('private.*'):
            if private.is_dir(): shutil.rmtree(private)
        result['private_fixture_removed']=not list(run.glob('private.*'))
        if not result['private_fixture_removed']: result['exit_code']=1
        result['port_reuse']=reusable_ports(os.environ)
        if not all(result['port_reuse'].values()): result['exit_code']=1
        result['resources_after']=resources(run)
        result['sanitized_output_sha256']=hashlib.sha256(output.read_bytes()).hexdigest() if output.exists() else None
        (run/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,indent=2));print(f'Retained evidence: {run}')
        for sig,handler in saved_handlers.items():signal.signal(sig,handler)
    return result['exit_code']

if __name__=='__main__': raise SystemExit(main())
