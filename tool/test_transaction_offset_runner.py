#!/usr/bin/env python3
"""No-broker regressions for fail-closed raw receipt and resource checks."""
import copy,json,socket,struct,unittest
import run_transaction_offset_integration as runner

MODES=('SSL','SASL_SSL')
SCENARIOS=('commit','abort','attempts','deadline','cancel','close')
def compact(s):
    b=s.encode();assert len(b)<127
    return bytes([len(b)+1])+b

def receipt(correlation,error=88,offset=-1,group='g'):
    request=struct.pack('>hhi',9,7,correlation)+struct.pack('>h',1)+b'c\0'+compact(group)+b'\2'+compact('t')+b'\2'+struct.pack('>i',0)+b'\0\1\0'
    response=struct.pack('>i',correlation)+b'\0'+struct.pack('>i',0)+b'\2'+compact('t')+b'\2'+struct.pack('>iqi',0,offset,-1)+b'\1'+struct.pack('>h',error)+b'\0\0'+struct.pack('>h',0)+b'\0'
    return dict(event='dart-operation-wire-response',version=7,requireStable=True,correlation=correlation,errors=[error],offsets=[offset],requestHex=(struct.pack('>i',len(request))+request).hex(),responseHex=(struct.pack('>i',len(response))+response).hex())

def valid_rows():
    rows=[]
    for mode in MODES:
        for scenario in SCENARIOS:
            group=f'{mode}-{scenario}'
            for stage,error,offset in [('baseline',0,10),('pending-stable-0',88,-1),('pending-stable-1',88,-1),('pending-stable-2',88,-1)]:
                raw=receipt(90,error,offset,group)
                rows.append(dict(event='official-java-helper',receipt=dict(event='fetch',version=7,requireStable=True,correlation=90,group=group,stage=stage,requestHex=raw['requestHex'],responseHex=raw['responseHex'],offsets=[dict(errorCode=error,offset=offset)])))
            rows.extend([dict(event='official-java-helper',receipt=dict(event='awaiting-resolution-command',group=group)),dict(event='dart-transaction-case-start',mode=mode,scenario=scenario,group=group,topic='t'),receipt(1,group=group),receipt(2,group=group)])
            count=3 if scenario=='attempts' else 2
            if count==3:rows.append(receipt(3,group=group))
            if scenario=='close':rows.append(dict(event='intentional-client-close-armed',connectionId=1,pendingResponses=0,requests=6,responses=6))
            rows.append(dict(event='transaction-resolution-command',group=group,operation88Responses=count,resolution='commit' if scenario=='commit' else 'abort'))
            rows.append(dict(event='official-java-helper',receipt=dict(event='transaction-commit-acknowledged' if scenario=='commit' else 'transaction-abort-acknowledged',group=group)))
            if scenario in ('commit','abort'):rows.append(receipt(3,0,20 if scenario=='commit' else 10,group))
            rows.append(dict(event='dart-transaction-case-pass',mode=mode,scenario=scenario,operation88Responses=count,operationFetchRequests=count+(scenario in ('commit','abort'))))
    return rows

def text(rows,reporter='00:52 +12: All tests passed!'):
    return '\n'.join(['TRANSACTION_EVIDENCE '+json.dumps(r) for r in rows]+[reporter])

class TransactionRunnerTest(unittest.TestCase):
    def test_resource_floors(self):
        runner.check_resources(dict(memory_available_kib=1048576,disk_free_bytes=2147483648))
        for sample in [dict(memory_available_kib=1048575,disk_free_bytes=2147483648),dict(memory_available_kib=1048576,disk_free_bytes=2147483647)]:
            with self.assertRaises(RuntimeError):runner.check_resources(sample)
    def test_explicit_cached_command(self):
        cmd=['dart','--disable-dart-dev','--packages=.dart_tool/package_config.json','.dart_tool/pub/bin/test/test.dart-3.12.0.snapshot','test/group_stable_offsets_transaction_integration_test.dart']
        self.assertEqual(runner.explicit_command(['--',*cmd]),cmd)
        for args in ([],['--','dart','test'],['--','dart','test','x.snapshot'],['--',*cmd[:-1]]):
            with self.assertRaises(ValueError):runner.explicit_command(args)
    def test_valid_raw_matrix(self):
        result=runner.verify_events(text(valid_rows()))
        self.assertEqual(len(result['cases']),12)
        self.assertTrue(all(len(r['responseSha256'])==64 for r in result['wire_receipts']))
    def test_missing_or_duplicate_case(self):
        rows=valid_rows()
        for changed in (rows[:-1],rows+[rows[-1]]):
            with self.assertRaises(AssertionError):runner.verify_events(text(changed))
    def test_reporter_and_skip_required(self):
        for reporter in ('','00:52 +11: All tests passed!','00:52 +12 ~1: All tests passed!'):
            with self.assertRaises(AssertionError):runner.verify_events(text(valid_rows(),reporter))
    def test_forged_decoded88_rejected(self):
        rows=valid_rows();r=next(r for r in rows if r['event']=='dart-operation-wire-response')
        r['responseHex']=receipt(1,0,10)['responseHex']
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_correlation_reuse_rejected(self):
        rows=valid_rows();rows[7]=receipt(1,group='SSL-commit')
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_resolution_before_second88_rejected(self):
        rows=valid_rows();rows[7],rows[8]=rows[8],rows[7]
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_wrong_request_group_rejected(self):
        rows=valid_rows();rows[6]=receipt(1,group='other')
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_truncated_frame_rejected(self):
        rows=valid_rows();rows[6]['responseHex']=rows[6]['responseHex'][:-2]
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_wrong_stable_flag_rejected(self):
        rows=valid_rows();rows[6]['requireStable']=False
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_ansi_success_reporter(self):
        result=runner.verify_events(text(valid_rows(),'\x1b[32m00:52 \x1b[0m\x1b[1m+12\x1b[0m: All tests passed!'))
        self.assertEqual(len(result['cases']),12)
    def test_ansi_skipped_reporter_rejected(self):
        reporter='\x1b[32m00:52 +12 \x1b[33m~1\x1b[0m: All tests passed!'
        with self.assertRaises(AssertionError):runner.verify_events(text(valid_rows(),reporter))
    def test_duplicate_group_rejected(self):
        rows=valid_rows();starts=[r for r in rows if r['event']=='dart-transaction-case-start'];starts[1]['group']=starts[0]['group']
        with self.assertRaisesRegex(AssertionError,'distinct group'):runner.verify_events(text(rows))
    def test_wrong_resolution_rejected(self):
        for scenario in ('commit','abort'):
            rows=valid_rows();commands=[r for r in rows if r['event']=='transaction-resolution-command']
            commands[0 if scenario=='commit' else 1]['resolution']='abort' if scenario=='commit' else 'commit'
            with self.assertRaisesRegex(AssertionError,'Wrong resolution'):runner.verify_events(text(rows))
    def close_event_rows(self):
        rows=valid_rows()
        start=next(i for i,r in enumerate(rows) if r['event']=='dart-transaction-case-start' and r['mode']=='SSL' and r['scenario']=='close')
        event=dict(event='expected-client-close-transport-event',connectionId=1,leg='downstream',callback='downstream-stream-onError',errorType='SocketException',errno=32,second88Flushed=True,intentionalClientClose=True,pendingResponses=0,requestAfterCloseArm=False,requests=6,responses=6,expectedCloseEvents=1)
        rows[start+4:start+4]=[event]
        return rows,event
    def test_expected_close_event_retained(self):
        rows,_=self.close_event_rows()
        result=runner.verify_events(text(rows))
        self.assertEqual(len(result['relay_lifecycle_events']),3)
    def test_unqualified_close_event_rejected(self):
        for key,value in [('leg','upstream'),('errno',104),('callback','upstream-stream-onError'),('errorType','HandshakeException'),('second88Flushed',False),('intentionalClientClose',False),('pendingResponses',1),('requestAfterCloseArm',True),('connectionId',2)]:
            rows,event=self.close_event_rows();event[key]=value
            with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_missing_close_arm_rejected(self):
        rows,_=self.close_event_rows();rows=[r for r in rows if r['event']!='intentional-client-close-armed']
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_counts_changed_after_close_arm_rejected(self):
        rows,event=self.close_event_rows();event['requests']=8;event['responses']=8
        with self.assertRaisesRegex(AssertionError,'Traffic after'):runner.verify_events(text(rows))
    def test_close_case_without_arm_rejected_even_without_epipe(self):
        rows=[r for r in valid_rows() if r['event']!='intentional-client-close-armed']
        with self.assertRaisesRegex(AssertionError,'requires explicit arm'):runner.verify_events(text(rows))
    def test_unexpected_relay_event_never_passes(self):
        rows,event=self.close_event_rows();event['event']='unexpected-relay-transport-event'
        with self.assertRaises(AssertionError):runner.verify_events(text(rows))
    def test_port_reuse_probe_fails_occupied_port(self):
        env={k:'0' for k in ('KAFKA_SECURE_TLS_PORT','KAFKA_SECURE_CONTROLLER_PORT','KAFKA_SECURE_SASL_PORT','KAFKA_SECURE_ADMIN_PORT','KAFKA_SECURE_SCRAM_PORT')}
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1',0));occupied.listen()
            env['KAFKA_SECURE_TLS_PORT']=str(occupied.getsockname()[1])
            self.assertFalse(all(runner.reusable_ports(env).values()))

if __name__=='__main__':unittest.main()
