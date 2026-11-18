"""Check codes emitted by the real native KJ test against the public error table."""
from pathlib import Path
import argparse,hashlib,json,re
parser=argparse.ArgumentParser()
parser.add_argument('--native-source',type=Path,required=True)
parser.add_argument('--receipt',type=Path,required=True)
parser.add_argument('--log',type=Path,required=True)
parser.add_argument('--errors',type=Path,required=True)
args=parser.parse_args();receipt=json.loads(args.receipt.read_text())
assert receipt['passed'] and not receipt['mutation']
for relative,digest in receipt['sources'].items():
 assert hashlib.sha256((args.native_source/relative).read_bytes()).hexdigest()==digest,relative
table={}
for line in args.errors.read_text().split('## 2.',1)[0].splitlines():
 if not line.startswith('| '):continue
 columns=[part.strip() for part in line.strip('|').split('|')]
 if len(columns)<5 or not columns[0].isdigit():continue
 for code in re.findall(r'`([A-Z][A-Z_]+)`',columns[1]):table[code]={'status':int(columns[0]),'executed':columns[3]}
log=args.log.read_text();codes=set(re.findall(r'^NATIVE_ERROR_CODE ([A-Z_]+)$',log,re.M))
statuses={code:int(status) for status,code in re.findall(r'^NATIVE_ERROR_STATUS ([0-9]+) ([A-Z_]+)$',log,re.M)}
assert codes=={'CPU_LIMIT_EXCEEDED','MEMORY_LIMIT_EXCEEDED','PROCESS_MEMORY_PRESSURE'}
assert set(statuses)=={'NOT_SUPPORTED','CPU_LIMIT_EXCEEDED','MEMORY_LIMIT_EXCEEDED','OVERLOADED','PROCESS_DRAINING'}
# PROCESS_DRAINING is a reserved, unused private enum value, not a public code.
production=(args.native_source/'src/workerd/threaded/threaded-server.c++').read_text()
assert 'Refusal::DRAINING' not in production
statuses.pop('PROCESS_DRAINING')
for code,status in statuses.items():assert table[code]['status']==status,(code,status,table.get(code))
for code in codes:assert table[code]['status']==503 and table[code]['executed'] in ('Partly executed','Result uncertain')
assert table['NOT_SUPPORTED']['executed']=='Partly executed'
assert table['OVERLOADED']['executed']=='Not executed'
print(json.dumps({'passed':True,'codes':sorted(codes|statuses.keys()),'tableSha256':hashlib.sha256(args.errors.read_bytes()).hexdigest(),
 'scope':'Native emitted codes and refusal statuses; B47 reports the existing Node LOAD_FAILED table discrepancy separately.'}))
