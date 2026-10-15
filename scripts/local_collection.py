"""Isolated local collection harness. Never migrates an external database."""
import argparse,hashlib,json,os,re,socket,subprocess,time,urllib.error,urllib.request
from pathlib import Path
from wasm_artifact import remote_source
ROOT=Path(__file__).resolve().parents[1]
BASE=['docker','compose','-f',str(ROOT/'deploy/local/compose.yaml')]
def run(*args,input=None,capture_output=False):
 return subprocess.run([*BASE,*args],input=input,text=True,check=True,cwd=ROOT,capture_output=capture_output)
def render_gateway():
 cfg=json.loads((ROOT/'deploy/local/envoy.json').read_text())
 remote=remote_source(ROOT/'dist/gateway-observer-v1.wasm','http://observer:9093/wasm/gateway-observer-v1.wasm','observer-wasm')
 filters=cfg['static_resources']['listeners'][0]['filter_chains'][0]['filters'][0]['typed_config']['http_filters']
 wasm=next(f['typed_config']['config'] for f in filters if f['name']=='envoy.filters.http.wasm')
 wasm['vm_config']['code']={'remote':remote}
 target=ROOT/'dist/envoy.remote.json'
 target.parent.mkdir(parents=True,exist_ok=True)
 target.write_text(json.dumps(cfg,indent=2)+'\n')
 return remote['sha256']
def wait_download(expected):
 client=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 for _ in range(45):
  try:
   with client.open('http://127.0.0.1:19093/wasm/gateway-observer-v1.wasm',timeout=1) as response:
    if hashlib.sha256(response.read()).hexdigest()==expected:return
  except (OSError,urllib.error.URLError):pass
  time.sleep(1)
 raise RuntimeError('Observer Wasm download readiness or checksum failure')
def external_host():
 env_file=ROOT/'deploy/local/.env'
 configured=env_file.exists() and re.search(r'^\s*(?:export\s+)?KAFKA_EXTERNAL_HOST\s*=',env_file.read_text(),re.M)
 host=os.environ.get('KAFKA_EXTERNAL_HOST')
 if not host and configured:
  cfg=json.loads(run('config','--format','json',capture_output=True).stdout)
  host=cfg['services']['kafka']['environment']['KAFKA_ADVERTISED_LISTENERS'].split('EXTERNAL://',1)[1].rsplit(':',1)[0]
 if not host:
  try:
   with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as route:
    route.connect(('192.0.2.1',80))  # UDP route lookup only; no packets are sent.
    host=route.getsockname()[0]
  except OSError as err:
   raise RuntimeError('Set KAFKA_EXTERNAL_HOST to the client-reachable server IPv4 address or hostname') from err
 if not re.fullmatch(r'[A-Za-z0-9_.-]+',host) or host=='0.0.0.0':
  raise RuntimeError('KAFKA_EXTERNAL_HOST must be a reachable IPv4 address or hostname, not 0.0.0.0')
 if not configured:
  env_file.parent.mkdir(parents=True,exist_ok=True)
  with env_file.open('a') as out:out.write('\n# Local collection Kafka external address\nKAFKA_EXTERNAL_HOST='+host+'\n')
 os.environ['KAFKA_EXTERNAL_HOST']=host
 print('Kafka external address: '+host+':29092',flush=True)
def migrate_storage():
 # Port changes recreate containers. Preserve old writable-layer data first.
 cfg=json.loads(run('config','--format','json',capture_output=True).stdout)
 paths=[('kafka','/tmp/kafka-logs','/var/lib/kafka/data','kafka-data'),
        ('doris','/opt/apache-doris/fe/doris-meta','/opt/apache-doris/fe/doris-meta','doris-fe-meta'),
        ('doris','/opt/apache-doris/be/storage','/opt/apache-doris/be/storage','doris-be-storage')]
 moves=[]
 old={}
 for service,source,target,key in paths:
  cid=run('ps','--all','-q',service,capture_output=True).stdout.strip()
  if not cid:continue
  info=json.loads(subprocess.run(['docker','inspect',cid],text=True,capture_output=True,check=True).stdout)[0]
  volume=cfg['volumes'][key]['name']
  if any(m['Destination']==target and m.get('Name')==volume for m in info['Mounts']):continue
  # Refuse to merge two histories or overwrite a prior/incomplete migration.
  exists=subprocess.run(['docker','volume','inspect',volume],capture_output=True).returncode==0
  if exists:raise RuntimeError('Refusing to overwrite existing storage volume '+volume)
  old[service]=cid
  moves.append((cid,source,volume,key,service))
 if not moves:return
 backup=ROOT/'.tmp'/('local-storage-'+str(time.time_ns()))
 backup.mkdir(parents=True)
 running=[]
 for service in ['doris','kafka','fixture','observer','consumer','gateway']:
  cid=run('ps','-q',service,capture_output=True).stdout.strip()
  if cid:running.append(cid)
 print('Preserving local Kafka/Doris storage in '+str(backup),flush=True)
 try:
  run('stop','--timeout','60','gateway','observer','consumer','fixture')
  run('stop','--timeout','60','kafka','doris')
  # Finish all backups before creating/populating any target volume.
  for cid,source,volume,key,service in moves:
   dest=backup/key
   dest.mkdir()
   subprocess.run(['docker','cp','-a',cid+':'+source+'/.',str(dest)],check=True)
  (backup/'containers.json').write_text(json.dumps(old,indent=2)+'\n')
  for cid,source,volume,key,service in moves:
   subprocess.run(['docker','volume','create','--label','com.docker.compose.project='+cfg['name'],
                   '--label','com.docker.compose.volume='+key,volume],check=True,stdout=subprocess.DEVNULL)
   script='cp -a /backup/. /data/'
   if service=='kafka':script+=' && chown -R 1000:1000 /data'
   subprocess.run(['docker','run','--rm','--network','none','--user','0',
                   '--mount','type=volume,source='+volume+',target=/data',
                   '--mount','type=bind,source='+str(backup/key)+',target=/backup,readonly',
                   'alpine:latest','sh','-c',script],check=True)
 except BaseException:
  # Original containers remain intact until Compose recreates them in up().
  if running:subprocess.run(['docker','start',*running],check=False,stdout=subprocess.DEVNULL)
  raise
 print('Storage copied to persistent volumes; backup retained at '+str(backup),flush=True)
def infra_up():
 external_host()
 migrate_storage()
 run('up','-d','kafka','doris')
 for _ in range(90):
  result=subprocess.run([*BASE,'exec','-T','doris','mysql','-uroot','-h127.0.0.1','-P9030','-e','SHOW BACKENDS;'],text=True,capture_output=True)
  rows=result.stdout.strip().splitlines()
  if result.returncode==0 and len(rows)>1:
   columns=rows[0].split('\t')
   if 'Alive' in columns and any(row.split('\t')[columns.index('Alive')]=='true' for row in rows[1:]):break
  time.sleep(2)
 else:raise RuntimeError('Doris readiness timeout')
 for name in ['001_init.sql','002_usage_summaries.sql','004_summary_coordination.sql','005_consumer_recovery.sql','006_optional_route_identity.sql']:
  run('exec','-T','doris','mysql','-uroot','-h127.0.0.1','-P9030',input=(ROOT/'migrations/doris/metering'/name).read_text())
 for _ in range(45):
  result=subprocess.run([*BASE,'exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:9092','--create','--if-not-exists','--topic','llm-metering-events','--partitions','24','--replication-factor','1','--config','retention.ms=259200000','--config','cleanup.policy=delete'],capture_output=True)
  if result.returncode==0:break
  time.sleep(2)
 else:raise RuntimeError('Kafka readiness timeout')
def up():
 for name in ['observer','consumer','fixture','gateway-observer-v1.wasm']:
  if not (ROOT/'dist'/name).is_file():raise RuntimeError('Run make build wasm first')
 expected=render_gateway()
 infra_up()
 run('up','-d','fixture','observer','consumer')
 # Mounted executables/artifacts may change without a Compose configuration change.
 run('restart','fixture','observer','consumer')
 wait_download(expected)
 run('up','-d','--force-recreate','gateway')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('action',choices=['up','infra-up','stop']);args=p.parse_args()
 if args.action=='up':up()
 elif args.action=='infra-up':infra_up()
 else:run('stop')
