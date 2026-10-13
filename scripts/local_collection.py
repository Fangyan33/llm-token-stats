"""Isolated local collection harness. Never migrates an external database."""
import argparse,json,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BASE=['docker','compose','-f',str(ROOT/'deploy/local/compose.yaml')]
def run(*args,input=None):
 return subprocess.run([*BASE,*args],input=input,text=True,check=True,cwd=ROOT)
def up():
 for name in ['observer','consumer','fixture','gateway-observer-v1.wasm']:
  if not (ROOT/'dist'/name).is_file():raise RuntimeError('Run make build wasm first')
 run('up','-d','kafka','doris')
 for _ in range(90):
  result=subprocess.run([*BASE,'exec','-T','doris','mysql','-uroot','-h127.0.0.1','-P9030','-e','SHOW BACKENDS;'],text=True,capture_output=True)
  rows=result.stdout.strip().splitlines()
  if result.returncode==0 and len(rows)>1:
   columns=rows[0].split('\t')
   if 'Alive' in columns and any(row.split('\t')[columns.index('Alive')]=='true' for row in rows[1:]):break
  time.sleep(2)
 else:raise RuntimeError('Doris readiness timeout')
 for name in ['001_init.sql','002_usage_summaries.sql','004_summary_coordination.sql','005_consumer_recovery.sql']:
  run('exec','-T','doris','mysql','-uroot','-h127.0.0.1','-P9030',input=(ROOT/'migrations/doris/metering'/name).read_text())
 for _ in range(45):
  result=subprocess.run([*BASE,'exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:9092','--create','--if-not-exists','--topic','llm-metering-events','--partitions','24','--replication-factor','1','--config','retention.ms=259200000','--config','cleanup.policy=delete'],capture_output=True)
  if result.returncode==0:break
  time.sleep(2)
 else:raise RuntimeError('Kafka readiness timeout')
 run('up','-d','fixture','observer','consumer','gateway')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('action',choices=['up','stop']);args=p.parse_args()
 if args.action=='up':up()
 else:run('stop')
