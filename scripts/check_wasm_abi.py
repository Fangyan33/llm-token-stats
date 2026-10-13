"""Check the artifact, without executing it or assuming runtime compatibility."""
import sys
from pathlib import Path
b=Path(sys.argv[1]).read_bytes()
assert b[:8]==b'\x00asm\x01\x00\x00\x00'
def var(b,i):
 n=s=0
 while True:
  c=b[i];i+=1;n|=(c&127)<<s;s+=7
  if c<128:return n,i

def name(b,i):
 n,i=var(b,i);return b[i:i+n].decode(),i+n
exports=set();imports=set();i=8
while i<len(b):
 kind=b[i];i+=1;n,i=var(b,i);section=b[i:i+n];i+=n
 if kind==7:
  count,p=var(section,0)
  for _ in range(count):
   key,p=name(section,p);p+=1;_,p=var(section,p);exports.add(key)
 if kind==2:
  count,p=var(section,0)
  for _ in range(count):
   module,p=name(section,p);key,p=name(section,p);t=section[p];p+=1;imports.add(key)
   if t==0:_,p=var(section,p)
   elif t==1:p+=1;flags,p=var(section,p);_,p=var(section,p);p=var(section,p)[1] if flags&1 else p
   elif t==2:flags,p=var(section,p);_,p=var(section,p);p=var(section,p)[1] if flags&1 else p
   elif t==3:p+=2
required={'proxy_abi_version_0_2_0','proxy_on_grpc_receive','proxy_on_grpc_close','proxy_on_grpc_receive_initial_metadata','proxy_on_grpc_receive_trailing_metadata','proxy_on_request_body','proxy_on_tick'}
assert required<=exports, required-exports
assert {'proxy_grpc_stream','proxy_grpc_send','proxy_grpc_cancel'}<=imports
assert not any('sock_' in x for x in imports)
print('PASS: Wasm ABI marker, gRPC callbacks/hostcalls, no socket imports')
