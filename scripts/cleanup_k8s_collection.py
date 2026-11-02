"""清理本地 Kubernetes 采集测试资源，删除命名空间内 Kafka/Doris，保留本地诊断。"""
import sys
from k8s_collection import main

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:], action='cleanup'))
