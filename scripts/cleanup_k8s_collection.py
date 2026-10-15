"""清理本地 Kubernetes 采集测试资源，保留共享 Kafka/Doris 数据卷。"""
import sys
from k8s_collection import main

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:], action='cleanup'))
