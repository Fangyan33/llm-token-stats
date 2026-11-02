# No compiler, Docker socket, kubeconfig, or host mount in the test Job.
FROM alpine:latest
COPY k8s-suite/ /suite/
WORKDIR /suite/tests/integration
ENTRYPOINT ["/suite/integration.test", "-test.v", "-test.count=1", "-test.timeout=20m"]
