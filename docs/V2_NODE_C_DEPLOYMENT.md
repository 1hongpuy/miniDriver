# Node C DataNode Deployment

This document starts one V2 DataNode on Node C. The Gateway remains on the
Gateway machine at `100.75.93.124:18081`.

The NodeAgent only supervises child processes on its own machine. Starting the
Node C agent does not start or stop processes on the Gateway, Node B, or any
other machine.

## 1. Preconditions

Node C needs:

- Tailscale connected to the same tailnet as the Gateway.
- The same V2 Git branch as the Gateway, currently `v2`.
- The exact same `cluster.secret` file content as the Gateway.
- TCP access over Tailscale to `100.75.93.124:18081`.

Do not put `cluster.secret` in Git or into this document.

## 2. Get the V2 source

On Node C, either clone V2 directly:

```bash
git clone --branch v2 --single-branch \
  https://github.com/1hongpuy/miniDriver.git \
  ~/miniKV_v2/miniDriver
```

Or, when the repository was already cloned as `main`:

```bash
cd ~/miniKV_v2/miniDriver
git fetch origin --prune
git switch --track origin/v2
```

Verify the checked-out source:

```bash
git branch --show-current
git log --oneline --decorate -5
```

The first command must print `v2`.

## 3. Install build dependencies and compile

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config \
  libssl-dev libleveldb-dev libyaml-cpp-dev

cd ~/miniKV_v2/miniDriver
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

Expected binaries:

```bash
ls -l build/bin/minikv_v2_datanode build/bin/minikv_v2_node_agent
```

## 4. Install the shared cluster secret

The Gateway and every DataNode must use byte-for-byte identical secret content.
This is required for node registration, heartbeats, Chunk commits, and upload
capability verification.

```bash
mkdir -p ~/minikv-v2-secrets
chmod 700 ~/minikv-v2-secrets

# Copy the file from the Gateway using your preferred secure transfer method.
# The final path on Node C must be:
# ~/minikv-v2-secrets/cluster.secret
scp ~/minikv-v2-secrets/cluster.secret \
  <远程用户名>@<目标服务器Tailscale-IP>:~/minikv-v2-secrets/cluster.secret

chmod 600 ~/minikv-v2-secrets/cluster.secret
sha256sum ~/minikv-v2-secrets/cluster.secret
```

Compare the SHA-256 output with the Gateway. Do not print the secret itself.

## 5. Find Node C's Tailscale address

```bash
tailscale ip -4
```

Use the returned address as `advertiseAddress` below. It is the address that
the Gateway, browser clients, and other DataNodes use to reach Node C's data
plane on port `9002`.

## 6. Create the Node C agent configuration

```bash
mkdir -p ~/minikv-v2/config
nano ~/minikv-v2/config/node-c.yaml
```

Replace `<NODE_C_TAILSCALE_IP>` with the output from `tailscale ip -4`.
Replace `/home/<NODE_C_USER>` when Node C does not use that home directory.

```yaml
node:
  nodeId: node-c
  advertiseAddress: <NODE_C_TAILSCALE_IP>

cluster:
  secretFile: /home/<NODE_C_USER>/minikv-v2-secrets/cluster.secret
  gatewayAddress: 100.75.93.124
  gatewayPort: 18081

# Keep this section when the V2 browser frontend is served at this origin.
# It controls browser CORS access to Node C's direct Chunk endpoint.
web:
  allowedOrigin: http://100.75.93.124:8082

services:
  - id: datanode-0
    type: datanode
    enabled: true
    listenPort: 9002
    dataDir: /home/<NODE_C_USER>/minikv-v2/run/datanode
    restart:
      policy: on-failure
      initialBackoffSeconds: 2
      maxBackoffSeconds: 30
    logs:
      stdout: /home/<NODE_C_USER>/minikv-v2/logs/datanode.out.log
      stderr: /home/<NODE_C_USER>/minikv-v2/logs/datanode.err.log
    logging:
      file: /home/<NODE_C_USER>/minikv-v2/logs/datanode-node-c.log
      level: info
      queueSize: 8192
      rotateBytes: 20971520
      rotateFiles: 5
```

`allowedOrigin` is for the planned HTTP V2 frontend at port `8082`. If the
frontend origin changes later, update this exact value and restart Node C's
agent. It is not a replacement for TLS or Tailscale ACLs.

## 7. Check connectivity before starting

```bash
nc -vz 100.75.93.124 18081
```

If `nc` is unavailable:

```bash
timeout 3 bash -c '</dev/tcp/100.75.93.124/18081'
```

Success means Node C can reach the Gateway control plane. If it fails, check
Tailscale status, Tailscale ACLs, and host firewall rules before starting the
DataNode.

## 8. Start Node C in the foreground

```bash
cd ~/miniKV_v2/miniDriver

./build/bin/minikv_v2_node_agent \
  --config ~/minikv-v2/config/node-c.yaml \
  --bin-dir "$PWD/build/bin"
```

The command intentionally remains in the foreground. The NodeAgent waits for
child exits and restarts the DataNode after an unexpected failure. Leave this
terminal running for the first deployment.

In a second terminal, inspect the child process logs:

```bash
tail -F ~/minikv-v2/logs/datanode-node-c.log
```

The DataNode listens on `9002`, then registers itself with the Gateway. If the
Gateway is temporarily unavailable, registration is retried every second. Once
registered, the DataNode sends a heartbeat every eight seconds.

## 9. Verify from the Gateway machine

```bash
curl http://127.0.0.1:18081/api/v2/admin/nodes
```

The response should contain an entry similar to:

```json
{
  "nodeId": "node-c",
  "address": "<NODE_C_TAILSCALE_IP>",
  "httpPort": 9002
}
```

Also confirm Node C is listening locally:

```bash
ss -ltnp | grep ':9002'
```

## 10. Stop and update

Press `Ctrl+C` in the foreground NodeAgent terminal. The agent sends SIGTERM
to its DataNode child and does not restart it during shutdown.

For a code update:

```bash
cd ~/miniKV_v2/miniDriver
git pull
cmake --build build -j"$(nproc)"
```

Then run the start command again. Do not run two NodeAgents that both manage a
DataNode on port `9002` on the same machine.
