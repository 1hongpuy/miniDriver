# MiniDriver 3.0 local Compose

```bash
export MINIKV_V2_CLUSTER_SECRET='replace-with-a-random-secret'
docker compose -f deploy/v3/compose.local.yaml build
docker compose -f deploy/v3/compose.local.yaml up -d --wait
./deploy/v3/smoke.sh
docker compose -f deploy/v3/compose.local.yaml down
```

`down`不会删除volume；只有显式执行`down -v`才删除对象数据。四个命名volume彼此独立，禁止把三个DataNode改为同一目录。
