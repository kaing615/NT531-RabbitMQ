# Pub/Sub baseline (fanout exchange)

Bo script nay la ban chuyen tu work-queue sang pub/sub fanout voi:
- M subscriber/queue, mac dinh M=3
- manual ACK
- Mode A: non-durable queue, non-persistent message, confirms OFF
- Mode B: durable queue, persistent message, confirms OFF
- Bien thay doi: rate, prefetch, message size

## File chinh
- `producer_pubsub.py`: publisher fanout
- `subscriber.py`: subscriber manual ACK, log latency
- `run_subscribers_until_drained.sh`: start M subscribers, theo doi CPU/RAM/backlog, doi drain
- `summarize_pubsub_run.py`: tong hop throughput/latency/CPU/RAM/backlog
- `run_one_pubsub.sh`: chay 1 to hop tham so
- `run_pubsub_matrix.sh`: quet matrix benchmark

## Chay 1 lan
```bash
chmod +x *.sh
./run_one_pubsub.sh --mode A --rate 1000 --m 3 --prefetch 50 --size 1024 --seconds 30
```

## Chay ca matrix
```bash
chmod +x *.sh
./run_pubsub_matrix.sh
```

## Ghi chu moi truong
Mac dinh script dang giong bo work-queue cua ban:
- Cluster 2 node
- node 1 container: `rabbit-1`
- node 2 ssh: `nguyentrinh2135@node2-server`
- metrics CPU/RAM doc tu process `beam.smp`

Neu moi truong khac, override bang env:
```bash
CONTAINER_1=rabbit-1 NODE2_SSH=user@node2-server RABBIT_HOST=127.0.0.1 ./run_one_pubsub.sh
```
