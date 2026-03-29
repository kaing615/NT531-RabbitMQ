# RabbitMQ Pub/Sub Baseline – 2 VM local

Triển khai theo mô hình:

- **Node 1**: Load generation layer  
  Chạy `producer_pubsub.py`, `subscriber.py`, và các script benchmark
- **Node 2**: RabbitMQ broker  
  Chạy RabbitMQ server và nhận kết nối từ Node 1

Kịch bản này dùng **fanout exchange** để broadcast message tới **M subscriber queues** (mặc định `M=3`). Bộ script benchmark hiện tại dùng producer `producer_pubsub.py`, subscriber `subscriber.py`, trình tổng hợp `summarize_pubsub_run.py`, cùng ba shell script remote-broker cho mô hình Node 1 phát tải và Node 2 làm broker. :contentReference[oaicite:0]{index=0} :contentReference[oaicite:1]{index=1} :contentReference[oaicite:2]{index=2}

---

## 1. Kiến trúc triển khai

### Node 1 – Load generation layer

Chạy các thành phần:

- `producer_pubsub.py`
- `subscriber.py`
- `run_subscribers_until_drained_remote_broker_pubsub.sh`
- `run_one_remote_broker_pubsub.sh`
- `run_pubsub_matrix_remote_broker.sh`

Vai trò:

- sinh tải publish vào RabbitMQ broker trên Node 2
- tạo các subscriber để consume message
- thu thập log benchmark:
  - `producer_stdout.log`
  - `subscribers_stdout.log`
  - `s1.jsonl`, `s2.jsonl`, `s3.jsonl`
  - `cpu_rabbit.log`
  - `mem_rabbit.log`
  - `queue_ts.csv`
  - `summary.csv`

### Node 2 – RabbitMQ broker

Chạy các thành phần:

- RabbitMQ server
- fanout exchange
- subscriber queues
- `rabbitmqctl` để Node 1 SSH vào lấy số liệu queue/metrics

Vai trò:

- nhận message từ producer
- route message từ fanout exchange sang các subscriber queues
- trả message cho subscriber ở Node 1
- cung cấp số liệu queue/CPU/RAM broker cho Node 1
