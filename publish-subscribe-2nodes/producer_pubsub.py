#!/usr/bin/env python3
import argparse
import json
import os
import time
import uuid
import pika


def parse_args():
    p = argparse.ArgumentParser(description="RabbitMQ Pub/Sub producer benchmark (fanout)")
    p.add_argument("--host", default=os.getenv("RABBIT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("RABBIT_PORT", "5672")))
    p.add_argument("--user", default=os.getenv("RABBIT_USER", "admin"))
    p.add_argument("--password", default=os.getenv("RABBIT_PASS", "admin123"))
    p.add_argument("--exchange", default=os.getenv("EXCHANGE_NAME", "events_fanout_A"))
    p.add_argument("-n", "--messages", type=int, default=10000)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--rate", type=float, default=0.0, help="msg/s, 0=unlimited")
    p.add_argument("--durable", action="store_true", help="declare durable exchange")
    p.add_argument("--persistent", action="store_true", help="delivery_mode=2")
    return p.parse_args()



def main():
    args = parse_args()

    creds = pika.PlainCredentials(args.user, args.password)
    params = pika.ConnectionParameters(
        host=args.host,
        port=args.port,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=60,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.exchange_declare(exchange=args.exchange, exchange_type="fanout", durable=args.durable, auto_delete=False)

    props = pika.BasicProperties(content_type="application/json")
    if args.persistent:
        props.delivery_mode = 2

    interval = (1.0 / args.rate) if args.rate and args.rate > 0 else 0.0
    next_send = time.perf_counter()

    run_id = uuid.uuid4().hex[:10]
    t0 = time.perf_counter()
    for i in range(args.messages):
        payload = {
            "run_id": run_id,
            "seq": i,
            "sent_ts": time.time(),
            "body": "x" * max(0, args.payload_bytes - 64),
        }
        body = json.dumps(payload).encode("utf-8")
        ch.basic_publish(
            exchange=args.exchange,
            routing_key="",
            body=body,
            properties=props,
            mandatory=False,
        )

        if interval > 0:
            next_send += interval
            now = time.perf_counter()
            if next_send > now:
                time.sleep(next_send - now)
            else:
                next_send = now

    t1 = time.perf_counter()
    duration = t1 - t0
    throughput = args.messages / duration if duration > 0 else 0.0

    print("=== Producer Benchmark Result ===")
    print(f"host: {args.host}:{args.port}")
    print(f"exchange: {args.exchange}")
    print(f"messages: {args.messages}")
    print(f"payload_bytes: ~{args.payload_bytes}")
    print(f"durable_exchange: {args.durable}")
    print(f"persistent_msg: {args.persistent}")
    print("publisher_confirms: False")
    print(f"rate_limit: {args.rate} msg/s (0 = unlimited)")
    print(f"run_id: {run_id}")
    print(f"duration_sec: {duration:.4f}")
    print(f"throughput_msg_per_sec: {throughput:.2f}")

    conn.close()


if __name__ == "__main__":
    main()
