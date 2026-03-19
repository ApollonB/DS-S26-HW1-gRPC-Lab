import asyncio
import time
import json
import argparse
import os

import grpc
import redis.asyncio as redis

from collections import defaultdict

from proto import telemetry_pb2
from proto import telemetry_pb2_grpc

# --------------------------------------------------
# Redis Configuration
# --------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)


# --------------------------------------------------
# Async Aggregation Store
# --------------------------------------------------

class AggregationStoreAsync:

    async def update(self, measurement):

        type_loc_key = f"agg:{measurement.meta.sensor_type}:{measurement.meta.location}"
        sensor_stats_key = f"sensor:{measurement.meta.sensor_id}:stats"
        sensor_recent_key = f"sensor:{measurement.meta.sensor_id}:recent"

        while True:
            try:
                async with redis_client.pipeline(transaction=True) as pipe:

                    await pipe.watch(type_loc_key, sensor_stats_key)

                    global_current = await pipe.hgetall(type_loc_key)

                    g_count = int(global_current.get("count", 0)) + 1
                    g_sum = float(global_current.get("sum", 0)) + measurement.value
                    g_min = min(float(global_current.get("min", measurement.value)), measurement.value)
                    g_max = max(float(global_current.get("max", measurement.value)), measurement.value)

                    sensor_current = await pipe.hgetall(sensor_stats_key)

                    s_count = int(sensor_current.get("count", 0)) + 1
                    s_sum = float(sensor_current.get("sum", 0)) + measurement.value
                    s_min = min(float(sensor_current.get("min", measurement.value)), measurement.value)
                    s_max = max(float(sensor_current.get("max", measurement.value)), measurement.value)

                    recent_entry = json.dumps({
                        "ts": measurement.ts_unix_ms,
                        "value": measurement.value
                    })

                    pipe.multi()

                    pipe.hset(type_loc_key, mapping={
                        "count": g_count,
                        "sum": g_sum,
                        "min": g_min,
                        "max": g_max,
                    })

                    pipe.hset(sensor_stats_key, mapping={
                        "count": s_count,
                        "sum": s_sum,
                        "min": s_min,
                        "max": s_max,
                    })

                    pipe.lpush(sensor_recent_key, recent_entry)
                    pipe.ltrim(sensor_recent_key, 0, 19)

                    await pipe.execute()
                    break

            except redis.WatchError:
                continue

    async def snapshot(self):

        keys = await redis_client.keys("agg:*")

        result = {}

        for key in keys:
            data = await redis_client.hgetall(key)
            result[key] = data

        return result


store = AggregationStoreAsync()


# --------------------------------------------------
# Ingest Service (Client Streaming)
# --------------------------------------------------

class IngestService(telemetry_pb2_grpc.IngestServiceServicer):

    async def PushMeasurements(self, request_iterator, context):

        print("[Collector] IngestService active")

        received = 0

        async for measurement in request_iterator:

            received += 1

            print(
                f"[Ingest] {measurement.meta.sensor_id} "
                f"{measurement.meta.sensor_type}@{measurement.meta.location} "
                f"value={measurement.value:.2f}"
            )

            await store.update(measurement)

        return telemetry_pb2.IngestAck(received=received)


# --------------------------------------------------
# Aggregate Streaming Service (placeholder streaming)
# --------------------------------------------------

class AggregateService(telemetry_pb2_grpc.AggregateServiceServicer):

    async def StreamAggregates(self, request, context):

        print("[Collector] AggregateService active")

        while True:

            snap = await store.snapshot()

            for key, v in snap.items():

                if not v:
                    continue

                count = int(v["count"])
                sum_v = float(v["sum"])

                avg = sum_v / count if count else 0

                yield telemetry_pb2.Aggregate(
                    key=telemetry_pb2.AggregateKey(
                        sensor_type=key.split(":")[1],
                        location=key.split(":")[2],
                    ),
                    count=count,
                    sum=sum_v,
                    min=float(v["min"]),
                    max=float(v["max"]),
                    updated_unix_ms=int(time.time() * 1000),
                )

            await asyncio.sleep(5)


# --------------------------------------------------
# Query Service
# --------------------------------------------------

class QueryService(telemetry_pb2_grpc.QueryServiceServicer):

    async def GetSensorStats(self, request, context):

        sensor_stats_key = f"sensor:{request.sensor_id}:stats"
        sensor_recent_key = f"sensor:{request.sensor_id}:recent"

        stats = await redis_client.hgetall(sensor_stats_key)
        recent_raw = await redis_client.lrange(sensor_recent_key, 0, 19)

        recent_values = []

        for entry in recent_raw:
            obj = json.loads(entry)
            recent_values.append(
                telemetry_pb2.RecentValue(
                    value=float(obj["value"]),
                    timestamp_ms=int(obj["ts"])
                )
            )

        return telemetry_pb2.GetSensorStatsResponse(
            sensor_id=request.sensor_id,
            recent_values=recent_values
        )


# --------------------------------------------------
# Stats Printer
# --------------------------------------------------

async def stats_printer():

    while True:

        await asyncio.sleep(5)

        snap = await store.snapshot()

        print("\n===== REDIS AGGREGATES =====")

        for key, v in snap.items():

            if not v:
                continue

            count = int(v["count"])
            sum_v = float(v["sum"])

            avg = sum_v / count if count else 0

            print(
                f"{key} count={count} avg={avg:.2f} "
                f"min={float(v['min']):.2f} max={float(v['max']):.2f}"
            )

        print("============================\n")


# --------------------------------------------------
# Server Bootstrap
# --------------------------------------------------

async def serve(port: int):

    server = grpc.aio.server()

    telemetry_pb2_grpc.add_IngestServiceServicer_to_server(
        IngestService(), server
    )

    telemetry_pb2_grpc.add_AggregateServiceServicer_to_server(
        AggregateService(), server
    )

    telemetry_pb2_grpc.add_QueryServiceServicer_to_server(
        QueryService(), server
    )

    server.add_insecure_port(f"[::]:{port}")

    await server.start()

    print(f"Collector running on :{port}")

    asyncio.create_task(stats_printer())

    await server.wait_for_termination()


# --------------------------------------------------

if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)

    args = ap.parse_args()

    asyncio.run(serve(args.port))
    