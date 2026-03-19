import random
import asyncio
import grpc
import os

from proto import telemetry_pb2
from proto import telemetry_pb2_grpc

from sensor.mock_sensor import MockSensor
from sensor.sensor_helper import (
    temp_boiler_room,
    temp_warehouse,
    temp_assembly_line,
    temp_lab,
    humidity_boiler_room,
    humidity_warehouse,
    humidity_assembly_line,
    humidity_lab,
    vibration_boiler_room,
    vibration_warehouse,
    vibration_assembly_line,
    vibration_lab,
)

collector_addrs_str = os.getenv(
    "COLLECTOR_ADDRS",
    "localhost:50051,localhost:50052"
)

COLLECTOR_ADDRS = [
    addr.strip()
    for addr in collector_addrs_str.split(",")
    if addr.strip()
]

async def send_sensor(sensor: MockSensor):
    backoff = 1

    while True:
        addr = random.choice(COLLECTOR_ADDRS)

        try:
            async with grpc.aio.insecure_channel(addr) as channel:

                stub = telemetry_pb2_grpc.IngestServiceStub(channel)

                async def request_generator():
                    async for reading in sensor.stream():
                        yield telemetry_pb2.Measurement(
                            meta=telemetry_pb2.SensorMeta(
                                sensor_id=reading.sensor_id,
                                sensor_type=reading.sensor_type,
                                location=reading.location,
                            ),
                            value=reading.value,
                            ts_unix_ms=reading.ts_unix_ms,
                        )

                print(f"[{sensor.sensor_id}] streaming -> {addr}")

                await stub.PushMeasurements(request_generator())

                backoff = 1

        except grpc.aio.AioRpcError as e:
            print(f"[{sensor.sensor_id}] disconnected from {addr}:", e.code())

        sleep_time = backoff + random.uniform(0, 0.5)
        print(f"[{sensor.sensor_id}] retrying in {sleep_time:.1f}s")

        await asyncio.sleep(sleep_time)

        backoff = min(backoff * 2, 30)


async def main():

    sensors = [
        MockSensor("T-BR-01", "temperature", "boiler_room", 0.5, temp_boiler_room),
        MockSensor("T-BR-02", "temperature", "boiler_room", 0.7, temp_boiler_room),
        MockSensor("H-WH-01", "humidity", "warehouse", 1.2, humidity_warehouse),
        MockSensor("T-WH-03", "temperature", "warehouse", 1.1, temp_warehouse),
        MockSensor("T-WH-04", "temperature", "warehouse", 0.9, temp_warehouse),
        MockSensor("V-AL-01", "vibration", "assembly_line", 0.3, vibration_assembly_line),
        MockSensor("T-L-05", "temperature", "lab", 1.5, temp_lab),
    ]

    sensors = random.sample(sensors, k=5)

    tasks = [
        asyncio.create_task(send_sensor(s))
        for s in sensors
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())