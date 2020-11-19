import asyncio
import random

from mode import flight_recorder
import faust

from logging import getLogger

log = getLogger(__name__)


app = faust.App("lockup-demo-4",
                broker="localhost:9092",
                stream_recovery_delay=1.0)

channel = app.channel()
topic = app.topic("my-topic-4", partitions=12, internal=True)

eventno = 0

@app.timer(interval=0.5)
async def produce_ids():
    global eventno
    event = random.randint(0, 10000)
    #print(f"Producing event {event}")
    await topic.send(value=eventno)
    eventno+=1


@app.agent(topic)
async def topic_consumer_agent(stream):
    print("Start agent")

    async for event in stream:
        print(f"Consumed event {event} from topic, sending to channel.")
        try:
            ret = await channel.send(value=event)
            print("Sent to channel")
        except BaseException as exc:
            log.exception("Got Exception inside agent")
            raise exc


@app.agent(channel, concurrency=1)
async def channel_consumer_agent(stream):
    async for event in stream:
        await asyncio.sleep(4)
        print(f"Consumed event {event} from channel.")
