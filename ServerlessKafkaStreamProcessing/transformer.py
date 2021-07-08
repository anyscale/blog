from delivery_report import delivery_report
import io
import json
import ray
import requests
from colorthief import ColorThief

ray.init(address='auto')

@ray.remote
class RayProducer:
  def __init__(self, kafka, sink):
    from confluent_kafka import Producer
    self.producer = Producer({**kafka['connection'], **kafka['producer']})
    self.sink = sink

  def produce(self, palette):
    self.producer.produce(self.sink, json.dumps(palette).encode('utf-8'), callback=delivery_report)
    self.producer.poll(0)

  def destroy(self):
    self.producer.flush(30)

@ray.remote(num_cpus=1)
def get_palette(msg_value):
  try:
    r = requests.get(json.loads(msg_value.decode('utf-8'))['url'])
    palette = ColorThief(io.BytesIO(r.content)).get_palette(color_count=6)
    producer = ray.get_actor('producer')
    ray.get(producer.produce.remote(palette))
  except Exception as e:
    print('Unable to process image:', e)

@ray.remote(num_cpus=.05)
class RayConsumer(object):
  def __init__(self, kafka, source):
    from confluent_kafka import Consumer
    self.c = Consumer({**kafka['connection'], **kafka['consumer']})
    self.c.subscribe([source])

  def start(self):
    self.run = True
    while self.run:
      msg = self.c.poll(1.)
      if msg is None:
        continue
      if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
      ray.get(get_palette.remote(msg.value()))

  def stop(self):
    self.run = False

  def destroy(self):
    self.c.close()

kafka = {
  'connection': {'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVER:PORT'},
  'consumer': {
    'group.id': 'ray',
    'enable.auto.commit': True,
    'auto.offset.reset': 'earliest'
  },
  'producer': {}
}
n_consumers = 80 # At most, number of partitions in the `urls` topic.

consumers = [RayConsumer.remote(kafka, 'urls') for _ in range(n_consumers)]
producer = RayProducer.options(name='producer').remote(kafka, 'palettes')

try:
  refs = [c.start.remote() for c in consumers]
  ray.get(refs)
except KeyboardInterrupt:
  for c in consumers:
    c.stop.remote()
finally:
  for c in consumers:
    c.destroy.remote()
  producer.destroy.remote()
  ray.kill(producer)