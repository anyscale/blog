from delivery_report import delivery_report
import json
import requests
from confluent_kafka import Producer

# Establish HTTP connection to Twitter event stream.
headers = {
  'Authorization': 'Bearer YOUR_BEARER_TOKEN'
}
params = {
  'expansions': 'attachments.media_keys',
  'media.fields': 'url'
}
r = requests.get(
  'https://api.twitter.com/2/tweets/search/stream',
  headers=headers,
  params=params,
  stream=True
)
tweets = r.iter_lines()

# Set up Kafka producer.
kafka = {
  'connection': {'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVER:PORT'},
  'producer': {}
}
p = Producer({**kafka['connection'], **kafka['producer']})

# Produce messages until the process is interrupted.
try:
  tweet = next(tweets)
  if tweet:
    t = json.loads(tweet.decode('utf-8'))
    if 'includes' in t and 'media' in t['includes']:
      for image in t['includes']['media']:
        if 'media_key' in image and 'url' in image:
          msg = json.dumps({
            'id': image['media_key'],
            'url': image['url']
          }).encode('utf-8')
          p.produce('urls', msg, callback=delivery_report)
          p.poll(0)
except KeyboardInterrupt:
  pass
finally:
  p.flush(30)