def delivery_report(err, msg):
  if err is not None:
    print('Message delivery failed: {}'.format(err))
  else:
    print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))