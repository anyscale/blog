Serverless Kafka Stream Processing with Ray
==========================
It is important to note that this demo assumes that you have a working Kafka cluster available (for more information, refer to the Kafka quick start docs).The demo application works as follows:
It takes a Kafka topic containing URLs which point to images (consumption). 
It extracts a color palette for each image (processing).
It outputs the palette to a second topic (production).



