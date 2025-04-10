# Motivations
I built this to learn about the internals of message queues like RabbitMQ where a producer
pushes to a FIFO a queue and then a single consumer polls from that queue to process requests
at their own pace.
<br><br>
I learned about Inflight, which is essentially when a request has been dequeued but not yet
nacked or acked. Dead letter queues (DLQs) are essentially for when a specific threshold for
retries has been reached and thus the request is no longer meant to be retried anymore.
Ack is short for acknowledge and nack is short for not acknowledged.
<br><br>
OpenMQ is an in memory message queue system that has features of TTL expiration as well as
persistence via AOF.
