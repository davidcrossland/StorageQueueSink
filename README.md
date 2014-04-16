StorageQueueSink
================

Description
==
A Apache Spark metric sink that sends a notification message to an Azure Storage Queue when a job has been detected to be complete

Usage
==

Add the following lines to conf/metrics.properties

*.sink.queue.class=org.apache.spark.metrics.sink.ServiceBusQueueSink
*.sink.queue.period=1
*.sink.queue.unit=seconds
*.sink.queue.connection=DefaultEndpointsProtocol=http;AccountName=%storageName%;AccountKey=$storageKey%
*.sink.queue.queue=%queueName%

Note:
Substitute %storageName% and %storageKey% as apropriate for the storage queue, and replace %queueName%

Bugs
==

If a job completes very quickly and the metrics system has not been invoked between job sumbission and completion we do not detect a job has finished.  The metric system reports allJobs > 0 before it reports runningJobs > 0 and unless we detect a job has been in a running state we cannot report on success/failure
