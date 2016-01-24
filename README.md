# emr-simple
This realisation for parse domain name in msgtype=response in raw data WARC file on Amazon EMR.</br>
And summary domain name in reduce, send result for SQS Amazon. Input files get form url.
Special for Noah Silverman. </br>
Example usage: <inUrlPath> <outDir> <bucketName> <myQueueUrl> <region> <accessKey> <secretKey> </br>
s3://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00000-ip-10-137-6-227.ec2.internal.warc.gz  </br>
elasticmapreduce/outDir/  </br>
aws-logs-xxx-us-west-2  </br>
sqsQueueName  </br>
us-west-2 </br>
accessKey </br>
secretKey </br>
