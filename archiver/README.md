Archiver
========

Archives Eventstores to S3.


Running Integration tests
=========================
To run subclasses of S3IntegrationTest.java, you need to have an S3 configuration file.
On a command line, copy and paste the below (execute in the same directory as this README file): 

```
  cat > s3_do_not_check_in.properties <<EOF
  s3.accessKey=<YOUR ACCESS KEY>
  s3.secretKey=<YOUR SECRET KEY>
  s3.region=eu-west-2
  s3.protocol=HTTPS
  tg.eventstore.archive.bucketName=test-tg-eventstore-archive
  EOF
```

Now you should be able to run the S3 integration tests.
If the s3_do_not_check_in.properties file is missing, those tests are skipped (as opposed to failing).
