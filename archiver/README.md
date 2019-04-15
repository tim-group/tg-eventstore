Archiver
========

Archives Eventstores to S3.


Running Integration tests
=========================
To run S3ArchiverIntegrationTest.java, your S3 user needs access to test-tg-eventstore-archive bucket.

To enable S3 access for your user, please follow these steps:
  (1) Log in to Amazon Console
  (2) Open Services -> IAM -> Users
  (3) Find your user
  (4) Click [Add Permission]
  (5) Click [Attach existing policies directly]
  (6) Find policy test-tg-eventstore-archive_RW and click it. This will allow read and write access to the test bucket.
  (7) Save
  
On a command line, copy and paste the below 

cat > s3_do_not_check_in.properties <<EOF
s3.accessKey=AKIAIMGMQ53PZL2UK7TA
s3.secretKey=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
s3.region=eu-west-2
s3.protocol=HTTPS
tg.eventstore.archive.bucketName=test-tg-eventstore-archive
EOF


Now you should be able to run the S3ArchiverIntegrationTest test.

If the  s3_do_not_check_in.properties file is missing, the tests in S3ArchiverIntegrationTest are skipped (as oppose to fail).


