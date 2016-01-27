# hive-bucket-helpers

This is a hive-2.0 UDF, this will not work with hive-1.x due to methods used.

    hive> add jar /tmp/bucket-helpers-1.0-SNAPSHOT.jar
    > ;
    Added [/tmp/bucket-helpers-1.0-SNAPSHOT.jar] to class path
    Added resources: [/tmp/bucket-helpers-1.0-SNAPSHOT.jar]
    hive> CREATE TEMPORARY FUNCTION bucket_check as 'org.notmysock.hive.udf.BucketCheckUDF';
    
    hive> select INPUT__FILE__NAME, bucket_check(hash(a),INPUT__FILE__NAME, 7) from x where not bucket_check(hash(a),INPUT__FILE__NAME, 7);
    OK
    Time taken: 0.715 seconds

That is how a good run looks like, but you have to be careful to pick the right column for the hash and the right bucket number.

    hive> select INPUT__FILE__NAME, bucket_check(hash(a),INPUT__FILE__NAME, 12) from x where not bucket_check(hash(a),INPUT__FILE__NAME, 12);
    OK
    hdfs://sandbox.hortonworks.com:8020/tmp/hive/gopal/774e1d2d-4815-4f58-a806-9c529d854f6d/_tmp_space.db/7d6ecbbc-dd4d-4b59-b5c6-bb3c672992e7/000001_0     false
    Time taken: 0.198 seconds, Fetched: 1 row(s)

This how a miss looks like (I'm explicitly using a different bucket count to demonstrate a bucketing ALTER table after insert).
