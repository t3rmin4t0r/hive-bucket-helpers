package org.notmysock.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

@Description(name = "bucket_check", value = "_FUNC_(hash(bucket_col), INPUT__FILE__NAME, bucket_count) - returns true if the bucketing is good ")
public class BucketCheckUDF extends UDF {
	public static final Logger LOG = LoggerFactory
			.getLogger(BucketCheckUDF.class);
	transient BooleanWritable good = new BooleanWritable(true);
	transient BooleanWritable bad = new BooleanWritable(false);
	public final BooleanWritable evaluate(IntWritable hash, Text a, IntWritable bucket_count) {
		String filename = a.toString();
		filename = filename.substring(filename.lastIndexOf('/')+1);
		int bucket = Utilities.getBucketIdFromFile(filename);
		Preconditions.checkArgument(bucket < bucket_count.get(), "Is this really bucketed into {} buckets?", bucket_count);
		int expected = ObjectInspectorUtils.getBucketNumber(hash.get(), bucket_count.get());
		if (expected != bucket) {
			LOG.warn("filename={}, hash={}, bucket={}, expected={}", filename, hash.get(), bucket, expected);
			return bad;
		}
		return good;
	}
}
