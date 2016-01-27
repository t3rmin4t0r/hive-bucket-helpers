package org.notmysock.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class BucketIdUDF extends UDF {
	transient IntWritable result = new IntWritable();
	public final IntWritable evaluate(Text a) {
		String filename = a.toString();
		filename = filename.substring(filename.indexOf('/')+1);
		result.set(Utilities.getBucketIdFromFile(filename));
		return result;
	}
}
