package edu.miu.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtils {

	public static void deletePathIfExists(Configuration conf, String stepOutputPath)
			throws IOException {
		Path path = new Path(stepOutputPath);
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

}
