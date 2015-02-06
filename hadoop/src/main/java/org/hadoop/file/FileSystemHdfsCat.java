package org.hadoop.file;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemHdfsCat {
	public static void main(String[] args){
		String url = "hdfs://master:9000/test/output/part-r-00000";
		
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(url), conf);
			FSDataInputStream open = fs.open(new Path(url));
			IOUtils.copyBytes(open, System.out,4096, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
