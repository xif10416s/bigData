package org.fxi.test.ml.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class Utils {

	// 写资源文件，含中文
	public static void writePropertiesFile(String filename, String key,
			String value) {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Properties properties = new Properties();
		try {
			InputStream fis = new FileInputStream(filename);
			properties.load(fis);
			OutputStream outputStream = new FileOutputStream(filename);
			properties.setProperty(key, value);
			properties.store(outputStream, "tt");
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Utils.writePropertiesFile(
				"C:/ml/result/ActivityUserByActivityStatusRegistTime.txt",
				"11", 11 + "");

	}

	public static Integer valueOf(String part) {
		return "\\N".equals(part) ? 0 : Integer.parseInt(part) ;
	}
}
