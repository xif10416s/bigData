package org.fxi.test.ml.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;

public class Utils {
	
	public static final String SPLIT_TAB="	";
	public static final String SPLIT_LINE="\r\n";

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
//		Utils.writePropertiesFile(
//				"C:/ml/result/ActivityUserByActivityStatusRegistTime.txt",
//				"11", 11 + "");
		saveToFile("I:/data/ml/result/save.txt","a	b	\r\nc	b");

	}

	public static Integer valueOf(String part) {
		return "\\N".equals(part) ? 0 : Integer.parseInt(part) ;
	}
	
	public static void saveToFile(String file, String conent) {     
        BufferedWriter out = null;     
        try {     
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,true)));     
            out.write(conent);     
        } catch (Exception e) {     
            e.printStackTrace();     
        } finally {     
            try {     
                if(out != null){  
                    out.close();     
                }  
            } catch (IOException e) {     
                e.printStackTrace();     
            }     
        }     
    }     
}
