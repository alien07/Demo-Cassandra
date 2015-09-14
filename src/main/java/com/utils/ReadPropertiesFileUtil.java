package com.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

public class ReadPropertiesFileUtil {

	private static final String RESOURCES_CONFIGS_PATH = "configs";
	private static final String DB_CONFIG_PROPERTIES_FILE = "db_config.properties";

	public static void main(String[] args) {
		printProperties(readDbConfig());
		System.exit(0);
	}

	private ReadPropertiesFileUtil() {

	}

	public static Properties readDbConfig() {
		Properties properties = new Properties();
		try {
			
			final StringBuilder fileName = new StringBuilder(File.separator)
					.append(RESOURCES_CONFIGS_PATH).append(File.separator).append(DB_CONFIG_PROPERTIES_FILE);
			
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			InputStream fileInput = cl.getResourceAsStream(fileName.toString());
			properties.load(fileInput);
			fileInput.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}
	
	public static void printProperties(Properties properties) {
		Enumeration<Object> enuKeys = properties.keys();
		while (enuKeys.hasMoreElements()) {
			String key = (String) enuKeys.nextElement();
			String value = properties.getProperty(key);
			System.out.println(key + ": " + value);
		}
	}

}
