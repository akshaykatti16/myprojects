package com.teradata.kylofeedaccelerator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ApplicationPropertyValues 
{
	public ApplicationPropertyValues() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public  String SOURCE_GET_FEEDS_URL = null;
	public String SOURCE_EXPORT_FEED_URL = null;
	public  String TARGET_IMPORT_FEED_URL = null;
	public  String FEED_DOWNLOAD_PATH = null;
	public  String SOURCE_USER_NAME = null;
    public  String SOURCE_PASSWORD = null;
    public  String TARGET_USER_NAME = null;
    public  String TARGET_PASSWORD = null;
	InputStream inputStream;
    
	
	public String getSOURCE_GET_FEEDS_URL() {
		return SOURCE_GET_FEEDS_URL;
	}


	public String getSOURCE_EXPORT_FEED_URL() {
		return SOURCE_EXPORT_FEED_URL;
	}


	public String getTARGET_IMPORT_FEED_URL() {
		return TARGET_IMPORT_FEED_URL;
	}


	public String getFEED_DOWNLOAD_PATH() {
		return FEED_DOWNLOAD_PATH;
	}


	public String getSOURCE_USER_NAME() {
		return SOURCE_USER_NAME;
	}


	public String getSOURCE_PASSWORD() {
		return SOURCE_PASSWORD;
	}


	public String getTARGET_USER_NAME() {
		return TARGET_USER_NAME;
	}


	public String getTARGET_PASSWORD() {
		return TARGET_PASSWORD;
	}


	public void setPropValues() throws IOException 
	{
 
		try {
			Properties prop = new Properties();
			String propFileName = "application.properties";
			
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			 
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			
			SOURCE_GET_FEEDS_URL = prop.getProperty("SOURCE_GET_FEEDS_URL");
			SOURCE_EXPORT_FEED_URL = prop.getProperty("SOURCE_EXPORT_FEED_URL");
			TARGET_IMPORT_FEED_URL = prop.getProperty("TARGET_IMPORT_FEED_URL");
			FEED_DOWNLOAD_PATH = prop.getProperty("FEED_DOWNLOAD_PATH");
			
			SOURCE_USER_NAME = prop.getProperty("SOURCE_USER_NAME");
			SOURCE_PASSWORD = prop.getProperty("SOURCE_PASSWORD");
			TARGET_USER_NAME = prop.getProperty("TARGET_USER_NAME");
			TARGET_PASSWORD = prop.getProperty("TARGET_PASSWORD");	

		} 
		catch (Exception e) 
		{
			System.out.println("Exception: " + e.getMessage());
		} 
		finally 
		{
			inputStream.close();
		}
	}
}
