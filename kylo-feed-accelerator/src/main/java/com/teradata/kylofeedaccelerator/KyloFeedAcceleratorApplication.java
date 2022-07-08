package com.teradata.kylofeedaccelerator;


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.core.io.FileSystemResource;

import java.util.Base64;
import org.springframework.web.client.RestTemplate;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


@SpringBootApplication 

public class KyloFeedAcceleratorApplication 
{
	public static final Logger log = LoggerFactory.getLogger(KyloFeedAcceleratorApplication.class);
	
	//load from properties file : KYLO rest urls for get-feeds,export-feed,import-feed
	public static  String SOURCE_GET_FEEDS_URL = null;
	public static String SOURCE_EXPORT_FEED_URL = null;
	public static  String TARGET_IMPORT_FEED_URL = null;

	
	//load from properties file : path to which feed zip from source gets exported to, and from which it gets imported to target
	public static  String FEED_DOWNLOAD_PATH = null;	

	
	//load from properties file : Credentials to the rest endpoint
    
	public static  String SOURCE_USER_NAME = null;
    public static  String SOURCE_PASSWORD = null;
    public static  String TARGET_USER_NAME = null;
    public static  String TARGET_PASSWORD = null;

    //to hold total no of feeds
    public static long sourceRecordsTotal = 0L;
    
    //to hold inputed systemFeedName and feedId
	public static HashMap<Integer, String> feedIDs = new HashMap<>();
	public static HashMap<Integer, String> feedNames = new HashMap<>();
	public static HashMap<String, String> feedNameIdValues = new HashMap<String, String>();
	
	//to hold feednumbers to be promoted
	public static int feednumbers[] = null;
	public static HashMap<String, String> feedNameIdToPromote = new HashMap<String, String>();
	public static Scanner input = new Scanner(System.in);

		
	public static void main(String[] args) throws ParseException, IOException 
	{
		
		log.info("ERROR");
		SpringApplication.run(KyloFeedAcceleratorApplication.class, args);
		
		//LOADING PROPERTY FILE values
		ApplicationPropertyValues props = new ApplicationPropertyValues();
		props.setPropValues();
		
		SOURCE_USER_NAME = props.getSOURCE_USER_NAME();
		SOURCE_PASSWORD = props.getSOURCE_PASSWORD();
		SOURCE_GET_FEEDS_URL = props.getSOURCE_GET_FEEDS_URL();
		SOURCE_EXPORT_FEED_URL = props.getSOURCE_EXPORT_FEED_URL();
		
		FEED_DOWNLOAD_PATH = props.getFEED_DOWNLOAD_PATH();
		
		TARGET_USER_NAME = props.getTARGET_USER_NAME();
		TARGET_PASSWORD = props.getTARGET_PASSWORD();
		TARGET_IMPORT_FEED_URL = props.getTARGET_IMPORT_FEED_URL();
		
		System.out.println("****     KYLO FEED PROMOTION Accelerator Started******");
		//Creating basic authorization headers & Request to return JSON format
		HttpHeaders headers = new HttpHeaders();
		String auth = SOURCE_USER_NAME + ":" + SOURCE_PASSWORD;
		byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(Charset.forName("US-ASCII")));
		String authHeader = "Basic " + new String(encodedAuth);
		headers.set("Authorization", authHeader);
		headers.setAccept(Arrays.asList(new MediaType[] { MediaType.APPLICATION_JSON }));
		headers.setContentType(MediaType.APPLICATION_JSON);
 
		HttpEntity<String> entity = new HttpEntity<String>(headers);
		RestTemplate restTemplate = new RestTemplate();
		

		//method call - get feeds
		String responseString = methodToGetFeeds(restTemplate,entity);
		
		//method call - parse json & store into map objects
		methodToParse(responseString,sourceRecordsTotal,feedNames,feedIDs,feedNameIdValues);
		
		//displaying no,feedname
		System.out.println("***************** Source system existing Feeds *****************");
		Iterator<Entry<Integer, String>> entriesSet1 = feedNames.entrySet().iterator();
		while(entriesSet1.hasNext())
		{
			Entry<Integer, String> entry = entriesSet1.next();
			System.out.println(entry.getKey()+"* "+entry.getValue());
		}
		System.out.println();
		
		/*
		///displaying no,feedid
		Iterator<Entry<Integer, String>> entriesSet2 = feedIDs.entrySet().iterator();
		while(entriesSet2.hasNext())
		{
			Entry<Integer, String> entry = entriesSet2.next();
			System.out.println(entry.getKey()+"* "+entry.getValue());
		}
		System.out.println();
		Iterator<Entry<String, String>> entriesSet5 = feedNameIdValues.entrySet().iterator();
		while(entriesSet5.hasNext())
		{
			Entry<String, String> entry = entriesSet5.next();
			System.out.println(entry.getKey()+"* "+entry.getValue());
		}
		*/
		//Method cal - Inputing feednumber from user 
		acceptInputFromUser(sourceRecordsTotal,feednumbers,feedIDs,feedNames,feedNameIdToPromote,input);		
		System.out.println("********************* Exporting user specified feed(s) from source : *************************");
		RestTemplate exportRestTemplate = new RestTemplate();
		//Sending in credentials as headers
		HttpHeaders exportheaders = new HttpHeaders();
		//for basic authorization
		exportheaders.set("Authorization", authHeader);
		//setting the content type
		exportheaders.setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM,MediaType.ALL));
		exportheaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
		HttpEntity<String> exportEntity = new HttpEntity<String>(exportheaders);
		
		
		File file = new File(FEED_DOWNLOAD_PATH);
		file.mkdirs();
		
		exportFeeds(exportRestTemplate,exportEntity,feedNameIdToPromote);
		System.out.println("user Selected FEED(s) EXPORT FROM SOURCE SUCCESSFUL!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.out.println("Feeds that will be imported :"+feedNameIdToPromote);
		
		System.out.println("********************* Importing user specified feed to target : *************************");
		RestTemplate importRestTemplate = new RestTemplate();
		HttpHeaders importheaders = new HttpHeaders();
		
		//Creating basic authorization headers
		String authTarget = TARGET_USER_NAME + ":" + TARGET_PASSWORD;
		byte[] encodedAuthTarget = Base64.getEncoder().encode(authTarget.getBytes(Charset.forName("US-ASCII")));
		String authHeaderTarget = "Basic " + new String(encodedAuthTarget);
		importheaders.set("Authorization", authHeaderTarget);
		
		importheaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON,MediaType.ALL));
		importheaders.setContentType(MediaType.MULTIPART_FORM_DATA);
		    
		importFeeds(importRestTemplate,importheaders,feedNameIdToPromote,input);
		System.out.println("user Selected FEED(s) IMPORT TO TARGET SUCCESSFUL!!!!!!!!!!!!!!!!!!!!!!!!!");
		input.close();
		/*
		//Temp directory delete
		System.out.println("Deleting temp directory "+FEED_DOWNLOAD_PATH);
		FileUtils.deleteDirectory(file);
		*/

		System.exit(0);
	}//endOfMain

	private static void importFeeds(RestTemplate importRestTemplate, HttpHeaders importheaders,
			HashMap<String, String> feedNameIdToPromote, Scanner input) {
		// TODO Auto-generated method stub
		
		String importConnectingReusableFlow = null;
		Iterator<Entry<String, String>> entriesSet6 = feedNameIdToPromote.entrySet().iterator();
		while(entriesSet6.hasNext())
		{
			Entry<String, String> entry = entriesSet6.next();
			System.out.println("Do you want to enable 'importConnectingReusableFlow' for feed: "+entry.getKey()+" ?(Enter YES/NO)");
			importConnectingReusableFlow = input.next().toUpperCase();
			System.out.println("Importing feed -> "+entry.getKey()+" started!");

			MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<String, Object>();
			bodyMap.add("categorySystemName", null);
			bodyMap.add("templateProperties", null);
			bodyMap.add("overwrite", true);
			bodyMap.add("overwriteFeedTemplate", true);
			bodyMap.add("importConnectingReusableFlow", importConnectingReusableFlow);
			bodyMap.add("feedProperties", null);
	    	bodyMap.add("file", new FileSystemResource(new File(FEED_DOWNLOAD_PATH.concat("\\").concat(entry.getKey()).concat(".feed.zip"))));
	    	
	        HttpEntity<MultiValueMap<String, Object>> importEntity = new HttpEntity<MultiValueMap<String,Object>>(bodyMap, importheaders);
	        ResponseEntity<String> importResponse = importRestTemplate.exchange(TARGET_IMPORT_FEED_URL, HttpMethod.POST,
	        		importEntity, String.class);
	        System.out.println("Status code for this REST POST http response: "+importResponse.getStatusCode());
	        String resp = importResponse.getBody();
			JSONParser parseimp = new JSONParser();
			Boolean Validimp = false;
			Boolean Successimp = false;
			//Type caste the parsed json data to json object
			JSONObject jobjimp = null;
			try {
				jobjimp = (JSONObject) parseimp.parse(resp);
				Validimp = (Boolean) jobjimp.get("valid");
				//System.out.println(Validimp);
				Successimp = (Boolean) jobjimp.get("success");
				//System.out.println(Successimp);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        if(Validimp==true && Successimp==true)
	        {
	    		System.out.println(entry.getKey()+" FEED IMPORT TO TARGET SUCCESSFUL!!!!!!!!!!!!!!!!!!!!!!!!!");
	        }
	        else
	        {
	        	System.out.println(entry.getKey()+" could not be imported to target");
	        }
	        importConnectingReusableFlow = null;
	        System.out.println();
		}
		
	}

	private static void exportFeeds(RestTemplate exportRestTemplate, HttpEntity<String> exportEntity, HashMap<String, String> feedNameIdToPromote) {
		// TODO Auto-generated method stub
		Iterator<Entry<String, String>> entriesSet5 = feedNameIdToPromote.entrySet().iterator();
		while(entriesSet5.hasNext())
		{
			
			Entry<String, String> entry = entriesSet5.next();
			System.out.println("Exporting feed -> "+entry.getKey()+" started!");
			ResponseEntity<byte[]> exportResponse = exportRestTemplate.
					exchange(SOURCE_EXPORT_FEED_URL.concat("/").concat(entry.getValue()), HttpMethod.GET,
					exportEntity, byte[].class);
			
			System.out.println("**** Status code of response for export url : "+exportResponse.getStatusCode());			
/*			System.out.println("**** Json Headers of response for export : "+exportResponse.getHeaders());
*/						
			FileOutputStream fileOutputStream = null;
			try {
				fileOutputStream = new FileOutputStream(FEED_DOWNLOAD_PATH.concat("\\").concat(entry.getKey())
						.concat(".feed.zip"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("Zip folder containing feed related files created at location : "+FEED_DOWNLOAD_PATH.concat("\\"));
			try {
				org.apache.commons.io.IOUtils.write(exportResponse.getBody(),fileOutputStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				fileOutputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Exporting feed -> "+entry.getKey()+" completed!");
			System.out.println();
		}
		
		
	}

	private static void acceptInputFromUser(long sourceRecordsTotal, int[] feednumbers,
			HashMap<Integer, String> feedIDs, HashMap<Integer, String> feedNames,
			HashMap<String, String> feedNameIdToPromote, Scanner input) {
		// TODO Auto-generated method stub
		System.out.println("Do you want to promote all feeds ? (Enter YES/NO)");
		String promoteAll = input.next().toLowerCase();
		if(promoteAll.equals("no"))
		{
			System.out.println("Enter the total no of feeds that you want to promote");
			int totalFeedsToPromote = input.nextInt();
	
			if(totalFeedsToPromote > sourceRecordsTotal)
			{
				System.out.println(" User specifies no of feeds greater than the source system has");
				System.exit(0);
			}
			else
			{
				feednumbers = new int[totalFeedsToPromote];
				System.out.println("Enter the feednumber(s) (one number per line) that needs to be promoted  ");  
				for(int i=0;i<feednumbers.length;i++)
				{
					feednumbers[i] = input.nextInt();
				}
				System.out.println("feednumbers that will be promoted :");
				for(int i=0;i<feednumbers.length;i++)
				{
					System.out.println(feednumbers[i]);
				}
				
			}	
			
			//Validation: add only matching feedNameIdValues to map object
			for(int i=0;i<feednumbers.length;i++)
			{
				Iterator<Entry<Integer, String>> entriesSet3 = feedIDs.entrySet().iterator();
				Iterator<Entry<Integer, String>> entriesSet4 = feedNames.entrySet().iterator();

				while(entriesSet3.hasNext() && entriesSet4.hasNext())
				{
					Entry<Integer, String> entryId = entriesSet3.next();
					Entry<Integer, String> entryName = entriesSet4.next();
					if(feednumbers[i]==entryId.getKey() && feednumbers[i]==entryName.getKey())
					{
						feedNameIdToPromote.put(entryName.getValue(), entryId.getValue());
					}
				}
			}
			System.out.println("feedNameId that will be promoted :");
			System.out.println(feedNameIdToPromote);
		}
		else
		{
			feednumbers = new int[(int) sourceRecordsTotal];
			for(int i=0;i<feednumbers.length;i++)
			{
				feednumbers[i]=i+1;
			}
			
			System.out.println("feednumbers that will be promoted :");
			for(int i=0;i<feednumbers.length;i++)
			{
				System.out.println(feednumbers[i]);
			}
			//add all feedids to list
				{
					feedNameIdToPromote.putAll(feedNameIdValues);
				}
				System.out.println("feedNameId that will be promoted :");
				System.out.println(feedNameIdToPromote);
		}
		
	}

	private static void methodToParse(String responseString, long sourceRecordsTotal2,
			HashMap<Integer, String> feedNames2, HashMap<Integer, String> feedIDs2,
			HashMap<String, String> feedNameIdValues2) {
		// TODO Auto-generated method stub
		//JSONParser reads the data from string object and break each data into key value pairs
				JSONParser parse = new JSONParser();
		    
				//Type caste the parsed json data to json object
				JSONObject jobj = null;
				try {
					jobj = (JSONObject) parse.parse(responseString);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    
				System.out.println("***************** Total no of existing feeds in source environment : *************************");
				System.out.println(jobj.get("recordsTotal"));
				sourceRecordsTotal = (long) jobj.get("recordsTotal");
				System.out.println();

				JSONArray jsonFeedsArray = (JSONArray) jobj.get("data");
				System.out.println();

				//iterating over "data" array of values
				int x=1;
				for(int i=0; i<jsonFeedsArray.size();i++)
				{
				
					//Getting set of values under "data" header
					JSONObject jsonFeedObject = (JSONObject)jsonFeedsArray.get(i);
				
					//Storing the feednames & feedids to 2 map objects
					feedNames.put(x,(String)jsonFeedObject.get("systemFeedName"));
					feedIDs.put(x, (String)jsonFeedObject.get("feedId"));
					feedNameIdValues.put((String)jsonFeedObject.get("systemFeedName"), (String)jsonFeedObject.get("feedId"));
					x++;
				}
				System.out.println();

		
	}


	private static String methodToGetFeeds(RestTemplate restTemplate, HttpEntity<String> entity) 
	{
		//passing kylo "get feeds" API uri
		ResponseEntity<String> response = restTemplate.exchange(SOURCE_GET_FEEDS_URL,
	    		HttpMethod.GET, entity, String.class);
		System.out.println("********************* Displaying the status of the response from source get feeds url : *************************");
		System.out.println("******************* Response Satus Code: "+response.getStatusCode());
		String responseString = response.getBody().toString(); 
		System.out.println();
		return responseString;
	}
	
}//endOfCLass

