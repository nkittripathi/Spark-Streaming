package com.kotak.spark.kafka.streaming;


import java.io.Serializable;

import javax.jms.JMSException;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kotak.spark.eventmonitor.ActiveMQWriter;
import com.kotak.spark.eventmonitor.ProvenanceGenerator;

import java.util.UUID;

import scala.Tuple2;

public class EnhanceStream implements Serializable {
	
	private static final Logger log = LoggerFactory.getLogger(EnhanceStream.class);	
	private static final long serialVersionUID = 1L;
	private static String activeMQHost = "tcp://localhost:61616";
	ActiveMQWriter writer = new ActiveMQWriter(activeMQHost);	

	public static String enhanceRecordDetail()  {
			
			log.info("Sending enhance record request");
			/* String url = "http://localhost:8079/emp?empid=1";
			
			URL obj = new URL(url);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();

			// optional default is GET
			con.setRequestMethod("GET");

			//add request header
			//con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
			System.out.println("\nSending 'GET' request to URL : " + url);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			//print result
			System.out.println("Response is " + response.toString()); */
	    	   	String response = "Hello  ";
			return response.toString();

		}
	 
	 public void enhanceRecord (JavaPairInputDStream<String, String> stream, String feedname, String category, String uuid, String firstProcessorID) {
		 		
		 	JavaDStream<String> lines = stream.map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1L;
	
			@Override
         	public String call(Tuple2<String, String> tuple2) {
				try {
					
		        	Long startTime = System.currentTimeMillis();
		        	String[] newUUID = {ProvenanceGenerator.generateNewPseudoFlowFile().toString()};
		        	String flowFileID = UUID.randomUUID().toString();
		        long inputBytes = tuple2._2().length();		
				String effectiveDate = (new JSONObject(tuple2._2())).get("effectiveDate").toString();
				String localTransactionDateTime = (new JSONObject(tuple2._2())).get("localTransactionDateTime").toString();
				String accountIdentification1 = (new JSONObject(tuple2._2())).get("accountIdentification1").toString();
				String primaryAccountNumber = (new JSONObject(tuple2._2())).get("primaryAccountNumber").toString();
				String actionCode = (new JSONObject(tuple2._2())).get("actionCode").toString();
				String transactionAmount = (new JSONObject(tuple2._2())).get("transactionAmount").toString();
				String cardAcceptor = (new JSONObject(tuple2._2())).get("cardAcceptor").toString();
				String acquiringInstitutionCountryCode = (new JSONObject(tuple2._2())).get("acquiringInstitutionCountryCode").toString();
				String reservedForPrivateUse1 = (new JSONObject(tuple2._2())).get("reservedForPrivateUse1").toString();
				String enhanceDetail = enhanceRecordDetail();
				String enhancedRecord = effectiveDate + " "+ localTransactionDateTime + " "+ accountIdentification1 + " "+ primaryAccountNumber + " "+ actionCode + " "+ transactionAmount + " "+ cardAcceptor + " "+ acquiringInstitutionCountryCode + " "+ reservedForPrivateUse1 + " "+ " Additional Message " + enhanceDetail;
				
				log.info("Sending enriched record to Unica");
				
				ProvenanceGenerator generator = new ProvenanceGenerator("ExecuteSparkStreamingJob", "Spark Streaming", ComponentIdGenerator.generateId().toString());			
			    try {
					writer.connect();
				} catch (JMSException e1) {
					e1.printStackTrace();
				} 
			     
			    	generator.registerStreamingProvenance(firstProcessorID, category + "."+ feedname, flowFileID, "spark", startTime, System.currentTimeMillis(), newUUID, true, true, false);
		        generator.registerStatistic(firstProcessorID, startTime, System.currentTimeMillis(), (long) inputBytes, (long) enhancedRecord.length());
		            
		        try {
		            	generator.submitProvenance(writer);
		        } catch (JMSException e) {
		            	e.printStackTrace();
		        }
		        
		        generator.clearProvenance();  
				return enhancedRecord;
				}
				catch (JSONException e) {		
					return "This is invalid JSON";					
				}
         }
       });	
		 	lines.print();
	 }
}
