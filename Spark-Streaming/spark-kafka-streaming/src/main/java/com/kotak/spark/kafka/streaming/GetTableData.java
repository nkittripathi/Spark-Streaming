package com.kotak.spark.kafka.streaming;

import java.io.Serializable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.json.JSONException;
import org.json.JSONObject;

import scala.Tuple2;

public class GetTableData implements Serializable {
	
	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static String sendGet()  {

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

	/* public static void enhanceData(JavaInputDStream<ConsumerRecord<String, String>> messages ) {
		   
		  
		   messages.map(new Function<ConsumerRecord<String, String>,String>() {

	            
				@Override
				public String call(ConsumerRecord<String, String> record) throws Exception {
					// TODO Auto-generated method stub
					String i  = sendGet();
					String j = (new JSONObject(record.value())).get("Ankit").toString();
					
					

					return i + j;
				}
	        }).print();
	   } */
	 
	 public static void enhanceData (JavaPairInputDStream<String, String> stream) {
		 
		 	JavaDStream<String> lines = stream.map(new Function<Tuple2<String, String>, String>() {
         /**
				 * 
				 */
				private static final long serialVersionUID = 1L;

		/**
			 * 
			 */
		

		@Override
         	public String call(Tuple2<String, String> tuple2) {
				try {
					
					String effectiveDate = (new JSONObject(tuple2._2())).get("effectiveDate").toString();
					String localTransactionDateTime = (new JSONObject(tuple2._2())).get("localTransactionDateTime").toString();
					String accountIdentification1 = (new JSONObject(tuple2._2())).get("accountIdentification1").toString();
					String primaryAccountNumber = (new JSONObject(tuple2._2())).get("primaryAccountNumber").toString();
					String actionCode = (new JSONObject(tuple2._2())).get("actionCode").toString();
					String transactionAmount = (new JSONObject(tuple2._2())).get("transactionAmount").toString();
					String cardAcceptor = (new JSONObject(tuple2._2())).get("cardAcceptor").toString();
					String acquiringInstitutionCountryCode = (new JSONObject(tuple2._2())).get("acquiringInstitutionCountryCode").toString();
					String reservedForPrivateUse1 = (new JSONObject(tuple2._2())).get("reservedForPrivateUse1").toString();
					return effectiveDate + " "+ localTransactionDateTime + " "+ accountIdentification1 + " "+ primaryAccountNumber + " "+ actionCode + " "+ transactionAmount + " "+ cardAcceptor + " "+ acquiringInstitutionCountryCode + " "+ reservedForPrivateUse1 + " "+ " Additional Message " +sendGet();
				}
				catch (JSONException e) {
					
					return "This is invalid JSON";
					
				}
         }
       });
		 	lines.print();
	 }
	   

}
