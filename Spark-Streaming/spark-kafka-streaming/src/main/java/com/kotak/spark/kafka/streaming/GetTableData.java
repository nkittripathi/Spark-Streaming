package com.bank.spark.kafka.streaming;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.jms.JMSException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.json.JSONException;
import org.json.JSONObject;
import com.thinkbiganalytics.nifi.provenance.model.stats.AggregatedFeedProcessorStatisticsV2;
import com.kotak.spark.eventmonitor.ActiveMQWriter;
import com.kotak.spark.eventmonitor.ProvenanceGenerator;
import com.kotak.streaming.provenance.ProvenanceServiceFactory;
//import com.kotak.spark.eventmonitor.ActiveMQWriter;
//import com.kotak.spark.eventmonitor.ProvenanceGenerator;
import com.kotak.streaming.provenance.SparkProvenance;
import com.kotak.streaming.provenance.SparkProvenanceConfiguration;
import com.thinkbiganalytics.nifi.provenance.model.ProvenanceEventRecordDTO;
import com.thinkbiganalytics.nifi.provenance.model.util.ProvenanceEventDtoBuilder;
import com.thinkbiganalytics.provenance.api.ProvenanceEventService;

import com.kotak.spark.kafka.streaming.GenerateFlowFile;

import java.util.UUID;

import scala.Tuple2;

public class GetTableData implements Serializable {
	
	 /**
	 * 
	 */
	GetTableData() {
		
	}
	private static final long serialVersionUID = 1L;
	private static String activeMQHost = "tcp://localhost:61616";
	 ActiveMQWriter writer = new ActiveMQWriter(activeMQHost);	
	

	
	
	public static Properties setProducerConf() {
	Properties props = new Properties();
	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "director-06f3b8ae-ebdd-4daa-b922-26ef351da020.c.sincere-muse-174610.internal:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	
	return props;
	}
	
	/*public static KafkaProducer createKafkaProduce(Properties props) {
		//KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
		return (new KafkaProducer<>(props));
	} */
	
	

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
	 
	 public void enhanceData (JavaPairInputDStream<String, String> stream, String feed, String uuid, String uuidjob, String category) {
		 
		
		 	JavaDStream<String> lines = stream.map(new Function<Tuple2<String, String>, String>() {
         /**
				 * 
				 */
				private static final long serialVersionUID = 1L;
	
		@Override
         	public String call(Tuple2<String, String> tuple2) {
				try {
					
		        	Long startTime = System.currentTimeMillis();
		        	String firstProcessorID = "2e31bd93-7d88-3b58-705b-e2a6d91c9752";
		        	String[] newUUID = {ProvenanceGenerator.generateNewPseudoFlowFile().toString()};
		        	String feedName = feed;
		        	String flowFileID = UUID.randomUUID().toString();
		       // 	String category = "streaming";
		        long inputBytes = 10;
		        Object object = "My Object";
		        ProvenanceGenerator generator = new ProvenanceGenerator("App", "Spark Streaming", ComponentIdGenerator.generateId().toString());
				
				 try {
					writer.connect();
				} catch (JMSException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} 
		     
		    	generator.registerStreamingProvenance(firstProcessorID, category + "."+ feedName, flowFileID, "spark", startTime,
	                    System.currentTimeMillis(), newUUID, true, true, false);

	            generator.registerStatistic(firstProcessorID, startTime, System.currentTimeMillis(), (long) inputBytes, (long) object.toString().length());
	            
	            try {
            		System.out.print("##### Before submitProvenance");
				generator.submitProvenance(writer);
				System.out.print("##### After submitProvenance");
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            generator.clearProvenance(); 

		        System.out.println("##### Before registerStreamingProvenance ");
					
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
	 
	/* public void run(@Nonnull final PrintStream out, @Nonnull final String[] args) {
	    	
 		System.out.println("##### Inside Run");
 		//log.info("##### Inside Run");
     // Check how many arguments were passed in
     if (args.length < 5) {
     //	log.info("##### Inside Run in if");
         String msg = "Proper Usage is: <flowfile-id> <job-flowfile-id> <feed-name (category.feed)> <connection-url (url to connect to JMS or KAFAK)> <type (JMS, KAFKA)>" +
                      "You provided " + args.length + " args which are (comma separated): " + StringUtils.join(args, ",");
         out.println(msg);
         throw new IllegalArgumentException(msg);
     }

     ProvenanceEventService provenanceEventService = null;
    // final SparkContext sparkContext = SparkContext.getOrCreate();

     try {
     	System.out.println("##### Inside Run after try");
     	//log.info("##### Inside Run after try");
         final SparkProvenanceConfiguration params = new SparkProvenanceConfiguration(args);

         //Get the proper ProvenanceService
         provenanceEventService = ProvenanceServiceFactory.getProvenanceEventService(params);

         //Collection of custom Provenance Events we will be sending to Kylo
         List<ProvenanceEventRecordDTO> events = new ArrayList<>();

         //do some work.  Look up the database names in Hive
         //final HiveContext hiveContext = new HiveContext(sparkContext);

         //Do some work... i.e. look up the Databases in Hive
         ProvenanceEventRecordDTO event = newEvent("Spark-Kafka",params);
         //Dataset df = hiveContext.sql("show databases");
         //event.getAttributeMap().put("databases", df.toJSON().collectAsList().toString());
         
         event.setEventTime(System.currentTimeMillis());
         event.setStream(true);
         events.add(event);
        

         event = newEvent("Another Step",params);
         event.getAttributeMap().put("UUID 1", UUID.randomUUID().toString());
         event.setEventTime(System.currentTimeMillis());
         event.getAttributeMap().put("timestamp", String.valueOf(System.currentTimeMillis()));
         events.add(event);

         //Send the events off
         provenanceEventService.sendEvents(events);

         //log.info("Spark app finished");
     } catch (Exception e) {
       //  log.error("Failed to run Spark Provenance Job: {}", e.toString(), e);

     } finally {
         provenanceEventService.closeConnection();
         //sparkContext.stop();
        // log.info("Exiting!!!!");
        // System.exit(0);

     }
 }


	 /**
	     * Build a new Provenance Event
	     * @param componentName
	     * @param startingEvent
	     * @param params
	     * @return
	     */
	/*    private static ProvenanceEventRecordDTO newEvent(String componentName,SparkProvenanceConfiguration params){

	        return new ProvenanceEventDtoBuilder(params.getFeedName(),params.getFlowFileId(),componentName)
	            .jobFlowFileId(params.getJobFlowFileId())
	            .startTime(System.currentTimeMillis())
	            .startingEvent(false)
	            .build();
	    } */
	   

}

