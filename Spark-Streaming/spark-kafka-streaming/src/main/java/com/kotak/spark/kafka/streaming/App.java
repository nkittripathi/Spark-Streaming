package com.bank.spark.kafka.streaming;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Spark-Kafka Streaming application
 *
 */
public class App 
{	
	private static final Logger log = LoggerFactory.getLogger(App.class);	
	
    public static void main( String[] args )   
    {
    	 if (log.isInfoEnabled()) {
    		 log.info("Running Spark-Kafka Streaming with the following command line args (comma separated):{}", StringUtils.join(args, ","));
       } 
        new App().run(System.out, args);        
    }
   
    private void run(@Nonnull final PrintStream out, @Nonnull final String... args) {
        if (args.length < 11) {
            String msg = "Proper Usage is: <bootstrap.servers> <zookeeper.connect> <topiclist> <group.id> <auto.offset.reset> <feedname> <category> <uuid> <baseurl> <username> <password>\n"  + StringUtils.join(args, ",");
            out.println(msg);
            throw new IllegalArgumentException(msg);
        }
        
        SparkConf sparkConf = new SparkConf().setAppName("Streaming Started");
        String boostrapServer = args[0];
        String zookeeper = args[1];
        String topiclist = args[2];
        String groupId = args[3];
        String autoOffsetReset = args[4];
        String feedname = args[5];
    		String category = args[6];
    		String uuid = args[7];
    		String baseurl = args[8];
    		String username = args[9];
    		String password = args[10];
    		
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", boostrapServer);
        kafkaParams.put("zookeeper.connect", zookeeper);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", autoOffsetReset);      
        HashSet<String> topics = new HashSet<String>(Arrays.asList(topiclist.split(",")));
       
        	JavaPairInputDStream<String, String>  stream =
          KafkaUtils.createDirectStream(
            streamingContext,
            String.class,
            String.class,
            StringDecoder.class,
            StringDecoder.class,
            kafkaParams,
            topics
          );
               	
        	String firstProcessorId = GetProcessorDetails.getFirstProcessorName(baseurl, username, password, feedname, category);
        	EnhanceStream enhanceStream = new EnhanceStream();
        	enhanceStream.enhanceRecord(stream, feedname, category, uuid, firstProcessorId);
       
        	streamingContext.start();
        	streamingContext.awaitTermination();
        
    }
}
