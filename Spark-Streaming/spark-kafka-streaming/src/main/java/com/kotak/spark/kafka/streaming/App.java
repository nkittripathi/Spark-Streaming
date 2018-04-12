package com.kotak.spark.kafka.streaming;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
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
import com.google.common.collect.Lists;


import javax.annotation.Nonnull;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final Logger log = LoggerFactory.getLogger(App.class);
	
    public static void main( String[] args )
    
    {
    	 if (log.isInfoEnabled()) {
             log.info("Running Spark Validator with the following command line args (comma separated):{}", StringUtils.join(args, ","));
         }
        System.out.println( "Hello World!" );
        
        new App().run(System.out, args);
        
    }
    
    
    private void run(@Nonnull final PrintStream out, @Nonnull final String... args) {
        // Check how many arguments were passed in
        if (args.length < 4) {
            String msg = "Proper Usage is: <bootstrap.servers> <zookeeper.connect> <topiclist> <group.id> <auto.offset.reset>\n"  + StringUtils.join(args, ",");
            out.println(msg);
            throw new IllegalArgumentException(msg);
        }
        
        SparkConf sparkConf = new SparkConf().setAppName("Streaming Started");
      
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        System.out.println( "bootstrap.servers" + args[0]);
        System.out.println( "zookeeper.connect"+ args[1]);
        System.out.println( "group.id" + args[3] );
        System.out.println( "auto.offset.reset" + args[4]);
        System.out.println( "topiclist" + args[2]);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", args[0]);
        kafkaParams.put("zookeeper.connect", args[1]);
        kafkaParams.put("group.id", args[3]);
        kafkaParams.put("auto.offset.reset", args[4]);
        String topiclist = args[2];
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
        	
        	GetTableData.enhanceData(stream);
        	streamingContext.start();
        	streamingContext.awaitTermination();
        
    }
}
