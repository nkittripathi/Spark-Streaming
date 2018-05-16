package com.bank.streaming;
 
 
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
 
import com.bank.streaming.UnicaMessage;
 
import org.apache.spark.sql.functions.* ;
 
import java.util.Arrays;
 
//import scala.collection.immutable.map;
 
import java.util.Collections;
 
 
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
 
import javax.net.ssl.HttpsURLConnection;
 
import com.databricks.spark.csv.*;
//import org.apache.spark.streaming.kafka.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
 
import  com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
 
import kafka.serializer.StringDecoder;
import scala.Tuple2;
 
 
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Proxy;
import java.net.URL;
 
public class TestDCCase {
               
               
                               
                public static class Switch_Message implements java.io.Serializable,Cloneable  {
 
                                private String cardNumber ;  //2
                                private String processingCode;  //3
                                private String transactionAmount ;  //4
                               
                                private String traceAuditNo ;   //11          
                                private String  localTransactionTime ;    //12         
                                private String  localTransactionDate;    //13          
                                private String  merchantCategoryCode;  //18
                                private String  acquiringInstitutionCountryCode; //19
                               
                                private String forwardingInstitutionId ;//33         
                                private String referenceNumber ;  //37
                                private String  responseCode ;  //39
                               
                                private String  cardAcceptorNameLocation ; //43
                               
                                private String  crnNo ;   //59
                                private String  cardType ;              //59
                                private String     reasonCode ;   //59
                                private String     txnIdentifier ;   //59
                               
                                private String  accountNumber;  //102
                                private String  channelIdentifier ;  //123
                                private String  availableBalance ; //125
                                               
                                public String getCardNumber() {
                                                return cardNumber;
                                }
                                public void setCardNumber(String cardNumber) {
                                                this.cardNumber = cardNumber;
                                }
                               
                                public String getProcessingCode() {
                                                return processingCode;
                                }
                                public void setProcessingCode(String processingCode) {
                                                this.processingCode = processingCode;
                                }
                               
                                public String getTransactionAmount() {
                                                return transactionAmount;
                                }
                                public void setTransactionAmount(String transactionAmount) {
                                                this.transactionAmount = transactionAmount;
                                }
 
                                public String getTraceAuditNo() {
                                                return traceAuditNo;
                                }
                                public void setTraceAuditNo(String traceAuditNo) {
                                                this.traceAuditNo = traceAuditNo;
                                }
                               
                                public String getLocalTransactionTime() {
                                                return localTransactionTime;
                                }
                                public void setLocalTransactionTime(String localTransactionTime) {
                                                this.localTransactionTime = localTransactionTime;
                                }
                               
                                public String getLocalTransactionDate() {
                                                return localTransactionDate;
                                }
                                public void setLocalTransactionDate(String localTransactionDate) {
                                                this.localTransactionDate = localTransactionDate;
                                }
                               
                                public String getMerchantCategoryCode() {
                                                return merchantCategoryCode;
                                }
                                public void setMerchantCategoryCode(String merchantCategoryCode) {
                                                this.merchantCategoryCode = merchantCategoryCode;
                                }
                               
                                public String getAcquiringInstitutionCountryCode() {
                                                return acquiringInstitutionCountryCode;
                                }             
                                public void setAcquiringInstitutionCountryCode(String acquiringInstitutionCountryCode) {
                                                this.acquiringInstitutionCountryCode = acquiringInstitutionCountryCode;
                                }
 
                                public String getForwardingInstitutionId() {
                                                return forwardingInstitutionId;
                                }
                                public void setForwardingInstitutionId(String forwardingInstitutionId) {
                                                this.forwardingInstitutionId = forwardingInstitutionId;
                                }
 
                                public String getReferenceNumber() {
                                                return referenceNumber;
                                }
                                public void setReferenceNumber(String referenceNumber) {
                                                this.referenceNumber = referenceNumber;
                                }
                               
                                public String getResponseCode() {
                                                return responseCode;
                                }
                                public void setResponseCode(String responseCode) {
                                                this.responseCode = responseCode;
                                }
                                                               
                                public String getCardAcceptorNameLocation() {
                                                return cardAcceptorNameLocation;
                                }
                                public void setCardAcceptorNameLocation(String cardAcceptorNameLocation) {
                                                this.cardAcceptorNameLocation = cardAcceptorNameLocation;
                                }
                               
                                public String getCrnNo() {
                                                return crnNo;
                                }
                                public void setCrnNo(String crnNo) {
                                                this.crnNo = crnNo;
                                }
                               
                                public String getCardType() {
                                                return cardType;
                                }
                                public void setCardType(String cardType) {
                                                this.cardType = cardType;
                                }
                               
                                public String getReasonCode() {
                                                return reasonCode;
                                }
                                public void setReasonCode(String reasonCode) {
                                                this.reasonCode = reasonCode;
                                }
                               
                                public String getTxnIdentifier() {
                                                return txnIdentifier;
                                }
                                public void setTxnIdentifier(String txnIdentifier) {
                                                this.txnIdentifier = txnIdentifier;
                                }
                               
                                public String getAccountNumber() {
                                                return accountNumber;
                                }
                                public void setAccountNumber(String accountNumber) {
                                                this.accountNumber = accountNumber;
                                }
                               
                                public String getChannelIdentifier() {
                                                return channelIdentifier;
                                }
                                public void setChannelIdentifier(String channelIdentifier) {
                                                this.channelIdentifier = channelIdentifier;
                                }
                               
                                public String getAvailableBalance() {
                                                return availableBalance;
                                }
                                public void setAvailableBalance(String availableBalance) {
                                                this.availableBalance = availableBalance;
                                }
                               
                                public String toString() {
                                                return  getCardNumber() + "," +  getProcessingCode() + "," +  getTransactionAmount() + "," +  getTraceAuditNo()  + "," +  getLocalTransactionTime() + "," +
                                                                                getLocalTransactionDate() + "," +  getMerchantCategoryCode()  + "," +  getAcquiringInstitutionCountryCode()  + "," + getForwardingInstitutionId() + "," + 
                                                                                getReferenceNumber() + "," +  getResponseCode() + "," +  getCardAcceptorNameLocation()  + "," +   getCrnNo() + "," +  getCardType() + "," + getReasonCode() + "," +
                                                                                getTxnIdentifier() + "," + getAccountNumber() + "," + getChannelIdentifier() + "," +  getAvailableBalance(); 
                                                               
                                }
                               
                                public Switch_Message clone() throws CloneNotSupportedException {
                                                Switch_Message clone = null;
                        clone = (Switch_Message) super.clone();      
                        return clone;      
                    }         
                                                               
                }
                               
               
@SuppressWarnings("deprecation")
public static void main(String[] args) {
               
        SparkConf conf = new SparkConf()
                .setAppName("kafka-reader2");
                //.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));
       
        final HiveContext hiveContext = new HiveContext(sc);
               
                Map<String, String> kafkaParams = new HashMap<>();
                kafkaParams.put("metadata.broker.list", "10.240.20.189:9092");
                Set<String> topics = new HashSet<String>();
                topics.add("test_dc");                  
               
 
//                             final HttpsURLConnection urlConnMail = urlConn;
//                            final HttpURLConnection urlConnSms = urlConn1;
//                           
//                final String requestUnica = request;
//                            final       String sendEncodingUnica = sendEncoding;
//                                            final OutputStream outMail = out;
//                                            final OutputStream outSms = out1;
//                                           
               
               
    class RefreshFrequency implements Serializable {
                                  int count = 0;
                                  DataFrame dcDataFrame = null;
                                  
                                RefreshFrequency() {
                                                dcDataFrame=loadData();
                                                count=0;
                                }
                   
                                public int getCount() { return count; }
                                  
                                public void updateCount() { count++;}
                                  
                                 public DataFrame loadData()  {                                                   
                                   if (count % 100 == 0) {                                                    
                                                   if ( dcDataFrame != null) dcDataFrame.unpersist();                                                      
                                                    dcDataFrame = hiveContext.table("test.dc") ;
                                                   // dcDataFrame = hiveContext.sql ("select * from test.dc") ;
                                                   // Object arr = new Object[1] ;
                                                      dcDataFrame.persist();
                                                   if ( dcDataFrame.rdd().first().length() <= 0) System.exit(0);                                           
                                                                dcDataFrame.registerTempTable("dcTable");         
                                                               
                                                  return dcDataFrame;  
                                   } else {
                                                 return dcDataFrame; 
                                   }
                                 }
                                 
     }
   
        
       final RefreshFrequency freq = new RefreshFrequency();
       
               
                JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
               
                JavaDStream<Switch_Message> isoMsgValues = directKafkaStream.map(
                                                new Function<Tuple2<String,String>,Switch_Message> () {                                             
                                                                                private static final long serialVersionUID = 1L;
                                                                                @Override public Switch_Message call(Tuple2<String,String> x) {
                                                                                                String jsonData = x._2();
                                                                 ObjectMapper objectMapper = new ObjectMapper();                                                          
                                                                Switch_Message isoMsg = null;
                                                                                                try {
                                                                                                                isoMsg = objectMapper.readValue(jsonData, Switch_Message.class);
                                                                                                                } catch (IOException e) {
                                                                                                                // TODO Auto-generated catch block
                                                                                                                e.printStackTrace();
                                                                                                }                                                                             
                                                                return isoMsg ;
                                                    }
                                                  });
               
                                               
       isoMsgValues.foreachRDD(  
                                   
                  new Function2<JavaRDD<Switch_Message>, Time, Void>() {
                    @Override                                                          
                    
                    public Void call(JavaRDD<Switch_Message> rowRDD, Time time) {
 
 
                      // Convert RDD[String] to RDD[case class] to DataFrame
                      JavaRDD<Switch_Message> rdd = rowRDD.map(new Function<Switch_Message, Switch_Message>() {
                        public Switch_Message call(Switch_Message msg) {
                                Switch_Message record = null;
                                                                                                try {
                                                                                                                record = msg.clone();
                                                                                                } catch (CloneNotSupportedException e) {
                                                                                                                // TODO Auto-generated catch block
                                                                                                                e.printStackTrace();
                                                                                                }
                         // record.setWord(word);
                          return record;
                        }
                      });
                               
                                                      
                      DataFrame fisDataFrame = hiveContext.createDataFrame(rdd, Switch_Message.class);
                      fisDataFrame.registerTempTable("fisData");
                                                                                                                                                  
                                  
                                  DataFrame dcDataFrame = freq.loadData();
                                  freq.updateCount();
                                  
                                  DataFrame df = hiveContext.sql("select fisData.*,dcTable.* from fisData left outer join dcTable on "
                                                + " fisData.crnNo = dcTable.crn ") ;
                    
                          df.rdd().first();                                          
                      String path = "/app/dc/";
                      
                   //   df.write().mode(SaveMode.Append).saveAsTable("default.fis_test");             
                                      
 
                     //csv queryfisDataFrame.toJavaRDD()
                      return null;
                    }
                  }
                );
            
        
        ssc.start();
        ssc.awaitTermination();
       
    }
               
 
}
