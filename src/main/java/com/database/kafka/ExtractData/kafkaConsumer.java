package com.database.kafka.ExtractData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;    
import java.util.List;    
import java.util.Map;    
import java.util.Properties;    
  
import kafka.consumer.Consumer;    
import kafka.consumer.ConsumerConfig;    
import kafka.consumer.ConsumerIterator;    
import kafka.consumer.KafkaStream;    
import kafka.javaapi.consumer.ConsumerConnector;    
import com.database.kafka.ExtractData.Config.Topics;

public class kafkaConsumer extends Thread{    
  
    private Topics type;

    public kafkaConsumer(Topics type){    
        super();    
        this.type = type;  
    }    
    @Override    
    public void run() {
    	 Connection conn = null;
         Statement stmt = null;
         try{
             Class.forName("com.mysql.jdbc.Driver");
             conn = DriverManager.getConnection(Config.DB_URL,Config.USER,Config.PASS);
             
             PreparedStatement pstmt = conn.prepareStatement(type.insertSQL);  
             stmt = conn.createStatement();
             ConsumerConnector consumer = createConsumer();    
             Map<String, Integer> topicCountMap = new HashMap<String, Integer>();    
             topicCountMap.put(type.topicName, 1); // 一次从主题中获取一个数据    
	         Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);    
	         KafkaStream<byte[], byte[]> stream = messageStreams.get(type.topicName).get(0);// 获取每次接收到的这个数据    
	         ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();    
 	         while(iterator.hasNext()){    
	              String message = new String(iterator.next().message());
//	              System.out.println("接收到: " + message);
	              String[] info = message.split("\001");
	              for (int i = 0; i < info.length; i++) {  
	                  pstmt.setString(i + 1,  info[i]);  
	              }  
	              pstmt.executeUpdate();
	         }    
 	         
             stmt.close();
             conn.close();
         }catch(SQLException se){
             // 处理 JDBC 错误
             se.printStackTrace();
         }catch(Exception e){
             // 处理 Class.forName 错误
             e.printStackTrace();
         }finally{
             // 关闭资源
             try{
                 if(stmt!=null) stmt.close();
             }catch(SQLException se2){
             }// 什么都不做
             try{
                 if(conn!=null) conn.close();
             }catch(SQLException se){
                 se.printStackTrace();
             }
         }
    }    
    
    private ConsumerConnector createConsumer() {    
        Properties properties = new Properties();    
        properties.put("zookeeper.connect", "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181");//声明zk    
        properties.put("group.id", "group2");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));    
     }    
        
    public static void main(String[] args) {    
//        new kafkaConsumer(Config.Topics.ARTICLEINFO).start();
//        new kafkaConsumer(Config.Topics.ARTICLEINFO).start(); 
//        new kafkaConsumer(Config.Topics.ARTICLEINFO).start();
        
//        new kafkaConsumer(Config.Topics.USERBASIC).start();
//        new kafkaConsumer(Config.Topics.USEREDU).start(); 
//        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start();
//        
//        new kafkaConsumer(Config.Topics.USERBASIC).start();
//        new kafkaConsumer(Config.Topics.USEREDU).start(); 
//        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start(); 
//        
//        new kafkaConsumer(Config.Topics.USERBASIC).start();
//        new kafkaConsumer(Config.Topics.USEREDU).start(); 
//        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start(); 
        
        new kafkaConsumer(Config.Topics.ARTICLEINFO).start();
        new kafkaConsumer(Config.Topics.ARTICLEINFO).start(); 
        new kafkaConsumer(Config.Topics.ARTICLEINFO).start(); 
        
    }         
}  