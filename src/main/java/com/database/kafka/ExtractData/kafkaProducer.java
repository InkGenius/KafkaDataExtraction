package com.database.kafka.ExtractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.database.kafka.ExtractData.Config.Topics;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class kafkaProducer extends Thread {

    private Topics type;

    public kafkaProducer(Topics type){    
        super();    
        this.type = type;  
    } 
	@Override
	public void run() {
		Producer producer = createProducer();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		File file = new File("./dataset/" + type.topicName + "/" + type.topicName);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				producer.send(new KeyedMessage<Integer, String>(type.topicName,tempString));
	            System.out.println(df.format(new Date()) + "  " + tempString);
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		producer.close();
	}

	private Producer createProducer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181");// 声明zk
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "192.168.56.121:9092,192.168.56.122:9092,192.168.56.123:9092");// 声明kafka  broker
		return new Producer<Integer, String>(new ProducerConfig(properties));
	}

	public static void main(String[] args) {
		new kafkaProducer(Config.Topics.ARTICLEINFO).start();// 使用kafka集群中创建好的主题 test
	}

}