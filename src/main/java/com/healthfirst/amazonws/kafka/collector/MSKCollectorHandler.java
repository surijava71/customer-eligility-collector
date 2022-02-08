package com.healthfirst.amazonws.kafka.collector;

import java.util.Base64;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent.KafkaEventRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.healthfirst.amazonws.kafka.collector.producer.Producer;

public class MSKCollectorHandler implements RequestHandler<KafkaEvent, String> {
	
	private static final Logger log = LogManager.getLogger(MSKCollectorHandler.class);
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	String customer_eligibility_update_topic = "ods-customer-eligibilityupdate";
	Producer prod = null;

	public MSKCollectorHandler() {
		prod = new Producer();
	}

	public String handleRequest(KafkaEvent kafkaEvent, Context context) {
		
		String response = "200 SUCCESS";
		/**
		 * topic name from the event is the event key
		 */
		String eventKey = null;
		List<KafkaEventRecord> kafkaEventRecords = null;
		
		log.info("In Collector handleRequest - Kafka Event: \n", kafkaEvent.toString());
		log.info("In Collector handleRequest - Kafka Event Json: {} \n", gson.toJson(kafkaEvent));
		
		if (kafkaEvent.getRecords().keySet() != null)
			eventKey = kafkaEvent.getRecords().keySet().stream().findFirst().get();
		if (eventKey == null)
			return "400 Event Key is NULL - Stream doesn't have topic name to read events";

		log.info("In Collector handleRequest - Source Topic :: " + eventKey);
		
		/*
		 * Read the records for the topic
		 */
		kafkaEventRecords = kafkaEvent.getRecords().get(eventKey);
		
		for (KafkaEventRecord kafkaEventRecord : kafkaEventRecords) {
			byte[] decodedBytes = Base64.getDecoder().decode(kafkaEventRecord.getValue());
			String decodedString = new String(decodedBytes);
			
			log.info("In Collector handleRequest - Decoded Event Message: \n", decodedString);

			JsonObject json = null;
			
			try {
				json = new Gson().fromJson(decodedString, JsonObject.class);
				log.info("In Collector handleRequest - Json Message : {} \n", json);
			} catch (Exception e) {
				log.error(e.getMessage());
				return "400 Message formate Error";
			}

			try {
				prod.publish(customer_eligibility_update_topic, decodedString);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return response;
	}
}