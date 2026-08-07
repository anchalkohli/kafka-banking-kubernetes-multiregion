package com.example.ingestion.listener;

import com.example.ingestion.service.IngestionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component
public class MqMessageListener {
    private static final Logger log = LoggerFactory.getLogger(MqMessageListener.class);
    private final IngestionService ingestionService;

    public MqMessageListener(IngestionService ingestionService) { this.ingestionService = ingestionService; }

    @JmsListener(destination = "${ibm.mq.queue}", containerFactory = "jmsListenerContainerFactory")
    public void onMessage(String message) {
        log.info("Received MQ message");
        ingestionService.processAndSendToKafka(message);
    }
}
