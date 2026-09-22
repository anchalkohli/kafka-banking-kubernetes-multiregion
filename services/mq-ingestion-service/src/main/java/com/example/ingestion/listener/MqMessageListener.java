package com.example.ingestion.listener;

import com.example.ingestion.service.IngestionService;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component
public class MqMessageListener {
    private static final Logger log = LoggerFactory.getLogger(MqMessageListener.class);
    private final IngestionService ingestionService;

    public MqMessageListener(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @JmsListener(destination = "${ibm.mq.queue}", containerFactory = "jmsListenerContainerFactory")
    public void onMessage(Message message) throws JMSException {
        if (!(message instanceof TextMessage textMessage)) {
            throw new IllegalArgumentException("Unsupported JMS message type: " + message.getClass().getName());
        }

        String sourceMessageId = message.getJMSMessageID();
        String correlationId = message.getJMSCorrelationID();
        log.info("Received MQ message id={} correlationId={}", sourceMessageId, correlationId);
        ingestionService.processAndSendToKafka(textMessage.getText(), sourceMessageId, correlationId);
    }
}
