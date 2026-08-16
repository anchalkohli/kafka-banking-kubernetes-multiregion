package com.example.ingestion.config;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.support.destination.DynamicDestinationResolver;

@Configuration
public class JmsConfig {
    @Bean
    public ConnectionFactory connectionFactory(@Value("${ibm.mq.queue-manager}") String queueManager,
                                               @Value("${ibm.mq.channel}") String channel,
                                               @Value("${ibm.mq.conn-name}") String connName,
                                               @Value("${ibm.mq.user}") String user,
                                               @Value("${ibm.mq.password}") String password,
                                               @Value("${ibm.mq.ssl.enabled:false}") boolean sslEnabled,
                                               @Value("${ibm.mq.ssl.cipher-suite:}") String sslCipherSuite,
                                               @Value("${ibm.mq.ssl.peer-name:}") String sslPeerName,
                                               @Value("${ibm.mq.ssl.fips-required:false}") boolean sslFipsRequired) throws JMSException {
        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        factory.setQueueManager(queueManager);
        factory.setChannel(channel);
        factory.setConnectionNameList(connName);
        factory.setStringProperty(WMQConstants.USERID, user);
        factory.setStringProperty(WMQConstants.PASSWORD, password);

        if (sslEnabled) {
            if (sslCipherSuite == null || sslCipherSuite.isBlank()) {
                throw new IllegalStateException("IBM MQ TLS is enabled but ibm.mq.ssl.cipher-suite is not configured");
            }
            factory.setSSLCipherSuite(sslCipherSuite);
            factory.setSSLFipsRequired(sslFipsRequired);
            if (sslPeerName != null && !sslPeerName.isBlank()) {
                factory.setSSLPeerName(sslPeerName);
            }
        }
        return factory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory connectionFactory,
            @Value("${app.jms.concurrency:3-6}") String concurrency) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setDestinationResolver(new DynamicDestinationResolver());
        factory.setSessionTransacted(true);
        factory.setConcurrency(concurrency);
        factory.setRecoveryInterval(5000L);
        return factory;
    }
}
