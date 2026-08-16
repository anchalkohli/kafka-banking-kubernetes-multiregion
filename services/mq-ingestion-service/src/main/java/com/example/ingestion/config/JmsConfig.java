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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

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
                                               @Value("${ibm.mq.ssl.fips-required:false}") boolean sslFipsRequired,
                                               @Value("${ibm.mq.ssl.trust-store-location:}") String trustStoreLocation,
                                               @Value("${ibm.mq.ssl.trust-store-password:}") String trustStorePassword,
                                               @Value("${ibm.mq.ssl.trust-store-type:PKCS12}") String trustStoreType,
                                               @Value("${ibm.mq.ssl.key-store-location:}") String keyStoreLocation,
                                               @Value("${ibm.mq.ssl.key-store-password:}") String keyStorePassword,
                                               @Value("${ibm.mq.ssl.key-store-type:PKCS12}") String keyStoreType) throws JMSException {
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
            if (trustStoreLocation == null || trustStoreLocation.isBlank()) {
                throw new IllegalStateException("IBM MQ TLS is enabled but ibm.mq.ssl.trust-store-location is not configured");
            }

            factory.setSSLCipherSuite(sslCipherSuite);
            factory.setSSLFipsRequired(sslFipsRequired);
            if (sslPeerName != null && !sslPeerName.isBlank()) {
                factory.setSSLPeerName(sslPeerName);
            }
            factory.setSSLSocketFactory(buildSslContext(
                    trustStoreLocation,
                    trustStorePassword,
                    trustStoreType,
                    keyStoreLocation,
                    keyStorePassword,
                    keyStoreType).getSocketFactory());
        }
        return factory;
    }

    private SSLContext buildSslContext(String trustStoreLocation,
                                       String trustStorePassword,
                                       String trustStoreType,
                                       String keyStoreLocation,
                                       String keyStorePassword,
                                       String keyStoreType) {
        try {
            KeyStore trustStore = KeyStore.getInstance(trustStoreType);
            try (FileInputStream input = new FileInputStream(trustStoreLocation)) {
                trustStore.load(input, trustStorePassword.toCharArray());
            }

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            KeyManagerFactory keyManagerFactory = null;
            if (keyStoreLocation != null && !keyStoreLocation.isBlank()) {
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (FileInputStream input = new FileInputStream(keyStoreLocation)) {
                    keyStore.load(input, keyStorePassword.toCharArray());
                }
                keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(
                    keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(),
                    trustManagerFactory.getTrustManagers(),
                    null);
            return sslContext;
        } catch (GeneralSecurityException | IOException ex) {
            throw new IllegalStateException("Unable to initialize IBM MQ TLS context", ex);
        }
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
