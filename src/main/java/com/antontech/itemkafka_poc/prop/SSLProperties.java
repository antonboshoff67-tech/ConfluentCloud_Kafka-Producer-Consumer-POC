package com.antontech.itemkafka_poc.prop;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Binds the {@code keys.ssl.*} configuration used to build a mutual-TLS
 * {@link org.springframework.web.client.RestTemplate} for calling the
 * downstream gateway (see {@code MsgRoutingServiceImpl#getGatewaySSLTemplateConfig}).
 * <p>
 * Values are environment-variable driven: {@code ITEM_SSL_KEYSTORE},
 * {@code ITEM_SSL_KEYSTORE_PASSWORD}, {@code ITEM_SSL_TRUSTSTORE},
 * {@code ITEM_SSL_TRUSTSTORE_PASSWORD}. See {@code SETUP_GUIDE.md} for how to
 * generate/obtain a keystore and configure these for your own environment.
 */
@ToString
@Component
public class SSLProperties {

    /** Cipher suite override (optional, not currently applied automatically). */
    @Getter
    @Setter
    private String ciphers;

    /** Keystore type, e.g. {@code JKS} or {@code PKCS12} (optional). */
    @Getter
    @Setter
    private String keyStoreType;

    /** Absolute path to the client keystore file used for mutual TLS. */
    @Getter
    @Setter
    @Value("${keys.ssl.keyStore}")
    private String keyStore;

    /** Password protecting the keystore file above. */
    @Getter
    @Setter
    @Value("${keys.ssl.keyStorePassword}")
    private String keyStorePassword;

    /** Absolute path to the truststore file containing trusted CA certificates. */
    @Getter
    @Setter
    @Value("${keys.ssl.trustStore}")
    private String trustStore = null;

    /** Password protecting the truststore file above. */
    @Getter
    @Setter
    @Value("${keys.ssl.trustStorePassword}")
    private String trustStorePassword = null;
}



