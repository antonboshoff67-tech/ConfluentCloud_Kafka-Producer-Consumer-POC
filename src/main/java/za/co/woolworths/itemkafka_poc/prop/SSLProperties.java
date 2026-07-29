package za.co.woolworths.itemkafka_poc.prop;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@ToString
@Component
public class SSLProperties {

    @Getter
    @Setter
    private String ciphers;

    @Getter
    @Setter
    private String keyStoreType;

    @Getter
    @Setter
    @Value("${keys.ssl.keyStore}")
    private String keyStore;

    @Getter
    @Setter
    @Value("${keys.ssl.keyStorePassword}")
    private String keyStorePassword;

    @Getter
    @Setter
    @Value("${keys.ssl.trustStore}")
    private String trustStore = null;

    @Getter
    @Setter
    @Value("${keys.ssl.trustStorePassword}")
    private String trustStorePassword = null;
}
