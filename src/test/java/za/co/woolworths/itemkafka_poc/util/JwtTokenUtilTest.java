package za.co.woolworths.itemkafka_poc.util;

import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.junit.jupiter.api.Test;
import java.security.Key;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyPair;
import java.security.Security;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
class JwtTokenUtilTest {

  private String privateKeyString = "MIIEogIBAAKCAQEAkcMgHHPzLwiIYg7raoyR6QXdgbNJwou9Tah1/3qTEgw3bV6PrpqSL3PdrOBaiCa6cGwNr3i4ZGWh2B8iQuYcUVxsuImH9+uu6X/K0uE0avKrGj5e1Xawxb+VxuhHXkngD/LrRqryNCBAIJ3Cf7f8WdNJtUvSPO/FcU8EeYlmTOY9MUIeXptIupCwQZ/7jawpiXj/lau8lf67BrDYdiqH/gjwAja//LiqElWF7Ud6aNUbLp0ETj2nhnqvk2pWSfye3H4m5vVlqeKAeuVtPQBIrccCKfZ6AAfnfIhWFlnxkwEVsLtT2iDeZ9QZQcyiMudjSE8OE8pTo/9k55ux+09n8wIDAQABAoIBABQCYbOWNTFlLfesDaQUROlknTozRvJWZXe2IvCdvnexRqCUZQv2ew/Zi3oyWCstidVp99z5KPLbQsLcz0K4vPVsRx5z/r7JUzsJiGm3cY8m3dcd7b3G69BRkvl5pGZspL37vlF2oQYH0VXI4kzPEGyyRQGGtYcY8RUzYC1g77KpiXPYpjx24UeH2Mw9/werJfvHVAhZoHXsoYE3gxS1UT2L8/OlYiNIqoxPGIPPknk+Ih2MCHeuZUen5Q/3LU7nRyoKcnb0WRuQH4A9c3zdgZaked5jrxRRZT3ML+1UPmGEZ4m76nqJI9DFVhh68vqKZenwmJYCy8fqUzvZac4EzRECgYEAuYxfXU89gsgRvu4NGdjzrBlKa5e+RccKWevRunfvm/Q9syH+VdAyxBqPavmxLolsbBOY41StbDqu7UoF3YS+L3yorr7ej3YygoiJPyLHv/mC9szGM11Qk7gJKg1ohkAfWFBVqRc/51CxTfxwxgxJNDwOcIuInhmmkbQCV2YdrEMCgYEAyRt4oIDxc0z+uh7HyXUIO3OzmUeQ4BBbY+mD1m94xYNr5cyXPBI8WHk6ZMvVD6bhBsHyNzTs7TLOmSiBXExntTWWp1awwDVU0ZamOqRt7p0EDDFsKVONuTwDDpGcKRmxPFr2pE2f3Pwn4IvxAryf6BlBnE49P192JOgWkWPZcpECgYBrByB/J1UY2iT6WEAwV6d+7vuxgk8b2ssKwW8xuzr4X3Mzn/kgML8HxTZTA4KDkHt8//ThtJdwZY0/jMJDW+2EYMjAO5MPd6N9Dhr00zAbeWZkzAvL1xjCjJakriVLwzMRcw1mQuX2nOIZVPMMGkwjmDzQCHyejbc6NnZfSaT2FwKBgDl0483XiSRMByrdyG7CRQvXTqSoUbfydGnjOYt2ZabomVcaUMsU2rDcUdaidcj3AKuRlcY4FMjgDy6q5+qa2bZoDRbaKb2Afy8yP2PZvp7BtXpWVHSqLn+Rqb3r1BXaBU0dIAJpOxdD45C/6qfBhF4neRtTLyR//Z0lwezf/ixBAoGAUQehXNOpGKV4jO9En9P5PPJOj2LZk4DDWh7op15MBsGMyr1IUUY42TRTzP+6unfgiLq8xYYPjlIeHNFjztjk7nj892R/ykiiUY3+u8w2nHJc0jfQTk4rTM8D2qmRAcrCE7haE8I6FCBA6pe3KOPspT1eYugA5GknNEQyDmoIWqI=";
  private String issuer = "item-kafka";
  private int expiry = 129600;
  private String audience = "/item-kafka/app/send-items/v1";


  @Test
  void buildPEPToken() {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, expiry);
    Map<String, Object> header = new HashMap<>();
    header.put(Header.TYPE, Header.JWT_TYPE);
    header.put(JwsHeader.ALGORITHM, SignatureAlgorithm.RS256);
    header.put(JwsHeader.KEY_ID, issuer + ".1");
    String jti = UUID.randomUUID().toString();
    log.info("Build JWT");
    String jwtToken = "";
    jwtToken = Jwts.builder()
        .setHeader(header)
        .setId(jti)
        .setAudience(audience)
        .setIssuer(issuer)
        .setExpiration(calendar.getTime())
        .signWith(SignatureAlgorithm.RS256, this.getPrivatekey())
        .compact();
    log.info("JWT Token : {}", jwtToken);
  }

  private Key getPrivatekey() {
    try {
      Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
      StringBuilder sb = new StringBuilder();
      sb.append("-----BEGIN RSA PRIVATE KEY-----");
      sb.append(System.lineSeparator());
      sb.append(privateKeyString);
      sb.append(System.lineSeparator());
      sb.append("-----END RSA PRIVATE KEY-----");
      PEMParser pemParser = new PEMParser(new StringReader(sb.toString()));
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
      Object object = pemParser.readObject();
      KeyPair kp = converter.getKeyPair((PEMKeyPair) object);
      log.info("", kp.getPrivate());
      return kp.getPrivate();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}