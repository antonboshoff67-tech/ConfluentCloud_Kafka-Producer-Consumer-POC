package za.co.woolworths.itemkafka_poc.util;

import io.jsonwebtoken.Header;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.security.Key;
import java.security.KeyPair;
import java.security.Security;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
public class JwtTokenUtil implements Serializable {

  @Value("${jwt.pvtkey}")
  String key;
  @Value("${jwt.iss}")
  private String issuer;
  @Value("${jwt.exp}")
  private int expiry;

  public String buildGatewayToken() {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.MINUTE, expiry);
    Map<String, Object> header = new HashMap<>();
    header.put(Header.TYPE, Header.JWT_TYPE);
    header.put(JwsHeader.ALGORITHM, SignatureAlgorithm.RS256);
    header.put(JwsHeader.KEY_ID, issuer + ".1");
    String jti = UUID.randomUUID().toString();
    String audience = "/item-kafka/app/send-items/v1";
    log.info("Build JWT");
    String jwtToken =
            null;
    try {
      jwtToken = Jwts.builder()
              .setHeader(header)
              .setId(jti)
              .setAudience(audience)
              .setIssuer(issuer)
              .setExpiration(calendar.getTime())
              .signWith(SignatureAlgorithm.RS256, getPrivatekey(key))
              .compact();
    } catch (IOException e) {
      log.error(e.getMessage());
    }
    log.info("JWT Token : {}", jwtToken);
    return jwtToken;
  }

  public Key getPrivatekey(String privateKeyString) throws IOException {
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
    return kp.getPrivate();
  }
}
