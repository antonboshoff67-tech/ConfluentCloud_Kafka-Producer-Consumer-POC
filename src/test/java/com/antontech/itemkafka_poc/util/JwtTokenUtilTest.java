package com.antontech.itemkafka_poc.util;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JwtTokenUtilTest {

  @Test
  void buildGatewayToken_usesGeneratedKeyMaterial() throws Exception {
    KeyPair keyPair = generateKeyPair();
    JwtTokenUtil jwtTokenUtil = new JwtTokenUtil();

    ReflectionTestUtils.setField(jwtTokenUtil, "privateKey", toPem(keyPair.getPrivate()));
    ReflectionTestUtils.setField(jwtTokenUtil, "issuer", "item-kafka-producer");
    ReflectionTestUtils.setField(jwtTokenUtil, "expiryMinutes", 30);

    String token = jwtTokenUtil.buildGatewayToken();

    assertNotNull(token);
    assertFalse(token.isBlank());
  }

  private static KeyPair generateKeyPair() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    return keyPairGenerator.generateKeyPair();
  }

  private static String toPem(PrivateKey privateKey) {
    String encoded = Base64.getMimeEncoder(64, System.lineSeparator().getBytes(StandardCharsets.UTF_8))
        .encodeToString(privateKey.getEncoded());
    return String.join(System.lineSeparator(),
        "-----BEGIN PRIVATE KEY-----",
        encoded,
        "-----END PRIVATE KEY-----");
  }
}
