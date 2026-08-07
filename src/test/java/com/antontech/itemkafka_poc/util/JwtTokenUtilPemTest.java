package com.antontech.itemkafka_poc.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class JwtTokenUtilPemTest {

  @Test
  void getPrivateKey_acceptsPkcs8Pem() throws Exception {
    KeyPair keyPair = generateKeyPair();
    JwtTokenUtil jwtTokenUtil = new JwtTokenUtil();

    assertNotNull(jwtTokenUtil.getPrivateKey(toPem(keyPair.getPrivate())));
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

