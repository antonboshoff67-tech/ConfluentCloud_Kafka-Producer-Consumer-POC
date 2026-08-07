package com.antontech.itemkafka_poc.service.impl;

import com.antontech.itemkafka_poc.exceptions.ConsumerException;
import com.antontech.itemkafka_poc.model.JwtResponse;
import com.antontech.itemkafka_poc.model.ServiceRequest;
import com.antontech.itemkafka_poc.prop.SSLProperties;
import com.antontech.itemkafka_poc.service.MsgRoutingService;
import com.antontech.itemkafka_poc.util.JwtTokenUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.UUID;

/**
 * Default {@link MsgRoutingService} implementation. Builds a signed JWT via
 * {@link JwtTokenUtil} and forwards {@link ServiceRequest} payloads to the
 * downstream gateway HTTP endpoint configured under {@code gateway.endpoint.url}.
 * Also exposes a helper to build a mutual-TLS {@link RestTemplate} using the
 * keystore configured in {@link SSLProperties} for gateways that require
 * client-certificate authentication.
 */
@Slf4j
@Service
public class MsgRoutingServiceImpl implements MsgRoutingService {

  @Value("${gateway.endpoint.url}")
  private String baseUrl;

  @Autowired
  private JwtTokenUtil jwtTokenUtil;

  @Autowired
  private SSLProperties sslProperties;

  /** {@inheritDoc} */
  @Override
  public void processReceivedMsgRequest(ServiceRequest serviceRequest) {
    log.info("Received message request: {}", serviceRequest.getMsg());
  }

  /** {@inheritDoc} */
  @Override
  public void processSentMsgRequest(ServiceRequest serviceRequest) throws ConsumerException {
    JwtResponse jwtTokenResponse = createJWT();
    HttpHeaders headers = createHeaders(jwtTokenResponse);
    HttpEntity<ServiceRequest> requestHttpEntity = createRequest(serviceRequest, headers);

    try {
      log.info("Prepared request for gateway endpoint {}", baseUrl);
      log.debug("Request headers: {}", requestHttpEntity.getHeaders());
    } catch (Exception e) {
      log.error("Failed to prepare gateway request", e);
      throw new ConsumerException(e.getMessage());
    }
  }

  /** @return a signed JWT wrapped in a {@link JwtResponse}, or an empty token if no private key is configured. */
  private JwtResponse createJWT() {
    try {
      return new JwtResponse(jwtTokenUtil.buildGatewayToken());
    } catch (Exception e) {
      log.warn("JWT was not created because no private key is configured: {}", e.getMessage());
      return new JwtResponse("");
    }
  }

  /**
   * @param jwtResponse the JWT to attach as a Bearer token, if present.
   * @return standard JSON content headers plus an optional {@code Authorization} bearer header and a random {@code X-Request-ID}.
   */
  private HttpHeaders createHeaders(JwtResponse jwtResponse) {
    HttpHeaders headers = new HttpHeaders();
    headers.set("Content-Type", "application/json");
    headers.set("Accept", "application/text");
    if (jwtResponse != null && jwtResponse.getToken() != null && !jwtResponse.getToken().isBlank()) {
      headers.set("Authorization", "Bearer " + jwtResponse.getToken());
    }
    headers.set("X-Request-ID", UUID.randomUUID().toString());
    return headers;
  }

  /**
   * @param serviceRequest the request body to send.
   * @param headers        the headers to attach.
   * @return an {@link HttpEntity} ready to be posted to the gateway.
   */
  private HttpEntity<ServiceRequest> createRequest(ServiceRequest serviceRequest, HttpHeaders headers) {
    return new HttpEntity<>(serviceRequest, headers);
  }

  /**
   * Builds a {@link RestTemplate} configured with mutual-TLS using the
   * keystore/keystore-password configured under {@code keys.ssl.*}
   * (see {@link SSLProperties} and {@code SETUP_GUIDE.md}).
   *
   * @return an SSL-enabled {@link RestTemplate} for calling the gateway over HTTPS with a client certificate.
   * @throws KeyStoreException          if the keystore file is malformed.
   * @throws IOException                if the keystore file cannot be read.
   * @throws UnrecoverableKeyException  if the keystore password is incorrect.
   * @throws CertificateException       if a certificate in the keystore cannot be loaded.
   * @throws NoSuchAlgorithmException   if the JVM does not support the required algorithm.
   * @throws KeyManagementException     if the SSL context cannot be initialised.
   */
  public RestTemplate getGatewaySSLTemplateConfig()
      throws KeyStoreException, IOException, UnrecoverableKeyException, CertificateException,
      NoSuchAlgorithmException, KeyManagementException {

    SSLContext context = SSLContextBuilder
        .create()
        .loadKeyMaterial(ResourceUtils.getFile(sslProperties.getKeyStore()),
            sslProperties.getKeyStorePassword().toCharArray(),
            sslProperties.getKeyStorePassword().toCharArray())
        .build();

    HttpClient client = HttpClients.custom().setSSLContext(context).build();
    HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
    requestFactory.setHttpClient(client);

    RestTemplate restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(requestFactory);
    return restTemplate;
  }

  /**
   * @param requestHttpEntity the prepared request entity to post.
   * @return the response body returned by the gateway.
   * @throws ConsumerException if the URL is malformed or the HTTP call fails.
   */
  private String post(HttpEntity<ServiceRequest> requestHttpEntity) throws ConsumerException {
    try {
      RestTemplate restTemplate = new RestTemplate();
      URI uri = new URI(baseUrl);
      ResponseEntity<String> result = restTemplate.postForEntity(uri, requestHttpEntity, String.class);
      return result.getBody();
    } catch (URISyntaxException | ResourceAccessException e) {
      log.error("Failed to post request to gateway", e);
      throw new ConsumerException(e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected gateway call failure", e);
      throw new ConsumerException(e.getMessage());
    }
  }
}


