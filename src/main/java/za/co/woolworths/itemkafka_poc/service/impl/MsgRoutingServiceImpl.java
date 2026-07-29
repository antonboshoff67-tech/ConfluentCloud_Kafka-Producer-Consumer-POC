package za.co.woolworths.itemkafka_poc.service.impl;

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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import za.co.woolworths.itemkafka_poc.exceptions.ConsumerException;
//import za.co.woolworths.itemkafka_poc.model.Item;
import za.co.woolworths.itemkafka_poc.model.JwtResponse;
import za.co.woolworths.itemkafka_poc.model.ServiceRequest;
import za.co.woolworths.itemkafka_poc.prop.SSLProperties;
import za.co.woolworths.itemkafka_poc.service.MsgRoutingService;
import za.co.woolworths.itemkafka_poc.util.JwtTokenUtil;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class MsgRoutingServiceImpl implements MsgRoutingService {

  @Value("${gateway.endpoint.url}")
  String baseUrl;
  @Autowired
  private JwtTokenUtil jwtTokenUtil;
  @Autowired
  private SSLProperties sslProperties;

  /*@Autowired
  private za.co.woolworths.itemkafka_poc.repos.ItemRepository itemRepository;*/

  /*@Autowired
  private KafkaTemplate<String, Item> kafkaTemplate;*/

  @Override
  public void  processReceivedMsgRequest(ServiceRequest serviceRequest) {
    log.info("Message Received from Client : " + serviceRequest.getMsg());
  }

  @Override
  public void  processSentMsgRequest(ServiceRequest serviceRequest) throws ConsumerException {
    log.info("Create JWT");
    JwtResponse jwtTokenResponse = createJWT();
    log.info("Create HTTP Header");
    HttpHeaders headers = createHeaders(jwtTokenResponse);
    log.info("Create HTTP Entity");
    HttpEntity<ServiceRequest> requestHttpEntity = createRequest(serviceRequest, headers);
    log.info("Post request");
    try {
      // ToDo Uncomment this when we do a post request for the particular method
      //  String response = post(requestHttpEntity);
      String response = "Default post request";
      log.info("The responce is : {}", response);
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new ConsumerException(e.getMessage());
    }
  }

  private JwtResponse createJWT() {
    //ToDo - repair this when we got our SSL certs
    String token = "abc";
    //String token = jwtTokenUtil.buildGatewayToken();
    log.info("The token : {}", token);
    JwtResponse jwtResponse = new JwtResponse(token);
    return jwtResponse;
  }

  private HttpHeaders createHeaders(JwtResponse jwtResponse)
  {
    log.info("Creating the Headers");
    HttpHeaders headers = new HttpHeaders();
    //ToDo - uncomment this once we got our SSL certs
    //headers.set("Authorization", "Bearer " + jwtResponse.getToken());
    String uuid = UUID.randomUUID().toString();
    //headers.set("X-Request-ID", uuid);
    //headers.set("X-B3-TraceId", "T12345");
    //headers.set("X-B3-SpanId", "S12345");
    headers.set("Content-Type", "application/json");
    headers.set("Accept", "application/text");
    log.debug("headers created ... " + headers);
    return headers;
  }

  private HttpEntity<ServiceRequest> createRequest(ServiceRequest leadServiceRequest, HttpHeaders headers)
  {
    HttpEntity<ServiceRequest> request = new HttpEntity<>(leadServiceRequest, headers);
    return request;
  }

  public RestTemplate getGatewaySSLTemplateConfig()
      throws KeyStoreException, IOException, UnrecoverableKeyException, CertificateException,
      NoSuchAlgorithmException, KeyManagementException {

    SSLContext context = SSLContextBuilder
        .create()
        .loadKeyMaterial(ResourceUtils.getFile(sslProperties.getKeyStore()),
            sslProperties.getKeyStorePassword().toCharArray(),
            sslProperties.getKeyStorePassword().toCharArray())
        .build();
    HttpClient client = HttpClients
        .custom()
        .setSSLContext(context)
        .build();
    HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
    requestFactory.setHttpClient(client);
    //TODO: set timeout values
    RestTemplate restTemplate = new RestTemplate();
    //restTemplate.getInterceptors().add(interceptor);
    restTemplate.setRequestFactory(requestFactory);
    return restTemplate;
  }


  private String post(HttpEntity<ServiceRequest> requestHttpEntity) throws ConsumerException {
    String responce = "";
    try {
      // ToDo
      // Add SSL Config when  we got our SSL certs
      //RestTemplate restTemplate = getGatewaySSLTemplateConfig();
      RestTemplate restTemplate = new RestTemplate();
      log.info("baseUrl = {}", baseUrl);
      URI uri = new URI(baseUrl);
      log.info("HTTP Headers : {}", requestHttpEntity.getHeaders());
      ResponseEntity<String> result = restTemplate.postForEntity(uri, requestHttpEntity, String.class);
      log.info("The StatusCodeValue = {}", result.getStatusCodeValue());
      log.info("The StatusCode      = {}", result.getStatusCode());
      log.info("The Body            = {}", result.getBody());
      responce = result.getBody();
    } catch (URISyntaxException e) {
      log.error(e.getMessage());
      throw new ConsumerException(e.getMessage());
    } catch (ResourceAccessException r) {
      log.error(r.getMessage());
      throw new ConsumerException(r.getMessage());
    } catch (Exception x) {
      log.error(x.getMessage());
      throw new ConsumerException(x.getMessage());
    }
    return responce;
  }

  private static final String TOPIC_NAME = "Item_Topic";


  /*@Override
  @PostConstruct
  public void populateKafkaWithItems() {
    List<Item> items = itemRepository.findFirst10000ByItemLevelIsNotNull();

    for (Item item : items) {
      kafkaTemplate.send(TOPIC_NAME, item.getItemId(), item);
    }
  }*/

  // Other method implementations can be here...
}
