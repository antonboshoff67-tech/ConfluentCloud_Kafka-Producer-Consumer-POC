package za.co.woolworths.itemkafka_poc.service;

import za.co.woolworths.itemkafka_poc.exceptions.ConsumerException;
import za.co.woolworths.itemkafka_poc.model.ServiceRequest;

public interface MsgRoutingService {
  void processReceivedMsgRequest(ServiceRequest serviceRequest);

  void processSentMsgRequest(ServiceRequest serviceRequest) throws ConsumerException;

  //public void populateKafkaWithItems();
}
