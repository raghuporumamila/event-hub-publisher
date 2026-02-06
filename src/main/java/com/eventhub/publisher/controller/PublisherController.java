package com.eventhub.publisher.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.eventhub.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

@RestController
public class PublisherController {

	@Autowired
	RestTemplate restTemplate;
	private Publisher publisher = null;

	@Value("${evenhub.pubsub.topic}")
	private String topic;
	@Value("${evenhub.pubsub.project}")
	private String project;
	@Value("${evenhub.rest.client.daoApiEndpoint}")
	private String daoApiEndpoint;
	@Value("${evenhub.rest.client.schemaApiEndpoint}")
	private String schemaApiEndpoint;

	@Autowired
	private ObjectMapper objectMapper;

	@PostConstruct
	public void init() throws Exception {
		System.out.println("topic == " + topic);

	}

	@PreDestroy
	public void cleanup() {
		if (publisher != null) {
			publisher.shutdown();
		}
	}

	@RequestMapping(value = "/publish", method = RequestMethod.POST)
	public void publish(@RequestBody Event event) throws Exception {

		List<ApiFuture<String>> futures = new ArrayList<>();
		ProjectTopicName topicName = ProjectTopicName.of(project, topic);
		System.out.println(topicName);
		publisher = Publisher.newBuilder(topicName).build();

		String hostport = "localhost:8085";
		InstantiatingGrpcChannelProvider channelProvider =
				InstantiatingGrpcChannelProvider.newBuilder()
						.setEndpoint(hostport)
						.setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
						.build();

		publisher = Publisher.newBuilder(topicName)
				.setChannelProvider(channelProvider)
				.setCredentialsProvider(NoCredentialsProvider.create()) // No auth needed
				.build();

		try {
			/*
			String url = daoApiEndpoint + "/organizations/" + event.getOrganization().getId();
			Organization organization = restTemplate.exchange(url, HttpMethod.GET, null,
					new ParameterizedTypeReference<Organization>() {
					}).getBody();

			url = daoApiEndpoint + "/organizations/" + event.getOrganization().getId() + "/workspaces/" +
					event.getWorkspace().getId() + "/sources/" + event.getSource().getId();
			Source source = restTemplate.exchange(url, HttpMethod.GET, null,
					new ParameterizedTypeReference<Source>() {
					}).getBody();
			 */
			String url = daoApiEndpoint + "/organizations/" + event.getOrganization().getId() + "/workspaces/" +
					event.getWorkspace().getId() + "/eventDefinitions/" + event.getEventDefinition().getId();
			EventDefinition definition = restTemplate.exchange(url, HttpMethod.GET, null,
					new ParameterizedTypeReference<EventDefinition>() {
					}).getBody();

			event.setEventDefinition(definition);
			HttpHeaders headers1 = new HttpHeaders();
			headers1.setContentType(MediaType.APPLICATION_JSON);

			ValidateJsonData validateJsonData = new ValidateJsonData();
			validateJsonData.setPayload(event.getPayload());
			validateJsonData.setSchema(definition.getPayloadSchema());
			HttpEntity<ValidateJsonData> request = new HttpEntity<ValidateJsonData>(validateJsonData, headers1);

			ResponseEntity<String> response = restTemplate.postForEntity( schemaApiEndpoint + "/validate",
					request , String.class );

			if (!response.getStatusCode().equals(HttpStatus.OK)) {
				throw new RuntimeException(response.getBody());
			}


			Map<String, String> attributes = new HashMap<String, String>();
			attributes.put("orgId", event.getOrganization().getId().toString());
			attributes.put("workspaceId", event.getWorkspace().getId().toString());

			String body = objectMapper.writeValueAsString(event);
			// convert message to bytes
			ByteString data = ByteString.copyFromUtf8(body);
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes).build();
			// Schedule a message to be published. Messages are automatically batched.
			ApiFuture<String> future = publisher.publish(pubsubMessage);
			futures.add(future);

		} finally {
			// Wait on any pending requests
			List<String> messageIds = ApiFutures.allAsList(futures).get();

			for (String messageId : messageIds) {
				System.out.println(messageId);
			}
		}
	}

	/*
	@RequestMapping(value = "/publishBatch", method = RequestMethod.POST)
	public void publishBatch(@RequestBody String body) throws Exception {

		List<ApiFuture<String>> futures = new ArrayList<>();
		try {
			System.out.println("body == " + body);
			JsonElement jsonData = new JsonParser().parse(body);
			JsonArray jsonArray = jsonData.getAsJsonArray();
			for (JsonElement jsonElement : jsonArray) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();
				String eventName = jsonObject.get("name").getAsString();
				//System.out.println("eventName == " + eventName);
				String orgId = jsonObject.get("orgId").getAsString();
				EventDefinition definition = restTemplate.exchange(configProperties.getDaoApiEndpoint() + "/organization/eventDefinition?eventName=" +
						eventName + "&orgId=" + orgId + "&workspace=" + configProperties.getFirestoreWorkspace(), HttpMethod.GET, null,
						new ParameterizedTypeReference<EventDefinition>() {
						}).getBody();
	
				HttpHeaders headers1 = new HttpHeaders();
				headers1.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
	
				MultiValueMap<String, String> map= new LinkedMultiValueMap<String, String>();
				map.add("jsonSchema",  definition.getSchema());
				map.add("jsonData",  jsonObject.toString());
				//System.out.println("jsonData == " + jsonObject.toString());
				HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map, headers1);
	
				ResponseEntity<String> response = restTemplate.postForEntity( configProperties.getSchemaApiEndpoint() + "/validate", request , String.class );
	
				if (!response.getStatusCode().equals(HttpStatus.OK)) {
					throw new RuntimeException(response.getBody());
				}
	
				// convert message to bytes
				ByteString data = ByteString.copyFromUtf8(body);
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
	
				// Schedule a message to be published. Messages are automatically batched.
				ApiFuture<String> future = publisher.publish(pubsubMessage);
				futures.add(future);
			}
		} finally {
			// Wait on any pending requests
			List<String> messageIds = ApiFutures.allAsList(futures).get();

			for (String messageId : messageIds) {
				System.out.println(messageId);
			}
		}
	}*/
}
