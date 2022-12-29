package com.kafka.demo.service;

import com.google.gson.Gson;
import com.kafka.demo.dto.PayloadDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class KafkaConsumer {

    private final RestTemplateService restTemplateService;

    public KafkaConsumer(RestTemplateService restTemplateService) {
        this.restTemplateService = restTemplateService;
    }

    private Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "my-topic", groupId = "test-consumer-group")
    public void consume(String message) {
        Gson gson = new Gson();
        PayloadDTO data = gson.fromJson(message , PayloadDTO.class);

        // Print statement
        System.out.println("message = " + message);
        System.out.println("data = " + data);

        logger.info("getAuthor : {}", data.getAuthor());
        logger.info("getTitle : {}", data.getTitle());

//        restTemplateService.send(data);
        sendDataToElastic(data).subscribe();

        System.out.println("============selesai==============");

    }

    public Mono<PayloadDTO> sendDataToElastic(PayloadDTO data) {
        WebClient client = WebClient.builder()
                .baseUrl("http://localhost:9200/")
                .build();

        return client.post().uri("kafka-log/_doc/")
                .body(Mono.just(data), PayloadDTO.class)
                .retrieve()
                .bodyToMono(PayloadDTO.class);
    }
}
