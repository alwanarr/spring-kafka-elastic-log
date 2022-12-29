package com.kafka.demo.service;

import com.kafka.demo.dto.PayloadDTO;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class RestTemplateService {
    public String send(PayloadDTO data){
        RestTemplate restTemplate = new RestTemplate();
        HttpEntity<PayloadDTO> httpEntity = new HttpEntity<>(data);
        String url = "http://localhost:9200/kafka-log/_doc/";
        String result = restTemplate.postForObject(url, httpEntity, String.class);
        return result;
    }
}
