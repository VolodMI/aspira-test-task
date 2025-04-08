package com.task.leonparser.client;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class LeonApiClient {

    private final WebClient webClient;

    public LeonApiClient() {
        this.webClient = WebClient.builder()
                .baseUrl("https://leonbets.com/api-2/betline")
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10 MB buffer
                .defaultHeader("User-Agent", "Mozilla/5.0 (compatible; LeonScraper/1.0)")
                .build();
    }

    public Mono<JsonNode> getSports() {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/sports")
                        .queryParam("ctag", "en-US")
                        .queryParam("flags", "urlv2")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class);
    }

    public Mono<JsonNode> getMatchesByLeague(long leagueId) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/events/all")
                        .queryParam("ctag", "en-US")
                        .queryParam("league_id", leagueId)
                        .queryParam("hideClosed", "true")
                        .queryParam("flags", "reg,urlv2,mm2,rrc,nodup")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class);
    }

    public Mono<JsonNode> getFullEventInfo(long eventId) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/event/all")
                        .queryParam("ctag", "en-US")
                        .queryParam("eventId", eventId)
                        .queryParam("flags", "reg,urlv2,mm2,rrc,nodup,smgv2,outv2,wd2,dar")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class);
    }
}
