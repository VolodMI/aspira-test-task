package com.task.leonparser.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.leonparser.client.LeonApiClient;
import com.task.leonparser.config.LeonParserProperties;
import com.task.leonparser.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class LeonService {

    private final LeonApiClient client;
    private final LeonParserProperties properties;
    private final ObjectMapper objectMapper;

    private static final DateTimeFormatter UTC_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                    .withZone(ZoneOffset.UTC);

    public void startParsing() {
        client.getSports()
                .flatMapMany(this::parseSportsJson)
                .flatMap(this::processLeague, 3)
                .doOnComplete(() -> log.info("Parsing completed"))
                .onErrorContinue((error, obj) -> log.error("Error during parsing: {}", error.getMessage()))
                .subscribe();
    }

    private Flux<League> parseSportsJson(JsonNode sportsJson) {
        if (sportsJson == null || !sportsJson.isArray()) {
            return Flux.empty();
        }

        List<String> targetSports = properties.getTargetSports();
        boolean topLeaguesOnly = properties.isTopLeaguesOnly();

        try {
            List<Sport> sports = objectMapper.readValue(sportsJson.traverse(), new TypeReference<>() {
            });
            return Flux.fromIterable(sports)
                    .filter(sport -> targetSports.contains(sport.getName()))
                    .flatMap(sport -> Flux.fromIterable(sport.getRegions()))
                    .flatMap(region -> Flux.fromIterable(region.getLeagues()))
                    .filter(league -> !topLeaguesOnly || league.isTop());
        } catch (Exception e) {
            log.error("Error parsing sports JSON: {}", e.getMessage());
            return Flux.empty();
        }
    }

    private Flux<Void> processLeague(League league) {
        return client.getMatchesByLeague(league.getId())
                .flatMapMany(json -> {
                    try {
                        List<Match> matches = objectMapper.readValue(json.path("events").traverse(), new TypeReference<>() {
                        });
                        return Flux.fromIterable(matches);
                    } catch (Exception e) {
                        log.error("Error parsing matches: {}", e.getMessage());
                        return Flux.empty();
                    }
                })
                .take(properties.getMatchesLimit())
                .flatMap(match -> client.getFullEventInfo(match.getId())
                                .flatMap(fullEvent -> {
                                    try {
                                        Match event = objectMapper.readValue(fullEvent.traverse(), Match.class);
                                        return Mono.fromRunnable(() -> printEvent(league, event)).then(); // <--- ось тут
                                    } catch (Exception e) {
                                        log.error("Error parsing event: {}", e.getMessage());
                                        return Mono.empty();
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Failed to fetch full event info: {}", e.getMessage());
                                    return Mono.empty();
                                })
                        , 3);
    }

    private void printEvent(League league, Match event) {
        long kickoffTimestamp = event.getKickoff();
        Instant kickoffInstant = kickoffTimestamp > 9999999999L
                ? Instant.ofEpochMilli(kickoffTimestamp)
                : Instant.ofEpochSecond(kickoffTimestamp);
        String kickoffTime = UTC_FORMATTER.format(kickoffInstant);

        System.out.println(indent(0) + "Sport/League: " + league.getName());
        System.out.println(indent(1) + event.getName() + ", " + kickoffTime + ", " + event.getId());

        List<Market> markets = event.getMarkets();
        if (markets != null) {
            for (Market market : markets) {
                System.out.println(indent(2) + market.getName());
                List<Runner> runners = market.getRunners();
                if (runners != null) {
                    for (Runner runner : runners) {
                        System.out.println(indent(3) + runner.getName() + ", " + runner.getPrice() + ", " + runner.getId());
                    }
                }
            }
        }
        System.out.println();
    }


    private String indent(int level) {
        return " ".repeat(level * 4);
    }
}
