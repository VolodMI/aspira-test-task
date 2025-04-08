package com.task.leonparser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.task.leonparser.client.LeonApiClient;
import com.task.leonparser.config.LeonParserProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class LeonService {

    private final LeonApiClient client;
    private final LeonParserProperties properties;

    private static final DateTimeFormatter UTC_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                    .withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter FILE_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private BufferedWriter writer;

    public void startParsing() {
        String today = LocalDate.now().format(FILE_DATE_FORMATTER);
        String outputFolder = properties.getOutputFolder();
        String fileName = outputFolder + "output-" + today + ".txt";

        new File(outputFolder).mkdirs();

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName))) {
            this.writer = bufferedWriter;

            client.getSports()
                    .flatMapMany(this::parseSportsJson)
                    .flatMap(this::processLeague, 3)
                    .doOnComplete(() -> {
                        log.info("Parsing completed! Output file: {}", fileName);
                    })
                    .onErrorContinue((error, obj) -> log.error("Error during parsing: {}", error.getMessage()))
                    .blockLast();
        } catch (IOException e) {
            log.error("Failed to write to file: {}", e.getMessage());
        }
    }

    private Flux<LeagueNode> parseSportsJson(JsonNode sportsJson) {
        if (sportsJson == null || !sportsJson.isArray()) {
            return Flux.empty();
        }

        List<String> targetSports = properties.getTargetSports();
        boolean topLeaguesOnly = properties.isTopLeaguesOnly();

        return Flux.fromIterable(sportsJson)
                .filter(sport -> targetSports.contains(sport.path("name").asText()))
                .flatMap(sport -> Flux.fromIterable(sport.path("regions"))
                        .flatMap(region -> Flux.fromIterable(region.path("leagues"))
                                .filter(league -> {
                                    if (topLeaguesOnly) {
                                        return league.path("top").asBoolean();
                                    } else {
                                        return true;
                                    }
                                })
                                .map(league -> buildLeagueNode(sport, league))
                        )
                );
    }

    private LeagueNode buildLeagueNode(JsonNode sportNode, JsonNode leagueNode) {
        return new LeagueNode(
                sportNode.path("name").asText(),
                leagueNode.path("name").asText(),
                leagueNode.path("id").asLong()
        );
    }

    private Flux<Void> processLeague(LeagueNode league) {
        return client.getMatchesByLeague(league.leagueId())
                .flatMapMany(json -> {
                    JsonNode events = json.path("events");
                    return (events.isArray()) ? Flux.fromIterable(events) : Flux.empty();
                })
                .take(properties.getMatchesLimit())
                .flatMap(event -> {
                    long eventId = event.path("id").asLong();
                    return client.getFullEventInfo(eventId)
                            .flatMap(fullEvent -> Mono.fromRunnable(() -> printEvent(league, fullEvent)).then())
                            .onErrorResume(e -> {
                                log.error("Failed to fetch full event info for eventId {}: {}", eventId, e.getMessage());
                                return Mono.empty();
                            });
                }, 3);
    }

    private void printEvent(LeagueNode league, JsonNode event) {
        try {
            String matchName = event.path("name").asText();
            long kickoffTimestamp = event.path("kickoff").asLong();

            Instant kickoffInstant = kickoffTimestamp > 9999999999L
                    ? Instant.ofEpochMilli(kickoffTimestamp)
                    : Instant.ofEpochSecond(kickoffTimestamp);

            String kickoffTime = UTC_FORMATTER.format(kickoffInstant);
            long eventId = event.path("id").asLong();

            writer.write(indent(0) + league.sportName() + ", " + league.leagueName());
            writer.newLine();
            writer.write(indent(1) + matchName + ", " + kickoffTime + ", " + eventId);
            writer.newLine();

            JsonNode markets = event.path("markets");
            if (markets.isArray()) {
                for (JsonNode market : markets) {
                    String marketName = market.path("name").asText();
                    writer.write(indent(2) + marketName);
                    writer.newLine();

                    JsonNode runners = market.path("runners");
                    if (runners.isArray()) {
                        for (JsonNode runner : runners) {
                            String outcomeName = runner.path("name").asText();
                            double price = runner.path("price").asDouble();
                            long outcomeId = runner.path("id").asLong();

                            writer.write(indent(3) + outcomeName + ", " + price + ", " + outcomeId);
                            writer.newLine();
                        }
                    }
                }
            }
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.error("Failed to write event to file: {}", e.getMessage());
        }
    }

    private String indent(int level) {
        return " ".repeat(level * 4);
    }

    private record LeagueNode(String sportName, String leagueName, long leagueId) {
    }
}
