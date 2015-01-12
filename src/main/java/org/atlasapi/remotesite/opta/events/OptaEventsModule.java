package org.atlasapi.remotesite.opta.events;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import joptsimple.internal.Strings;

import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.atlasapi.remotesite.opta.events.soccer.OptaSoccerDataHandler;
import org.atlasapi.remotesite.opta.events.soccer.OptaSoccerDataTransformer;
import org.atlasapi.remotesite.opta.events.soccer.model.SoccerMatchData;
import org.atlasapi.remotesite.opta.events.soccer.model.SoccerTeam;
import org.atlasapi.remotesite.opta.events.sports.OptaSportsDataHandler;
import org.atlasapi.remotesite.opta.events.sports.OptaSportsDataTransformer;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.security.UsernameAndPassword;


public class OptaEventsModule {

    private static final String OPTA_HTTP_SOCCER_CONFIG_PREFIX = "opta.events.http.sports.soccer.";
    private static final String OPTA_HTTP_RUGBY_CONFIG_PREFIX = "opta.events.http.sports.rugby.";
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired EventStore eventStore;
    private @Autowired OrganisationStore organisationStore;
    private @Autowired @Qualifier("topicStore") TopicStore topicStore;
    
    private @Value("${opta.events.http.baseUrl}") String baseUrl;
    private @Value("${opta.events.http.username}") String username;
    private @Value("${opta.events.http.password}") String password;
    
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(soccerIngestTask().withName("Opta Soccer Events Updater"), RepetitionRules.NEVER);
        scheduler.schedule(sportsIngestTask().withName("Opta Sports Events Updater"), RepetitionRules.NEVER);
    }

    private OptaEventsIngestTask<SoccerTeam, SoccerMatchData> soccerIngestTask() {
        return new OptaEventsIngestTask<SoccerTeam, SoccerMatchData>(soccerFetcher(), soccerDataHandler());
    }

    private OptaEventsFetcher<SoccerTeam, SoccerMatchData> soccerFetcher() {
        return new HttpOptaEventsFetcher<>(sportConfig(OPTA_HTTP_SOCCER_CONFIG_PREFIX), HttpClients.webserviceClient(), soccerTransformer(), new UsernameAndPassword(username, password), baseUrl);
    }
    
    private OptaDataTransformer<SoccerTeam, SoccerMatchData> soccerTransformer() {
        return new OptaSoccerDataTransformer();
    }

    // Opta Sports are configured through three parameters: feed type, a competition id, and a season. Each sport is
    // held in an environment param suffixed with the sport's enum value, and the three parameters are joined with the | character.
    // This method reads any environment params with the supplied suffix and splits out the three config params into a special
    // holding type, and returns a map of sport -> configuration
    private Map<OptaSportType, OptaSportConfiguration> sportConfig(String sportPrefix) {
        Builder<OptaSportType, OptaSportConfiguration> configMapping = ImmutableMap.<OptaSportType, OptaSportConfiguration>builder();
        for (Entry<String, Parameter> property : Configurer.getParamsWithKeyMatching(Predicates.containsPattern(sportPrefix))) {
            String sportKey = property.getKey().substring(sportPrefix.length());
            String sportConfig = property.getValue().get();
            
            if (!Strings.isNullOrEmpty(sportConfig)) {
                OptaSportType sport = OptaSportType.valueOf(sportKey.toUpperCase());
                OptaSportConfiguration config = parseConfig(sportConfig);
                configMapping.put(sport, config);
            } else {
                log.warn("Opta HTTP configuration for sport {} is missing.", sportKey);
            }
        }
        return configMapping.build();
    }

    /**
     * Parses a String parameter into a set of three parameters required for the Opta Sports 
     * competition API. The format is [prefix].[sportName]=feedType|competition|seasonId
     * @param sportConfig
     * @return
     */
    private OptaSportConfiguration parseConfig(String sportConfig) {
        Iterable<String> configItems = Splitter.on('|').split(sportConfig);
        return OptaSportConfiguration.builder()
                .withFeedType(Iterables.get(configItems, 0))
                .withCompetition(Iterables.get(configItems, 1))
                .withSeasonId(Iterables.get(configItems, 2))
                .build();
    }

    @Bean
    private OptaSoccerDataHandler soccerDataHandler() {
        return new OptaSoccerDataHandler(organisationStore, eventStore, utility());
    }

    @Bean
    private OptaEventsUtility utility() {
        return new OptaEventsUtility(topicStore);
    }

    private OptaEventsIngestTask<SportsTeam, SportsMatchData> sportsIngestTask() {
        return new OptaEventsIngestTask<>(sportsFetcher(), sportsDataHandler());
    }

    private OptaEventsFetcher<SportsTeam, SportsMatchData> sportsFetcher() {
        return new HttpOptaEventsFetcher<>(sportConfig(OPTA_HTTP_RUGBY_CONFIG_PREFIX), HttpClients.webserviceClient(), sportsTransformer(), new UsernameAndPassword(username, password), baseUrl);
    }
    
    private OptaDataTransformer<SportsTeam, SportsMatchData> sportsTransformer() {
        return new OptaSportsDataTransformer();
    }

    private OptaSportsDataHandler sportsDataHandler() {
        return new OptaSportsDataHandler(organisationStore, eventStore, utility());
    }
}
