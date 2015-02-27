package org.atlasapi.remotesite.bt.events;

import java.util.Map;

import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.events.EventsUtility;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;


public final class BtEventsUtility extends EventsUtility<BtSportType> {
    
    private static final String EVENT_URI_BASE = "http://bt.com/events/";
    private static final Map<String, String> VENUE_LOOKUP = ImmutableMap.<String, String>builder()
            .put("Mandalay Bay Events Center, Las Vegas, Nevada", "http://dbpedia.org/resources/Mandalay_Bay_Events_Center")
            .put("Rogers Centre, Vancouver", "http://dbpedia.org/resources/Rogers_Arena")
            .put("MGM Grand Garden Arena, Las Vegas, Nevada", "http://dbpedia.org/resources/MGM_Grand_Garden_Arena")
            .put("US Bank Arena, Cincinnati, Ohio", "http://dbpedia.org/resources/U.S._Bank_Arena")
            .put("Baltimore Arena, Baltimore, Maryland", "http://dbpedia.org/resources/Baltimore_Arena")
            .put("Amway Center, Orlando, Florida", "http://dbpedia.org/resources/Amway_Center")
            .put("Colisee Pepsi, Quebec City, Quebec", "http://dbpedia.org/resources/Colisée_Pepsi")
            .put("Du Arena", "http://dbpedia.org/resources/Clarence_H._\"Du\"_Burns_Arena")
            .put("Ginasio Nelio Dias, Natal", "http://dbpedia.org/resources/Ginásio_Nélio_Dias")
            .put("American Airlines Center, Dallas, Texas", "http://dbpedia.org/resources/American_Airlines_Center")
            .put("The O2 Arena, London", "http://dbpedia.org/resources/O2_Arena_(London)")
            .put("CotaiArena, Macau", "http://dbpedia.org/resources/CotaiArena")
            .put("Arena Jaragua, Jaragua do Sul", "http://dbpedia.org/resources/Arena_Jaraguá")
            .put("Prudential Center, Newark, New Jersey", "http://dbpedia.org/resources/Prudential_Center")
            .put("United Center, Chicago, Illinois", "http://dbpedia.org/resources/United_Center")
            .put("Arena at Gwinnett Center, Duluth, Georgia", "http://dbpedia.org/resources/Arena_at_Gwinnett_Center")
            .put("Sleep Train Arena, Sacramento, California", "http://dbpedia.org/resources/Sleep_Train_Arena")
            .put("Brisbane Entertainment Centre, Brisbane, Queensland", "http://dbpedia.org/resources/Brisbane_Entertainment_Centre")
            .put("Losail Circuit", "http://dbpedia.org/resources/Losail_International_Circuit")
            .put("The Circuit Of The Americas, Austin", "http://dbpedia.org/resources/Circuit_of_the_Americas")
            .put("Autodromo Termas de Rio Hondo", "http://dbpedia.org/resources/Autódromo_Termas_de_Río_Hondo")
            .put("Jerez", "http://dbpedia.org/resources/Circuito_de_Jerez")
            .put("Le Mans", "http://dbpedia.org/resources/Le_Mans")
            .put("Mugello", "http://dbpedia.org/resources/Mugello_Circuit")
            .put("Circuit de Barcelona-Catalunya", "http://dbpedia.org/resources/Circuit_de_Catalunya")
            .put("Assen", "http://dbpedia.org/resources/TT_Circuit_Assen")
            .put("Sachsenring", "http://dbpedia.org/resources/Sachsenring")
            .put("Indianapolis Motor Speedway", "http://dbpedia.org/resources/Indianapolis_Motor_Speedway")
            .put("Automotodrom Brno", "http://dbpedia.org/resources/Masaryk_Circuit")
            .put("Silverstone", "http://dbpedia.org/resources/Silverstone_Circuit")
            .put("Misano World Circuit", "http://dbpedia.org/resources/Misano_World_Circuit_Marco_Simoncelli")
            .put("MotorLand Aragon", "http://dbpedia.org/resources/Ciudad_del_Motor_de_Aragón")
            .put("Motegi", "http://dbpedia.org/resources/Twin_Ring_Motegi")
            .put("Phillip Island", "http://dbpedia.org/resources/Phillip_Island_Grand_Prix_Circuit")
            .put("Sepang Circuit", "http://dbpedia.org/resources/Sepang_Circuit")
            .put("Comunitat Valenciana", "http://dbpedia.org/resources/Circuit_Ricardo_Tormo")
            .build();
    private static final Map<BtSportType, Map<String, String>> EVENT_GROUPS_LOOKUP = ImmutableMap.<BtSportType, Map<String, String>>builder()
            .put(BtSportType.UFC, ImmutableMap.of(
                    "UFC", "http://dbpedia.org/resources/UFC"
            ))
            .put(BtSportType.MOTO_GP, ImmutableMap.of(
                    "Moto GP", "http://dbpedia.org/resources/Moto_gp", 
                    "Motorsport", "http://dbpedia.org/resources/Motorsport",
                    "Grand Prix motorcycle racing", "http://dbpedia.org/resources/Grand_Prix_motorcycle_racing"
            ))
            .build();

    public BtEventsUtility(TopicStore topicStore) {
        super(topicStore);
    }

    @Override
    public String createEventUri(String id) {
        return EVENT_URI_BASE + id;
    }

    /**
     * BT currently don't provide Team information
     */
    @Override
    public String createTeamUri(String id) {
        throw new UnsupportedOperationException();
    }

    /**
     * BT currently don't provide either start or end times
     */
    @Override
    public Optional<DateTime> createEndTime(BtSportType sport, DateTime start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> fetchLocationUrl(String location) {
        return Optional.fromNullable(VENUE_LOOKUP.get(location));
    }

    @Override
    public Optional<Map<String, String>> fetchEventGroupUrls(BtSportType sport) {
        return Optional.fromNullable(EVENT_GROUPS_LOOKUP.get(sport));
    }

}
