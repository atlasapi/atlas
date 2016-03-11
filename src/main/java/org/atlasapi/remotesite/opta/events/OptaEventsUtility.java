package org.atlasapi.remotesite.opta.events;

import java.util.Map;

import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.events.EventsUtility;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;


public class OptaEventsUtility extends EventsUtility<OptaSportType> {
    
    private static final String EVENT_URI_BASE = "http://optasports.com/events/";
    private static final String TEAM_URI_BASE = "http://optasports.com/teams/";
    private static final Map<OptaSportType, Duration> DURATION_MAPPING = 
            ImmutableMap.<OptaSportType, Duration>builder()
                .put(OptaSportType.RUGBY_AVIVA_PREMIERSHIP, Duration.standardMinutes(100))
                .put(OptaSportType.FOOTBALL_GERMAN_BUNDESLIGA, Duration.standardMinutes(110))
                .put(OptaSportType.FOOTBALL_SCOTTISH_PREMIER_LEAGUE, Duration.standardMinutes(110))
                .put(OptaSportType.FOOTBALL_PREMIER_LEAGUE, Duration.standardMinutes(110))
                .put(OptaSportType.FOOTBALL_CHAMPIONS_LEAGUE, Duration.standardMinutes(110))
                .put(OptaSportType.FOOTBALL_EUROPA_LEAGUE, Duration.standardMinutes(110))
                .put(OptaSportType.FOOTBALL_FA_CUP, Duration.standardMinutes(110))
                .build();
    
    private static final Map<OptaSportType, DateTimeZone> TIMEZONE_MAPPING = 
            ImmutableMap.<OptaSportType, DateTimeZone>builder()
                .put(OptaSportType.RUGBY_AVIVA_PREMIERSHIP, DateTimeZone.forID("Europe/London"))
                .put(OptaSportType.FOOTBALL_GERMAN_BUNDESLIGA, DateTimeZone.forID("Europe/Berlin"))
                .put(OptaSportType.FOOTBALL_SCOTTISH_PREMIER_LEAGUE, DateTimeZone.forID("Europe/London"))
                .put(OptaSportType.FOOTBALL_PREMIER_LEAGUE, DateTimeZone.forID("Europe/London"))
                .put(OptaSportType.FOOTBALL_CHAMPIONS_LEAGUE, DateTimeZone.forID("Europe/London")) 
                .put(OptaSportType.FOOTBALL_EUROPA_LEAGUE, DateTimeZone.forID("Europe/London"))
                .put(OptaSportType.FOOTBALL_FA_CUP, DateTimeZone.forID("Europe/London"))
                .build();
    
    private static final Map<String, String> VENUE_LOOKUP = ImmutableMap.<String, String>builder()
            .put("Recreation Ground", "http://dbpedia.org/resources/Recreation_Ground_(Bath)")
            .put("Adams Park", "http://dbpedia.org/resources/Adams_Park")
            .put("stadium:mk", "http://dbpedia.org/resources/Stadium:mk")
            .put("The Stoop", "http://dbpedia.org/resources/The_Stoop")
            .put("Wembley Stadium", "http://dbpedia.org/resources/Wembley_stadium")
            .put("Welford Road", "http://dbpedia.org/resources/Welford_Road_Stadium")
            .put("Twickenham", "http://dbpedia.org/resources/Twickenham_Stadium")
            .put("Kassam Stadium", "http://dbpedia.org/resources/Kassam_Stadium")
            .put("Kingston Park", "http://dbpedia.org/resources/Kingston_Park_(stadium)")
            .put("Allianz Park", "http://dbpedia.org/resources/Barnet_Copthall")
            .put("Kingsholm", "http://dbpedia.org/resources/Kingsholm_Stadium")
            .put("Madejski Stadium", "http://dbpedia.org/resources/Madejski_Stadium")
            .put("AJ Bell Stadium", "http://dbpedia.org/resources/Salford_City_Stadium")
            .put("Sandy Park", "http://dbpedia.org/resources/Sandy_Park")
            .put("Franklin's Gardens", "http://dbpedia.org/resources/Franklin%27s_Gardens")
            .put("Allianz Arena", "http://dbpedia.org/resources/Allianz_Arena")
            .put("BayArena", "http://dbpedia.org/resources/BayArena")
            .put("Benteler-Arena", "http://dbpedia.org/resources/Benteler_Arena")
            .put("Borussia-Park", "http://dbpedia.org/resources/Borussia-Park")
            .put("Celtic Park", "http://dbpedia.org/resources/Celtic_Park")
            .put("Coface Arena", "http://dbpedia.org/resources/Coface_Arena")
            .put("Commerzbank Arena", "http://dbpedia.org/resources/Commerzbank-Arena")
            .put("Dens Park", "http://dbpedia.org/resources/Dens_Park")
            .put("Fir Park", "http://dbpedia.org/resources/Fir_Park")
            .put("Firhill Stadium", "http://dbpedia.org/resources/Firhill_Stadium")
            .put("HDI-Arena", "http://dbpedia.org/resources/HDI-Arena")
            .put("Imtech Arena", "http://dbpedia.org/resources/Volksparkstadion")
            .put("MAGE SOLAR Stadion", "http://dbpedia.org/resources/Mage_Solar_Stadion")
            .put("McDiarmid Park", "http://dbpedia.org/resources/McDiarmid_Park")
            .put("Mercedes-Benz Arena", "http://dbpedia.org/resources/Mercedes-Benz_Arena_(Stuttgart)")
            .put("New Douglas Park", "http://dbpedia.org/resources/New_Douglas_Park")
            .put("Olympiastadion", "http://dbpedia.org/resources/Olympic_Stadium_(Berlin)")
            .put("Pittodrie", "http://dbpedia.org/resources/Pittodrie_Stadium")
            .put("RheinEnergieStadion", "http://dbpedia.org/resources/RheinEnergieStadion")
            .put("Rugby Park", "http://dbpedia.org/resources/Rugby_Park")
            .put("SGL Arena", "http://dbpedia.org/resources/SGL_arena")
            .put("Signal Iduna Park", "http://dbpedia.org/resources/Signal_Iduna_Park")
            .put("St Mirren Park", "http://dbpedia.org/resources/St._Mirren_Park")
            .put("Tannadice Park", "http://dbpedia.org/resources/Tannadice_Park")
            .put("Tulloch Caledonian Stadium", "http://dbpedia.org/resources/Caledonian_Stadium")
            .put("VELTINS-Arena", "http://dbpedia.org/resources/Veltins-Arena")
            .put("Victoria Park, Dingwall", "http://dbpedia.org/resources/Victoria_Park,_Dingwall")
            .put("Volkswagen Arena", "http://dbpedia.org/resources/Volkswagen_Arena")
            .put("WIRSOL Rhein-Neckar-Arena", "http://dbpedia.org/resources/Rhein-Neckar_Arena")
            .put("Weserstadion", "http://dbpedia.org/resources/Weserstadion")
            .put("Anfield", "http://dbpedia.org/resources/Anfield") 
            .put("Boleyn Ground", "http://dbpedia.org/resources/Boleyn_Ground") 
            .put("Britannia Stadium", "http://dbpedia.org/resources/Britannia_Stadium") 
            .put("Emirates Stadium", "http://dbpedia.org/resources/Emirates_Stadium") 
            .put("Etihad Stadium", "http://dbpedia.org/resources/City_of_Manchester_Stadium") 
            .put("Goodison Park", "http://dbpedia.org/resources/Goodison_Park") 
            .put("King Power Stadium", "http://dbpedia.org/resources/King_Power_Stadium") 
            .put("Liberty Stadium", "http://dbpedia.org/resources/Liberty_Stadium") 
            .put("Loftus Road Stadium", "http://dbpedia.org/resources/Loftus_Road") 
            .put("Old Trafford", "http://dbpedia.org/resources/Old_Trafford") 
            .put("Selhurst Park", "http://dbpedia.org/resources/Selhurst_Park") 
            .put("St. James' Park", "http://dbpedia.org/resources/St_James%27_Park") 
            .put("St. Mary's Stadium", "http://dbpedia.org/resources/St_Mary%27s_Stadium") 
            .put("Stadium of Light", "http://dbpedia.org/resources/Stadium_of_Light") 
            .put("Stamford Bridge", "http://dbpedia.org/resources/Stamford_Bridge_(stadium)") 
            .put("The Hawthorns", "http://dbpedia.org/resources/The_Hawthorns") 
            .put("The KC Stadium", "http://dbpedia.org/resources/KC_Stadium") 
            .put("Turf Moor", "http://dbpedia.org/resources/Turf_Moor") 
            .put("Villa Park", "http://dbpedia.org/resources/Villa_Park") 
            .put("White Hart Lane", "http://dbpedia.org/resources/White_Hart_Lane") 
            .put("Ricoh Arena", "http://dbpedia.org/wiki/Ricoh_Arena")
            .put("Schwarzwald-Stadion", "http://dbpedia.org/wiki/Dreisamstadion")
            .put("Carrow Road", "http://dbpedia.org/wiki/Carrow_Road")
            .put("Vicarage Road", "http://dbpedia.org/wiki/Vicarage_Road")
            .put("Vitality Stadium", "http://dbpedia.org/wiki/Dean_Court") // Not a typo; known as Vitality for sponsorship reasons currently
            .put("AFAS Stadion", "http://dbpedia.org/resources/AFAS_Stadion")
            .put("Aker Stadion", "http://dbpedia.org/resources/Aker_Stadion")
            .put("Amsterdam ArenA", "http://dbpedia.org/resources/Amsterdam_Arena")
            .put("Arena Khimki", "http://dbpedia.org/resources/Arena_Khimki")
            .put("Arena Lviv", "http://dbpedia.org/resources/Arena_Lviv")
            .put("Artemio Franchi", "http://dbpedia.org/resources/Stadio_Artemio_Franchi")
            .put("Astana Arena", "http://dbpedia.org/resources/Astana_Arena")
            .put("Atatürk Olympic Stadium", "http://dbpedia.org/resources/Atat%C3%BCrk_Olympic_Stadium")
            .put("Audi Sportpark", "http://dbpedia.org/resources/Audi-Sportpark")
            .put("Bakcell Arena", "http://dbpedia.org/resources/Bakcell_Arena")
            .put("Borisov Arena", "http://dbpedia.org/resources/Borisov_Arena")
            .put("Camp Nou", "http://dbpedia.org/resources/Camp_Nou")
            .put("Constant Vanden Stockstadion", "http://dbpedia.org/resources/Constant_Vanden_Stock_Stadium")
            .put("Dnipro Arena", "http://dbpedia.org/resources/Dnipro-Arena")
            .put("Donbass Arena", "http://dbpedia.org/resources/Donbass_Arena")
            .put("Doosan Arena", "http://dbpedia.org/resources/Doosan_Arena")
            .put("Elbasan Arena", "http://dbpedia.org/resources/Elbasan_Arena")
            .put("El Madrigal", "http://dbpedia.org/resources/Estadio_El_Madrigal")
            .put("Ernst-Happel-Stadion", "http://dbpedia.org/resources/Ernst-Happel-Stadion")
            .put("Estádio da Luz", "http://dbpedia.org/resources/Est%C3%A1dio_da_Luz")
            .put("Estádio do Dragão", "http://dbpedia.org/resources/Est%C3%A1dio_do_Drag%C3%A3o")
            .put("Estádio do Restelo", "http://dbpedia.org/resources/Est%C3%A1dio_do_Restelo")
            .put("Estádio José Alvalade", "http://dbpedia.org/resources/Est%C3%A1dio_Jos%C3%A9_Alvalade")
            .put("Estádio Municipal de Braga", "http://dbpedia.org/resources/Est%C3%A1dio_Municipal_de_Braga")
            .put("Euroborg", "http://dbpedia.org/resources/Euroborg")
            .put("FK Partizan Stadium", "http://dbpedia.org/resources/Partizan_Stadium")
            .put("Generali Arena", "http://dbpedia.org/resources/Generali_Arena")
            .put("Georgios Karaiskakis Stadium", "http://dbpedia.org/resources/Karaiskakis_Stadium")
            .put("Ghelamco Arena", "http://dbpedia.org/resources/Ghelamco_Arena")
            .put("GSP Stadium", "http://dbpedia.org/resources/GSP_Stadium")
            .put("Guzanli Olympic Stadium", "http://dbpedia.org/resources/Guzanli_Olympic_Complex_Stadium")
            .put("Haifa International Stadium", "http://dbpedia.org/resources/Sammy_Ofer_Stadium")
            .put("INEA Stadion", "http://dbpedia.org/resources/Stadion_Miejski_(Pozna%C5%84)")
            .put("Jan Breydelstadion", "http://dbpedia.org/resources/Jan_Breydel_Stadium")
            .put("Juventus Stadium", "http://dbpedia.org/resources/Juventus_Stadium")
            .put("Kazan Arena", "http://dbpedia.org/resources/Kazan_Arena")
            .put("Kiev Olympic Stadium", "http://dbpedia.org/resources/Olimpiyskiy_National_Sports_Complex")
            .put("Kuban Stadium", "http://dbpedia.org/resources/Kuban_Stadium")
            .put("Lerkendal Stadium", "http://dbpedia.org/resources/Lerkendal_Stadion")
            .put("Lokomotiv Stadium, Moscow", "http://dbpedia.org/resources/Lokomotiv_Stadium_(Moscow)")
            .put("Maksimir", "http://dbpedia.org/resources/Stadion_Maksimir")
            .put("Matmut Atlantique", "http://dbpedia.org/resources/Nouveau_Stade_de_Bordeaux")
            .put("MCH Arena", "http://dbpedia.org/resources/MCH_Arena")
            .put("Merck-Stadion am Böllenfalltor", "http://dbpedia.org/resources/Merck-Stadion_am_B%C3%B6llenfalltor")
            .put("Mestalla", "http://dbpedia.org/resources/Mestalla_Stadium")
            .put("Olimpico", "http://dbpedia.org/resources/Stadio_Olimpico")
            .put("Parc des Princes", "http://dbpedia.org/resources/Parc_des_Princes")
            .put("Pepsi Arena", "http://dbpedia.org/resources/Polish_Army_Stadium")
            .put("Petrovski Stadium", "http://dbpedia.org/resources/Petrovsky_Stadium")
            .put("Philips Stadion", "http://dbpedia.org/resources/Philips_Stadion")
            .put("Ramón-Sánchez Pizjuán", "http://dbpedia.org/resources/Ram%C3%B3n_S%C3%A1nchez_Pizju%C3%A1n_Stadium")
            .put("San Mamés", "http://dbpedia.org/resources/San_Mam%C3%A9s_Stadium_(2013)")
            .put("San Paolo", "http://dbpedia.org/resources/Stadio_San_Paolo")
            .put("Santiago Bernabéu", "http://dbpedia.org/resources/Santiago_Bernab%C3%A9u_Stadium")
            .put("Stade de Gerland", "http://dbpedia.org/resources/Stade_de_Gerland")
            .put("Stade Geoffroy-Guichard", "http://dbpedia.org/resources/Stade_Geoffroy-Guichard")
            .put("Stade Louis II", "http://dbpedia.org/resources/Stade_Louis_II")
            .put("Stade Tourbillon", "http://dbpedia.org/resources/Stade_Tourbillon")
            .put("Stade Vélodrome", "http://dbpedia.org/resources/Stade_V%C3%A9lodrome")
            .put("Stadiumi Skënderbeu", "http://dbpedia.org/resources/Sk%C3%ABnderbeu_Stadium")
            .put("St Jakob-Park", "http://dbpedia.org/resources/St._Jakob-Park")
            .put("Swedbank Stadion", "http://dbpedia.org/resources/Swedbank_Stadion")
            .put("Theodoros Kolokotronis Stadium", "http://dbpedia.org/resources/Theodoros_Kolokotronis_Stadium")
            .put("Tofik Bakhramov Stadium", "http://dbpedia.org/resources/Tofiq_Bahramov_Stadium")
            .put("Toumba Stadium", "http://dbpedia.org/resources/Toumba_Stadium")
            .put("Türk Telekom Arena", "http://dbpedia.org/resources/T%C3%BCrk_Telekom_Arena")
            .put("Tynecastle", "http://dbpedia.org/resources/Tynecastle_Stadium")
            .put("Ulker Stadyumu", "http://dbpedia.org/resources/%C5%9E%C3%BCkr%C3%BC_Saraco%C4%9Flu_Stadium")
            .put("U Nisy", "http://dbpedia.org/resources/Stadion_u_Nisy")
            .put("Vicente Calderón", "http://dbpedia.org/resources/Vicente_Calder%C3%B3n_Stadium")
            .put("Volksparkstadion", "http://dbpedia.org/resources/Volksparkstadion")
            .put("WWK ARENA", "http://dbpedia.org/resources/WWK_ARENA")
            .build();
    private static final Map<OptaSportType, Map<String, String>> EVENT_GROUPS_LOOKUP = ImmutableMap.<OptaSportType, Map<String, String>>builder()
            .put(OptaSportType.RUGBY_AVIVA_PREMIERSHIP, ImmutableMap.of(
                    "English Premiership (rugby union)", "http://dbpedia.org/resources/English_Premiership_(rugby_union)", 
                    "Rugby Football", "http://dbpedia.org/resources/Rugby_football"
            ))
            .put(OptaSportType.FOOTBALL_SCOTTISH_PREMIER_LEAGUE, ImmutableMap.of(
                    "Football", "http://dbpedia.org/resources/Football", 
                    "Association Football", "http://dbpedia.org/resources/Association_football", 
                    "Scottish Premier League", "http://dbpedia.org/resources/Scottish_Premier_League"
            ))
            .put(OptaSportType.FOOTBALL_GERMAN_BUNDESLIGA, ImmutableMap.of(
                    "Football", "http://dbpedia.org/resources/Football", 
                    "Association Football", "http://dbpedia.org/resources/Association_football", 
                    "German Bundesliga", "http://dbpedia.org/resources/German_Bundesliga"
            ))
            .put(OptaSportType.FOOTBALL_PREMIER_LEAGUE, ImmutableMap.of(
                    "Football", "http://dbpedia.org/resources/Football", 
                    "Association Football", "http://dbpedia.org/resources/Association_football", 
                    "Premier League", "http://dbpedia.org/resources/Premier_League"
            ))
            .put(OptaSportType.FOOTBALL_CHAMPIONS_LEAGUE, ImmutableMap.of(
                    "Football", "http://dbpedia.org/resources/Football", 
                    "Association Football", "http://dbpedia.org/resources/Association_football", 
                    "UEFA Champions League", "http://dbpedia.org/resources/UEFA_Champions_League"
            ))
            .put(OptaSportType.FOOTBALL_EUROPA_LEAGUE, ImmutableMap.of(
                    "Football", "http://dbpedia.org/resources/Football", 
                    "Association Football", "http://dbpedia.org/resources/Association_football", 
                    "UEFA Europa League", "http://dbpedia.org/resources/UEFA_Europa_League"
            ))
            .put(OptaSportType.FOOTBALL_FA_CUP, ImmutableMap.of(
                    "Football", "http://dbpedia.org/resources/Football",
                    "Association Football", "http://dbpedia.org/resources/Association_football",
                    "FA Cup", "http://dbpedia.org/resources/FA_Cup"
            ))
            .build();
    private final ImmutableMap<OptaSportType, OptaSportConfiguration> config;
    
    public OptaEventsUtility(TopicStore topicStore, Map<OptaSportType, OptaSportConfiguration> config) {
        super(topicStore);
        this.config = ImmutableMap.copyOf(config);
    }

    @Override
    public String createEventUri(String id) {
        return EVENT_URI_BASE + id;
    }

    @Override
    public String createTeamUri(OptaSportType sportType, String id) {
        OptaSportConfiguration sportConfig = config.get(sportType);
        if (sportConfig == null) {
            throw new IllegalArgumentException("Sport type " + sportType.name() + " not configured");
        }
        return TEAM_URI_BASE + normalizeTeamId(id, sportConfig.prefixToStripFromId());
    }
    
    /**
     * Previously we received numeric IDs in the feed. However, when
     * we switched to the opta API from a file, some IDs were prefixed
     * with a value, and others weren't. So as to reference the 
     * previously-created teams, we'll strip the prefix. This is configured
     * on a sport-by-sport basis; see {@link OptaEventsModule}.
     * 
     * @param prefixToStripFromId 
     */
    private String normalizeTeamId(String id, Optional<String> prefixToStrip) {
        if (prefixToStrip.isPresent()
                && id.startsWith(prefixToStrip.get())) {
            return id.substring(prefixToStrip.get().length());
        } else {
            return id;
        }
    }

    @Override
    public Optional<DateTime> createEndTime(OptaSportType sport, DateTime start) {
        Optional<Duration> duration = Optional.fromNullable(DURATION_MAPPING.get(sport));
        if (!duration.isPresent()) {
            return Optional.absent();
        }
        return Optional.of(start.plus(duration.get()));
    }

    @Override
    public Optional<String> fetchLocationUrl(String location) {
        return Optional.fromNullable(VENUE_LOOKUP.get(location));
    }

    @Override
    public Optional<Map<String, String>> fetchEventGroupUrls(OptaSportType sport) {
        return Optional.fromNullable(EVENT_GROUPS_LOOKUP.get(sport));
    }

    /**
     * Fetches an appropriate Joda {@link DateTimeZone} for a given sport, returning
     * Optional.absent() if no mapping is found.
     * <p>
     * This method exists because the timezone information in the Opta feeds is either
     * ambiguous ('BST') or non-existent (the non-soccer feeds). Fortunately all sports
     * ingested thus far are each played within a single timezone.
     * @param sport the sport to fetch a timezone for
     */
    public Optional<DateTimeZone> fetchTimeZone(OptaSportType sport) {
        return Optional.fromNullable(TIMEZONE_MAPPING.get(sport));
    }
}
