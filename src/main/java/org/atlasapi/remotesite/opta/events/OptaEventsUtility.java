package org.atlasapi.remotesite.opta.events;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.events.EventsUtility;
import org.atlasapi.remotesite.opta.events.model.OptaSportType;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;


public class OptaEventsUtility extends EventsUtility<OptaSportType> {
    
    private static final String EVENT_URI_BASE = "http://optasports.com/events/";
    private static final String TEAM_URI_BASE = "http://optasports.com/teams/";

    private static final String COMPETITION_NAMESPACE = "com:optasports:competition";
    private static final Publisher COMPETITION_EVENT_GROUP_PUBLISHER = Publisher.OPTA;

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
    
    private static final Map<String, LocationTitleUri> VENUE_LOOKUP = ImmutableMap.<String, LocationTitleUri>builder()
            .put("Recreation Ground", new LocationTitleUri("Recreation Ground", "http://dbpedia.org/resources/Recreation_Ground_(Bath)"))
            .put("Adams Park", new LocationTitleUri("Adams Park", "http://dbpedia.org/resources/Adams_Park"))
            .put("stadium:mk", new LocationTitleUri("stadium:mk", "http://dbpedia.org/resources/Stadium:mk"))
            .put("Stadium:mk", new LocationTitleUri("stadium:mk", "http://dbpedia.org/resources/Stadium:mk"))
            .put("The Stoop", new LocationTitleUri("The Stoop", "http://dbpedia.org/resources/The_Stoop"))
            .put("Wembley Stadium", new LocationTitleUri("Wembley Stadium", "http://dbpedia.org/resources/Wembley_stadium"))
            .put("Wembley", new LocationTitleUri("Wembley Stadium", "http://dbpedia.org/resources/Wembley_stadium"))
            .put("Welford Road", new LocationTitleUri("Welford Road", "http://dbpedia.org/resources/Welford_Road_Stadium"))
            .put("Twickenham", new LocationTitleUri("Twickenham", "http://dbpedia.org/resources/Twickenham_Stadium"))
            .put("Kassam Stadium", new LocationTitleUri("Kassam Stadium", "http://dbpedia.org/resources/Kassam_Stadium"))
            .put("Kingston Park", new LocationTitleUri("Kingston Park", "http://dbpedia.org/resources/Kingston_Park_(stadium)"))
            .put("Allianz Park", new LocationTitleUri("Allianz Park", "http://dbpedia.org/resources/Barnet_Copthall"))
            .put("Kingsholm", new LocationTitleUri("Kingsholm", "http://dbpedia.org/resources/Kingsholm_Stadium"))
            .put("Madejski Stadium", new LocationTitleUri("Madejski Stadium", "http://dbpedia.org/resources/Madejski_Stadium"))
            .put("AJ Bell Stadium", new LocationTitleUri("AJ Bell Stadium", "http://dbpedia.org/resources/Salford_City_Stadium"))
            .put("Sandy Park", new LocationTitleUri("Sandy Park", "http://dbpedia.org/resources/Sandy_Park"))
            .put("Franklin's Gardens", new LocationTitleUri("Franklin's Gardens", "http://dbpedia.org/resources/Franklin%27s_Gardens"))
            .put("Allianz Arena", new LocationTitleUri("Allianz Arena", "http://dbpedia.org/resources/Allianz_Arena"))
            .put("BayArena", new LocationTitleUri("BayArena", "http://dbpedia.org/resources/BayArena"))
            .put("Benteler-Arena", new LocationTitleUri("Benteler-Arena", "http://dbpedia.org/resources/Benteler_Arena"))
            .put("Borussia-Park", new LocationTitleUri("Borussia-Park", "http://dbpedia.org/resources/Borussia-Park"))
            .put("Celtic Park", new LocationTitleUri("Celtic Park", "http://dbpedia.org/resources/Celtic_Park"))
            .put("Coface Arena", new LocationTitleUri("Coface Arena", "http://dbpedia.org/resources/Coface_Arena"))
            .put("Commerzbank Arena", new LocationTitleUri("Commerzbank Arena", "http://dbpedia.org/resources/Commerzbank-Arena"))
            .put("Dens Park", new LocationTitleUri("Dens Park", "http://dbpedia.org/resources/Dens_Park"))
            .put("Fir Park", new LocationTitleUri("Fir Park", "http://dbpedia.org/resources/Fir_Park"))
            .put("Firhill Stadium", new LocationTitleUri("Firhill Stadium", "http://dbpedia.org/resources/Firhill_Stadium"))
            .put("HDI-Arena", new LocationTitleUri("HDI-Arena", "http://dbpedia.org/resources/HDI-Arena"))
            .put("Imtech Arena", new LocationTitleUri("Imtech Arena", "http://dbpedia.org/resources/Volksparkstadion"))
            .put("MAGE SOLAR Stadion", new LocationTitleUri("MAGE SOLAR Stadion", "http://dbpedia.org/resources/Mage_Solar_Stadion"))
            .put("McDiarmid Park", new LocationTitleUri("McDiarmid Park", "http://dbpedia.org/resources/McDiarmid_Park"))
            .put("Mercedes-Benz Arena", new LocationTitleUri("Mercedes-Benz Arena", "http://dbpedia.org/resources/Mercedes-Benz_Arena_(Stuttgart)"))
            .put("New Douglas Park", new LocationTitleUri("New Douglas Park", "http://dbpedia.org/resources/New_Douglas_Park"))
            .put("Olympiastadion", new LocationTitleUri("Olympiastadion", "http://dbpedia.org/resources/Olympic_Stadium_(Berlin)"))
            .put("Pittodrie", new LocationTitleUri("Pittodrie", "http://dbpedia.org/resources/Pittodrie_Stadium"))
            .put("RheinEnergieStadion", new LocationTitleUri("RheinEnergieStadion", "http://dbpedia.org/resources/RheinEnergieStadion"))
            .put("Rugby Park", new LocationTitleUri("Rugby Park", "http://dbpedia.org/resources/Rugby_Park"))
            .put("SGL Arena", new LocationTitleUri("SGL Arena", "http://dbpedia.org/resources/SGL_arena"))
            .put("Signal Iduna Park", new LocationTitleUri("Signal Iduna Park", "http://dbpedia.org/resources/Signal_Iduna_Park"))
            .put("St Mirren Park", new LocationTitleUri("St Mirren Park", "http://dbpedia.org/resources/St._Mirren_Park"))
            .put("Tannadice Park", new LocationTitleUri("Tannadice Park", "http://dbpedia.org/resources/Tannadice_Park"))
            .put("Tulloch Caledonian Stadium", new LocationTitleUri("Tulloch Caledonian Stadium", "http://dbpedia.org/resources/Caledonian_Stadium"))
            .put("VELTINS-Arena", new LocationTitleUri("VELTINS-Arena", "http://dbpedia.org/resources/Veltins-Arena"))
            .put("Victoria Park, Dingwall", new LocationTitleUri("Victoria Park, Dingwall", "http://dbpedia.org/resources/Victoria_Park,_Dingwall"))
            .put("Volkswagen Arena", new LocationTitleUri("Volkswagen Arena", "http://dbpedia.org/resources/Volkswagen_Arena"))
            .put("WIRSOL Rhein-Neckar-Arena", new LocationTitleUri("WIRSOL Rhein-Neckar-Arena", "http://dbpedia.org/resources/Rhein-Neckar_Arena"))
            .put("Weserstadion", new LocationTitleUri("Weserstadion", "http://dbpedia.org/resources/Weserstadion"))
            .put("Anfield", new LocationTitleUri("Anfield", "http://dbpedia.org/resources/Anfield")) 
            .put("Boleyn Ground", new LocationTitleUri("Boleyn Ground", "http://dbpedia.org/resources/Boleyn_Ground")) 
            .put("Britannia Stadium", new LocationTitleUri("Britannia Stadium", "http://dbpedia.org/resources/Britannia_Stadium")) 
            .put("Emirates Stadium", new LocationTitleUri("Emirates Stadium", "http://dbpedia.org/resources/Emirates_Stadium")) 
            .put("Etihad Stadium", new LocationTitleUri("Etihad Stadium", "http://dbpedia.org/resources/City_of_Manchester_Stadium")) 
            .put("Goodison Park", new LocationTitleUri("Goodison Park", "http://dbpedia.org/resources/Goodison_Park")) 
            .put("King Power Stadium", new LocationTitleUri("King Power Stadium", "http://dbpedia.org/resources/King_Power_Stadium")) 
            .put("Liberty Stadium", new LocationTitleUri("Liberty Stadium", "http://dbpedia.org/resources/Liberty_Stadium")) 
            .put("Loftus Road Stadium", new LocationTitleUri("Loftus Road Stadium", "http://dbpedia.org/resources/Loftus_Road")) 
            .put("Old Trafford", new LocationTitleUri("Old Trafford", "http://dbpedia.org/resources/Old_Trafford")) 
            .put("Selhurst Park", new LocationTitleUri("Selhurst Park", "http://dbpedia.org/resources/Selhurst_Park")) 
            .put("St. James' Park", new LocationTitleUri("St. James' Park", "http://dbpedia.org/resources/St_James%27_Park"))
            .put("St James Park", new LocationTitleUri("St. James' Park", "http://dbpedia.org/resources/St_James%27_Park"))
            .put("St. James Park", new LocationTitleUri("St. James' Park", "http://dbpedia.org/resources/St_James%27_Park"))
            .put("St. Mary's Stadium", new LocationTitleUri("St. Mary's Stadium", "http://dbpedia.org/resources/St_Mary%27s_Stadium"))
            .put("Stadium of Light", new LocationTitleUri("Stadium of Light", "http://dbpedia.org/resources/Stadium_of_Light")) 
            .put("Stamford Bridge", new LocationTitleUri("Stamford Bridge", "http://dbpedia.org/resources/Stamford_Bridge_(stadium)")) 
            .put("The Hawthorns", new LocationTitleUri("The Hawthorns", "http://dbpedia.org/resources/The_Hawthorns")) 
            .put("The KC Stadium", new LocationTitleUri("The KC Stadium", "http://dbpedia.org/resources/KC_Stadium")) 
            .put("Turf Moor", new LocationTitleUri("Turf Moor", "http://dbpedia.org/resources/Turf_Moor")) 
            .put("Villa Park", new LocationTitleUri("Villa Park", "http://dbpedia.org/resources/Villa_Park")) 
            .put("White Hart Lane", new LocationTitleUri("White Hart Lane", "http://dbpedia.org/resources/White_Hart_Lane")) 
            .put("Ricoh Arena", new LocationTitleUri("Ricoh Arena", "http://dbpedia.org/wiki/Ricoh_Arena"))
            .put("Schwarzwald-Stadion", new LocationTitleUri("Schwarzwald-Stadion", "http://dbpedia.org/wiki/Dreisamstadion"))
            .put("Carrow Road", new LocationTitleUri("Carrow Road", "http://dbpedia.org/wiki/Carrow_Road"))
            .put("Vicarage Road", new LocationTitleUri("Vicarage Road", "http://dbpedia.org/wiki/Vicarage_Road"))
            .put("Vitality Stadium", new LocationTitleUri("Vitality Stadium", "http://dbpedia.org/wiki/Dean_Court")) // Not a typo; known as Vitality for sponsorship reasons currently
            .put("AFAS Stadion", new LocationTitleUri("AFAS Stadion", "http://dbpedia.org/resources/AFAS_Stadion"))
            .put("Aker Stadion", new LocationTitleUri("Aker Stadion", "http://dbpedia.org/resources/Aker_Stadion"))
            .put("Amsterdam ArenA", new LocationTitleUri("Amsterdam ArenA", "http://dbpedia.org/resources/Amsterdam_Arena"))
            .put("Arena Khimki", new LocationTitleUri("Arena Khimki", "http://dbpedia.org/resources/Arena_Khimki"))
            .put("Arena Lviv", new LocationTitleUri("Arena Lviv", "http://dbpedia.org/resources/Arena_Lviv"))
            .put("Artemio Franchi", new LocationTitleUri("Artemio Franchi", "http://dbpedia.org/resources/Stadio_Artemio_Franchi"))
            .put("Astana Arena", new LocationTitleUri("Astana Arena", "http://dbpedia.org/resources/Astana_Arena"))
            .put("Atatürk Olympic Stadium", new LocationTitleUri("Atatürk Olympic Stadium", "http://dbpedia.org/resources/Atat%C3%BCrk_Olympic_Stadium"))
            .put("Audi Sportpark", new LocationTitleUri("Audi Sportpark", "http://dbpedia.org/resources/Audi-Sportpark"))
            .put("Bakcell Arena", new LocationTitleUri("Bakcell Arena", "http://dbpedia.org/resources/Bakcell_Arena"))
            .put("Borisov Arena", new LocationTitleUri("Borisov Arena", "http://dbpedia.org/resources/Borisov_Arena"))
            .put("Camp Nou", new LocationTitleUri("Camp Nou", "http://dbpedia.org/resources/Camp_Nou"))
            .put("Constant Vanden Stockstadion", new LocationTitleUri("Constant Vanden Stockstadion", "http://dbpedia.org/resources/Constant_Vanden_Stock_Stadium"))
            .put("Dnipro Arena", new LocationTitleUri("Dnipro Arena", "http://dbpedia.org/resources/Dnipro-Arena"))
            .put("Donbass Arena", new LocationTitleUri("Donbass Arena", "http://dbpedia.org/resources/Donbass_Arena"))
            .put("Doosan Arena", new LocationTitleUri("Doosan Arena", "http://dbpedia.org/resources/Doosan_Arena"))
            .put("Elbasan Arena", new LocationTitleUri("Elbasan Arena", "http://dbpedia.org/resources/Elbasan_Arena"))
            .put("El Madrigal", new LocationTitleUri("El Madrigal", "http://dbpedia.org/resources/Estadio_El_Madrigal"))
            .put("Ernst-Happel-Stadion", new LocationTitleUri("Ernst-Happel-Stadion", "http://dbpedia.org/resources/Ernst-Happel-Stadion"))
            .put("Estádio da Luz", new LocationTitleUri("Estádio da Luz", "http://dbpedia.org/resources/Est%C3%A1dio_da_Luz"))
            .put("Estádio do Dragão", new LocationTitleUri("Estádio do Dragão", "http://dbpedia.org/resources/Est%C3%A1dio_do_Drag%C3%A3o"))
            .put("Estádio do Restelo", new LocationTitleUri("Estádio do Restelo", "http://dbpedia.org/resources/Est%C3%A1dio_do_Restelo"))
            .put("Estádio José Alvalade", new LocationTitleUri("Estádio José Alvalade", "http://dbpedia.org/resources/Est%C3%A1dio_Jos%C3%A9_Alvalade"))
            .put("Estádio Municipal de Braga", new LocationTitleUri("Estádio Municipal de Braga", "http://dbpedia.org/resources/Est%C3%A1dio_Municipal_de_Braga"))
            .put("Euroborg", new LocationTitleUri("Euroborg", "http://dbpedia.org/resources/Euroborg"))
            .put("FK Partizan Stadium", new LocationTitleUri("FK Partizan Stadium", "http://dbpedia.org/resources/Partizan_Stadium"))
            .put("Generali Arena", new LocationTitleUri("Generali Arena", "http://dbpedia.org/resources/Generali_Arena"))
            .put("Georgios Karaiskakis Stadium", new LocationTitleUri("Georgios Karaiskakis Stadium", "http://dbpedia.org/resources/Karaiskakis_Stadium"))
            .put("Ghelamco Arena", new LocationTitleUri("Ghelamco Arena", "http://dbpedia.org/resources/Ghelamco_Arena"))
            .put("GSP Stadium", new LocationTitleUri("GSP Stadium", "http://dbpedia.org/resources/GSP_Stadium"))
            .put("Guzanli Olympic Stadium", new LocationTitleUri("Guzanli Olympic Stadium", "http://dbpedia.org/resources/Guzanli_Olympic_Complex_Stadium"))
            .put("Haifa International Stadium", new LocationTitleUri("Haifa International Stadium", "http://dbpedia.org/resources/Sammy_Ofer_Stadium"))
            .put("INEA Stadion", new LocationTitleUri("INEA Stadion", "http://dbpedia.org/resources/Stadion_Miejski_(Pozna%C5%84)"))
            .put("Jan Breydelstadion", new LocationTitleUri("Jan Breydelstadion", "http://dbpedia.org/resources/Jan_Breydel_Stadium"))
            .put("Juventus Stadium", new LocationTitleUri("Juventus Stadium", "http://dbpedia.org/resources/Juventus_Stadium"))
            .put("Kazan Arena", new LocationTitleUri("Kazan Arena", "http://dbpedia.org/resources/Kazan_Arena"))
            .put("Kiev Olympic Stadium", new LocationTitleUri("Kiev Olympic Stadium", "http://dbpedia.org/resources/Olimpiyskiy_National_Sports_Complex"))
            .put("Kuban Stadium", new LocationTitleUri("Kuban Stadium", "http://dbpedia.org/resources/Kuban_Stadium"))
            .put("Lerkendal Stadium", new LocationTitleUri("Lerkendal Stadium", "http://dbpedia.org/resources/Lerkendal_Stadion"))
            .put("Lokomotiv Stadium, Moscow", new LocationTitleUri("Lokomotiv Stadium, Moscow", "http://dbpedia.org/resources/Lokomotiv_Stadium_(Moscow)"))
            .put("Maksimir", new LocationTitleUri("Maksimir", "http://dbpedia.org/resources/Stadion_Maksimir"))
            .put("Matmut Atlantique", new LocationTitleUri("Matmut Atlantique", "http://dbpedia.org/resources/Nouveau_Stade_de_Bordeaux"))
            .put("MCH Arena", new LocationTitleUri("MCH Arena", "http://dbpedia.org/resources/MCH_Arena"))
            .put("Merck-Stadion am Böllenfalltor", new LocationTitleUri("Merck-Stadion am Böllenfalltor", "http://dbpedia.org/resources/Merck-Stadion_am_B%C3%B6llenfalltor"))
            .put("Mestalla", new LocationTitleUri("Mestalla", "http://dbpedia.org/resources/Mestalla_Stadium"))
            .put("Olimpico", new LocationTitleUri("Olimpico", "http://dbpedia.org/resources/Stadio_Olimpico"))
            .put("Parc des Princes", new LocationTitleUri("Parc des Princes", "http://dbpedia.org/resources/Parc_des_Princes"))
            .put("Pepsi Arena", new LocationTitleUri("Pepsi Arena", "http://dbpedia.org/resources/Polish_Army_Stadium"))
            .put("Petrovski Stadium", new LocationTitleUri("Petrovski Stadium", "http://dbpedia.org/resources/Petrovsky_Stadium"))
            .put("Philips Stadion", new LocationTitleUri("Philips Stadion", "http://dbpedia.org/resources/Philips_Stadion"))
            .put("Ramón-Sánchez Pizjuán", new LocationTitleUri("Ramón-Sánchez Pizjuán", "http://dbpedia.org/resources/Ram%C3%B3n_S%C3%A1nchez_Pizju%C3%A1n_Stadium"))
            .put("San Mamés", new LocationTitleUri("San Mamés", "http://dbpedia.org/resources/San_Mam%C3%A9s_Stadium_(2013)"))
            .put("San Paolo", new LocationTitleUri("San Paolo", "http://dbpedia.org/resources/Stadio_San_Paolo"))
            .put("Santiago Bernabéu", new LocationTitleUri("Santiago Bernabéu", "http://dbpedia.org/resources/Santiago_Bernab%C3%A9u_Stadium"))
            .put("Stade de Gerland", new LocationTitleUri("Stade de Gerland", "http://dbpedia.org/resources/Stade_de_Gerland"))
            .put("Stade Geoffroy-Guichard", new LocationTitleUri("Stade Geoffroy-Guichard", "http://dbpedia.org/resources/Stade_Geoffroy-Guichard"))
            .put("Stade Louis II", new LocationTitleUri("Stade Louis II", "http://dbpedia.org/resources/Stade_Louis_II"))
            .put("Stade Tourbillon", new LocationTitleUri("Stade Tourbillon", "http://dbpedia.org/resources/Stade_Tourbillon"))
            .put("Stade Vélodrome", new LocationTitleUri("Stade Vélodrome", "http://dbpedia.org/resources/Stade_V%C3%A9lodrome"))
            .put("Stadiumi Skënderbeu", new LocationTitleUri("Stadiumi Skënderbeu", "http://dbpedia.org/resources/Sk%C3%ABnderbeu_Stadium"))
            .put("St Jakob-Park", new LocationTitleUri("St Jakob-Park", "http://dbpedia.org/resources/St._Jakob-Park"))
            .put("Swedbank Stadion", new LocationTitleUri("Swedbank Stadion", "http://dbpedia.org/resources/Swedbank_Stadion"))
            .put("Theodoros Kolokotronis Stadium", new LocationTitleUri("Theodoros Kolokotronis Stadium", "http://dbpedia.org/resources/Theodoros_Kolokotronis_Stadium"))
            .put("Tofik Bakhramov Stadium", new LocationTitleUri("Tofik Bakhramov Stadium", "http://dbpedia.org/resources/Tofiq_Bahramov_Stadium"))
            .put("Toumba Stadium", new LocationTitleUri("Toumba Stadium", "http://dbpedia.org/resources/Toumba_Stadium"))
            .put("Türk Telekom Arena", new LocationTitleUri("Türk Telekom Arena", "http://dbpedia.org/resources/T%C3%BCrk_Telekom_Arena"))
            .put("Tynecastle", new LocationTitleUri("Tynecastle", "http://dbpedia.org/resources/Tynecastle_Stadium"))
            .put("Ulker Stadyumu", new LocationTitleUri("Ulker Stadyumu", "http://dbpedia.org/resources/%C5%9E%C3%BCkr%C3%BC_Saraco%C4%9Flu_Stadium"))
            .put("U Nisy", new LocationTitleUri("U Nisy", "http://dbpedia.org/resources/Stadion_u_Nisy"))
            .put("Vicente Calderón", new LocationTitleUri("Vicente Calderón", "http://dbpedia.org/resources/Vicente_Calder%C3%B3n_Stadium"))
            .put("Volksparkstadion", new LocationTitleUri("Volksparkstadion", "http://dbpedia.org/resources/Volksparkstadion"))
            .put("WWK ARENA", new LocationTitleUri("WWK ARENA", "http://dbpedia.org/resources/WWK_ARENA"))
            .put("Sixways", new LocationTitleUri("Sixways", "http://dbpedia.org/resources/Sixways_Stadium"))
            .put("Ewood Park", new LocationTitleUri("Ewood Park", "http://dbpedia.org/resource/Ewood_Park"))
            .put("The Keepmoat Stadium", new LocationTitleUri("Keepmoat Stadium", "http://dbpedia.org/resource/Keepmoat_Stadium"))
            .put("Silverlake Stadium", new LocationTitleUri("Ten Acres", "http://dbpedia.org/resource/Ten_Acres"))
            .put("The Victoria Ground", new LocationTitleUri("Victoria Park (Hartlepool)", "http://dbpedia.org/resource/Victoria_Park_(Hartlepool)")) // Ambigous data from Opta
            .put("John Smith's Stadium", new LocationTitleUri("John Smith's Stadium", "http://dbpedia.org/resource/John_Smith%27s_Stadium"))
            .put("Greenhous Meadow", new LocationTitleUri("New Meadow", "http://dbpedia.org/resource/New_Meadow"))

            .put("ABAX Stadium", new LocationTitleUri("London Road Stadium", "http://dbpedia.org/resource/London_Road_Stadium"))
            .put("Brunton Park", new LocationTitleUri("Brunton Park", "http://dbpedia.org/resource/Brunton_Park"))
            .put("Fratton Park", new LocationTitleUri("Fratton Park", "http://dbpedia.org/resource/Fratton_Park"))
            .put("The Kassam Stadium", new LocationTitleUri("Kassam Stadium", "http://dbpedia.org/resource/Kassam_Stadium"))
            .put("City Ground", new LocationTitleUri("City Ground", "http://dbpedia.org/resource/City_Ground"))
            .put("Weston Homes Community Stadium", new LocationTitleUri("Colchester Community Stadium", "http://dbpedia.org/resource/Colchester_Community_Stadium"))
            .put("Gigg Lane", new LocationTitleUri("Gigg Lane", "http://dbpedia.org/resource/Gigg_Lane"))
            .put("Macron Stadium", new LocationTitleUri("Macron Stadium", "http://dbpedia.org/resource/Macron_Stadium"))
            .put("iPro Stadium", new LocationTitleUri("Pride Park Stadium", "http://dbpedia.org/resource/Pride_Park_Stadium"))
            .put("Huish Park", new LocationTitleUri("Huish Park", "http://dbpedia.org/resource/Huish_Park"))

            .put("Kingsmeadow Stadium", new LocationTitleUri("Kingsmeadow", "http://dbpedia.org/resource/Kingsmeadow"))
            .put("Wham Stadium", new LocationTitleUri("Crown Ground", "http://dbpedia.org/resource/Crown_Ground"))
            .put("Moss Lane", new LocationTitleUri("Moss Lane", "http://dbpedia.org/resource/Moss_Lane"))
            .put("The Hive Stadium", new LocationTitleUri("The Hive Stadium", "http://dbpedia.org/resource/The_Hive_Stadium"))
//            .put("Kirkby Road", new LocationTitleUri("", "http://dbpedia.org/resource/")) // https://en.wikipedia.org/wiki/Barwell_F.C.
            .put("Pirelli Stadium", new LocationTitleUri("Pirelli Stadium", "http://dbpedia.org/resource/Pirelli_Stadium"))
            .put("Abbey Stadium", new LocationTitleUri("Abbey Stadium", "http://dbpedia.org/resource/Abbey_Stadium"))
            .put("Broadfield Stadium", new LocationTitleUri("Broadfield Stadium", "http://dbpedia.org/resource/Broadfield_Stadium"))
            .put("Alexandra Stadium", new LocationTitleUri("Gresty Road", "http://dbpedia.org/resource/Gresty_Road"))
            .put("Victoria Road", new LocationTitleUri("Victoria Road", "http://dbpedia.org/resource/Victoria_Road_(Dagenham)"))
            .put("Perry's Crabble Stadium", new LocationTitleUri("Crabble Athletic Ground", "http://dbpedia.org/resource/Crabble_Athletic_Ground"))
            .put("Blundell Park", new LocationTitleUri("Blundell Park", "http://dbpedia.org/resource/Blundell_Park"))
            .put("Matchroom Stadium", new LocationTitleUri("Brisbane Road", "http://dbpedia.org/resource/Brisbane_Road"))
            .put("Field Mill", new LocationTitleUri("Field Mill", "http://dbpedia.org/resource/Field_Mill"))
            .put("The New Den", new LocationTitleUri("The Den", "http://dbpedia.org/resource/The_Den"))
            .put("Wincham Park", new LocationTitleUri("Wincham_Park", "http://dbpedia.org/resource/Wincham_Park"))
            .put("Home Park", new LocationTitleUri("Home Park", "http://dbpedia.org/resource/Home_Park"))
            .put("Spotland", new LocationTitleUri("Spotland Stadium", "http://dbpedia.org/resource/Spotland_Stadium"))
            .put("Glanford Park", new LocationTitleUri("Glanford Park", "http://dbpedia.org/resource/Glanford_Park"))
            .put("Bramall Lane", new LocationTitleUri("Bramall Lane", "http://dbpedia.org/resource/Bramall_Lane"))
            .put("Ashton Gate", new LocationTitleUri("Ashton Gate Stadium", "http://dbpedia.org/resource/Ashton_Gate_Stadium"))
            .put("Banks's Stadium", new LocationTitleUri("Bescot Stadium", "http://dbpedia.org/resource/Bescot_Stadium"))
            .put("Bloomfield Road", new LocationTitleUri("Bloomfield Road", "http://dbpedia.org/resource/Bloomfield_Road"))
            .put("Boundary Park", new LocationTitleUri("Boundary Park", "http://dbpedia.org/resource/Boundary_Park"))
            .put("Broadhurst Park", new LocationTitleUri("Broadhurst Park", "http://dbpedia.org/resource/Broadhurst_Park"))
            .put("Cardiff City Stadium", new LocationTitleUri("Cardiff City Stadium", "http://dbpedia.org/resource/Cardiff_City_Stadium"))
            .put("Elland Road", new LocationTitleUri("Elland Road", "http://dbpedia.org/resource/Elland_Road"))
            .put("Gallagher Stadium", new LocationTitleUri("Gallagher Stadium", "http://dbpedia.org/resource/Gallagher_Stadium"))
            .put("Giuseppe Meazza", new LocationTitleUri("San Siro", "http://dbpedia.org/resource/San_Siro"))
            .put("Globe Arena", new LocationTitleUri("Globe Arena", "http://dbpedia.org/resource/Globe_Arena_(football_stadium)"))
            .put("Griffin Park", new LocationTitleUri("Griffin Park", "http://dbpedia.org/resource/Griffin_Park"))
//            .put("Grosvenor Vale", new LocationTitleUri("", "http://dbpedia.org/resource/")) // https://en.wikipedia.org/wiki/Wealdstone_F.C.
            .put("Hillsborough", new LocationTitleUri("Hillsborough Stadium", "http://dbpedia.org/resource/Hillsborough_Stadium"))
//            .put("Kirkby Road", new LocationTitleUri("", "http://dbpedia.org/resource/"))) // https://en.wikipedia.org/wiki/Barwell_F.C.
            .put("Lamex Stadium", new LocationTitleUri("Broadhall Way", "http://dbpedia.org/resource/Broadhall_Way"))
            .put("Meadow Park", new LocationTitleUri("Meadow Park", "http://dbpedia.org/resource/Meadow_Park_(Borehamwood)"))
//            .put("Moor Lane", new LocationTitleUri("", "http://dbpedia.org/resource/"))) // https://en.wikipedia.org/wiki/Salford_City_F.C.
//            .put("NPower Loop Meadow Stadium", new LocationTitleUri("", "http://dbpedia.org/resource/")) // https://en.wikipedia.org/wiki/Didcot_Town_F.C.
            .put("Park View Road", new LocationTitleUri("Park View Road", "http://dbpedia.org/resource/Park_View_Road"))
            .put("Portman Road", new LocationTitleUri("Portman Road", "http://dbpedia.org/resource/Portman_Road"))
            .put("Proact Stadium", new LocationTitleUri("Proact Stadium", "http://dbpedia.org/resource/Proact_Stadium"))
            .put("Riverside Stadium", new LocationTitleUri("Riverside Stadium", "http://dbpedia.org/resource/Riverside_Stadium"))
            .put("Rodney Parade", new LocationTitleUri("Rodney Parade", "http://dbpedia.org/resource/Rodney_Parade"))
            .put("Sixfields Stadium", new LocationTitleUri("Sixfields Stadium", "http://dbpedia.org/resource/Sixfields_Stadium"))
            .put("St. Andrew's Stadium", new LocationTitleUri("St Andrew's", "http://dbpedia.org/resource/St_Andrew%27s_(stadium)"))
            .put("The Avanti Stadium", new LocationTitleUri("Cressing Road", "http://dbpedia.org/resource/Cressing_Road"))
            .put("The Coral Windows Stadium", new LocationTitleUri("Valley Parade", "http://dbpedia.org/resource/Valley_Parade"))
//            .put("The Enclosed Ground", new LocationTitleUri("The Enclosed Ground", "http://dbpedia.org/resource/"))) // CANNOT BE MATCHED https://en.wikipedia.org/wiki/Whitehawk_F.C.
            .put("The Memorial Stadium", new LocationTitleUri("Memorial Stadium", "http://dbpedia.org/resource/Memorial_Stadium_(Bristol)"))
            .put("The Northolme", new LocationTitleUri("The Northolme", "http://dbpedia.org/resource/The_Northolme"))
            .put("The Recreation Ground", new LocationTitleUri("The Recreation Ground", "http://dbpedia.org/resource/Recreation_Ground_(Aldershot)"))
            .put("The Shay Stadium", new LocationTitleUri("The Shay", "http://dbpedia.org/resource/The_Shay"))
            .put("Vale Park", new LocationTitleUri("Vale Park", "http://dbpedia.org/resource/Vale_Park"))
            .put("War Memorial Athletic Ground", new LocationTitleUri("War Memorial Athletic Ground", "http://dbpedia.org/resource/War_Memorial_Athletic_Ground"))
            .put("York Road", new LocationTitleUri("York Road", "http://dbpedia.org/resource/York_Road_(stadium)"))
            .build();
    private static final Map<OptaSportType, List<EventGroup>> EVENT_GROUPS_LOOKUP = ImmutableMap.
            <OptaSportType, List<EventGroup>>builder()
            .put(OptaSportType.RUGBY_AVIVA_PREMIERSHIP, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Rugby Football",
                            "http://dbpedia.org/resources/Rugby_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "English Premiership (rugby union)",
                            "http://dbpedia.org/resources/English_Premiership_(rugby_union)"
                    ),
                    EventGroup.of(
                            "English Premiership (rugby union)",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/English_Premiership_(rugby_union)",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
                ))
            .put(OptaSportType.FOOTBALL_SCOTTISH_PREMIER_LEAGUE, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Football",
                            "http://dbpedia.org/resources/Football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Association Football",
                            "http://dbpedia.org/resources/Association_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Scottish Premier League",
                            "http://dbpedia.org/resources/Scottish_Premier_League"
                    ),
                    EventGroup.of(
                            "Scottish Premier League",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/Scottish_Premier_League",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
            ))
            .put(OptaSportType.FOOTBALL_GERMAN_BUNDESLIGA, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Football",
                            "http://dbpedia.org/resources/Football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Association Football",
                            "http://dbpedia.org/resources/Association_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "German Bundesliga",
                            "http://dbpedia.org/resources/German_Bundesliga"
                    ),
                    EventGroup.of(
                            "German Bundesliga",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/German_Bundesliga",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
            ))
            .put(OptaSportType.FOOTBALL_PREMIER_LEAGUE, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Football",
                            "http://dbpedia.org/resources/Football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Association Football",
                            "http://dbpedia.org/resources/Association_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Premier League",
                            "http://dbpedia.org/resources/Premier_League"
                    ),
                    EventGroup.of(
                            "Premier League",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/Premier_League",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
            ))
            .put(OptaSportType.FOOTBALL_CHAMPIONS_LEAGUE, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Football",
                            "http://dbpedia.org/resources/Football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Association Football",
                            "http://dbpedia.org/resources/Association_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "UEFA Champions League",
                            "http://dbpedia.org/resources/UEFA_Champions_League"
                    ),
                    EventGroup.of(
                            "UEFA Champions League",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/UEFA_Champions_League",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
            ))
            .put(OptaSportType.FOOTBALL_EUROPA_LEAGUE, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Football",
                            "http://dbpedia.org/resources/Football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Association Football",
                            "http://dbpedia.org/resources/Association_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "UEFA Europa League",
                            "http://dbpedia.org/resources/UEFA_Europa_League"
                    ),
                    EventGroup.of(
                            "UEFA Europa League",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/UEFA_Europa_League",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
            ))
            .put(OptaSportType.FOOTBALL_FA_CUP, ImmutableList.of(
                    EventGroup.ofDefaultNs(
                            "Football",
                            "http://dbpedia.org/resources/Football"
                    ),
                    EventGroup.ofDefaultNs(
                            "Association Football",
                            "http://dbpedia.org/resources/Association_football"
                    ),
                    EventGroup.ofDefaultNs(
                            "FA Cup",
                            "http://dbpedia.org/resources/FA_Cup"
                    ),
                    EventGroup.of(
                            "FA Cup",
                            COMPETITION_NAMESPACE,
                            "http://optasports.com/competition/FA_Cup",
                            COMPETITION_EVENT_GROUP_PUBLISHER
                    )
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
    public Optional<LocationTitleUri> fetchLocationUrl(String location) {
        return Optional.fromNullable(VENUE_LOOKUP.get(location));
    }

    @Override
    public Optional<List<EventGroup>> fetchEventGroupUrls(OptaSportType sport) {
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
