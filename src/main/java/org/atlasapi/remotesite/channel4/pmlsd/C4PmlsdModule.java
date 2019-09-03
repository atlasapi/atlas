package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.channel4.pirate.C4PirateClient;
import org.atlasapi.remotesite.channel4.pirate.C4PirateForceIngestController;
import org.atlasapi.remotesite.channel4.pirate.C4PirateItemTransformer;
import org.atlasapi.remotesite.channel4.pmlsd.epg.C4EpgChannelDayUpdater;
import org.atlasapi.remotesite.channel4.pmlsd.epg.C4EpgClient;
import org.atlasapi.remotesite.channel4.pmlsd.epg.C4EpgEntryUriExtractor;
import org.atlasapi.remotesite.channel4.pmlsd.epg.C4EpgUpdater;
import org.atlasapi.remotesite.channel4.pmlsd.epg.ScheduleResolverBroadcastTrimmer;
import org.atlasapi.reporting.OwlReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.time.DayRangeGenerator;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class C4PmlsdModule {
    
    static final Publisher SOURCE = Publisher.C4_PMLSD;
    
    private static final Logger log = LoggerFactory.getLogger(C4PmlsdModule.class);

    private static final String ATOZ_BASE = "https://pmlsc.channel4.com/pmlsd/";
    private static final String P06_PLATFORM = "p06";

	private final static RepetitionRule BRAND_UPDATE_TIME = RepetitionRules.daily(new LocalTime(2, 0, 0));
	private final static RepetitionRule XBOX_UPDATE_TIME = RepetitionRules.daily(new LocalTime(1, 0, 0));
	private final static RepetitionRule TWO_HOURS = RepetitionRules.every(Duration.standardHours(2));
	private final static RepetitionRule TWO_HOURS_WITH_OFFSET = RepetitionRules.every(Duration.standardHours(2)).withOffset(Duration.standardHours(1));

    private @Autowired SimpleScheduler scheduler;

	private @Autowired @Qualifier("contentResolver") ContentResolver contentResolver;
	private @Autowired @Qualifier("contentWriter") ContentWriter contentWriter;
	private @Autowired ScheduleResolver scheduleResolver;
	private @Autowired ScheduleWriter scheduleWriter;
	private @Autowired ChannelResolver channelResolver;
	
	private @Value("${updaters.c4pmlsd.enabled}") Boolean tasksEnabled;
	private @Value("${c4.keystore.path}") String keyStorePath;
	private @Value("${c4.keystore.password}") String keyStorePass;

    private @Value("${c4.auth.key}") String authHeaderKey;
    private @Value("${c4.auth.password}") String authHeaderValue;
	
    private @Value("${service.web.id}") Long webServiceId;
    private @Value("${service.ios.id}") Long iOsServiceId;
    private @Value("${player.4od.id}") Long fourODPlayerId;

    private @Value("${c4.pirate.url}") String c4PirateUrl;
    private @Value("${c4.pirate.username}") String c4PirateUser;
    private @Value("${c4.pirate.password}") String c4PiratePass;
	
	public static Map<Publisher, String> PUBLISHER_TO_CANONICAL_URI_HOST_MAP 
	    = ImmutableMap.of(Publisher.C4_PMLSD, "pmlsc.channel4.com",
	                      Publisher.C4_PMLSD_P06, "p06.pmlsc.channel4.com");
	        
    @PostConstruct
    public void startBackgroundTasks() {
        if (tasksEnabled) {
            scheduler.schedule(pcC4Pmlsd15DayEpgUpdater().withName("C4 PMLSD Epg PC Updater (15 day)"), TWO_HOURS);
            scheduler.schedule(pcC4PmlsdLastMonthEpgUpdater().withName("C4 PMLSD Epg PC Updater (Last Month)"), RepetitionRules.NEVER);
            scheduler.schedule(pcC4PmlsdAtozUpdater().withName("C4 PMLSD 4OD PC Updater"), BRAND_UPDATE_TIME);
            //scheduler.schedule(xbox4PmlsdEpgUpdater().withName("C4 PMLSC Epg XBox Updater (15 day)"), TWO_HOURS_WITH_OFFSET);
            scheduler.schedule(xboxC4PmlsdAtozUpdater().withName("C4 PMLSD 4OD XBox Updater"), XBOX_UPDATE_TIME);
            log.info("C4 update scheduled tasks installed");
        }
    }
    
    @Bean protected SimpleHttpClient c4HttpsClient() {
        if (Strings.isNullOrEmpty(keyStorePass) || Strings.isNullOrEmpty(keyStorePath)) {
            if (tasksEnabled) {
                throw new RuntimeException("Check c4.keystore.path or c4.keystore.password");
            } else {
                return UnconfiguredSimpleHttpClient.get();
            }
        }
        try {
            return new SimpleHttpClientBuilder()
                    .withUserAgent(HttpClients.ATLAS_USER_AGENT)
                    .withSocketTimeout(30, TimeUnit.SECONDS)
                    .withSslSocketFactory(SSLSocketFactory.getSocketFactory())
                    .withHeader(authHeaderKey, authHeaderValue)
                    .withTrustUnverifiedCerts()
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean C4AtomApi atomPmlsdApi() {
        return new C4AtomApi(channelResolver);
    }
    
	private C4EpgUpdater pcC4Pmlsd15DayEpgUpdater() {
        OwlReporter owlReporter = getOwlReporter();
        return new C4EpgUpdater(
	            atomPmlsdApi(),
	            pcC4PlmsdEpgChannelDayUpdater(owlReporter),
	            new DayRangeGenerator().withLookAhead(7).withLookBack(7),
                owlReporter
        );
    }

    private C4EpgUpdater pcC4PmlsdLastMonthEpgUpdater() {
        OwlReporter owlReporter = getOwlReporter();
        return new C4EpgUpdater(
                atomPmlsdApi(),
                pcC4PlmsdEpgChannelDayUpdater(owlReporter),
                new DayRangeGenerator().withLookAhead(0).withLookBack(31),
                owlReporter
        );
    }

    private C4EpgChannelDayUpdater pcC4PlmsdEpgChannelDayUpdater(OwlReporter owlReporter) {
	    ScheduleResolverBroadcastTrimmer trimmer = new ScheduleResolverBroadcastTrimmer(SOURCE, scheduleResolver, contentResolver, contentWriter);
        return new C4EpgChannelDayUpdater(
                new C4EpgClient(c4HttpsClient()),
                pmlsdLastUpdatedSettingContentWriter(owlReporter),
                contentResolver,
                pcPmlsdBrandFetcher(Optional.empty(), Optional.empty(), owlReporter),
                trimmer,
                scheduleWriter,
                Publisher.C4_PMLSD,
                new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4EpgEntryUriExtractor()),
                Optional.empty(),
                c4PCLocationPolicyIds()
        );
	}
	
// Disabling the P06 EPG updater, since it doesn't contain hierarchy links if the programme is
// not available
//
//	@Bean public C4EpgUpdater xbox4PmlsdEpgUpdater() {
//        return new C4EpgUpdater(atomPmlsdApi(), xboxC4PlmsdEpgChannelDayUpdater(), new DayRangeGenerator().withLookAhead(7).withLookBack(7));
//    }
//    
//    @Bean public C4EpgChannelDayUpdater xboxC4PlmsdEpgChannelDayUpdater() {
//        ScheduleResolverBroadcastTrimmer trimmer = new ScheduleResolverBroadcastTrimmer(SOURCE, scheduleResolver, contentResolver, pmlsdLastUpdatedSettingContentWriter());
//        return new C4EpgChannelDayUpdater(new C4EpgClient(c4HttpsClient()), pmlsdLastUpdatedSettingContentWriter(),
//                contentResolver, xboxPmlsdBrandFetcher(Optional.of(Platform.XBOX),Optional.of(P06_PLATFORM)), trimmer, 
//                Publisher.C4_PMLSD_P06, new SourceSpecificContentFactory<>(Publisher.C4_PMLSD_P06, new C4EpgEntryUriExtractor()), 
//                Optional.of(P06_PLATFORM));
//    }
    
	private C4AtoZAtomContentUpdateTask pcC4PmlsdAtozUpdater() {
        OwlReporter owlReporter = getAtoZOwlReporter();
        return new C4AtoZAtomContentUpdateTask(
		        c4HttpsClient(),
		        ATOZ_BASE,
		        pcPmlsdBrandFetcher(Optional.empty(), Optional.empty(), owlReporter),
		        Publisher.C4_PMLSD,
                owlReporter
        );
	}
	
	private C4AtoZAtomContentUpdateTask xboxC4PmlsdAtozUpdater() {
        OwlReporter owlReporter = getAtoZOwlReporter();
        return new C4AtoZAtomContentUpdateTask(
	            c4HttpsClient(),
	            ATOZ_BASE,
	            Optional.of(P06_PLATFORM),
	            xboxPmlsdBrandFetcher(Optional.of(Platform.XBOX), Optional.of(P06_PLATFORM), owlReporter),
	            Publisher.C4_PMLSD_P06,
                owlReporter
        );
	}
	
	private C4AtomBackedBrandUpdater pcPmlsdBrandFetcher(
	        Optional<Platform> platform,
	        Optional<String> platformParam,
            OwlReporter owlReporter
    ) {
	    C4AtomApiClient client = new C4AtomApiClient(c4HttpsClient(), ATOZ_BASE, platformParam);
	    C4BrandExtractor extractor = new C4BrandExtractor(
	            client,
	            platform,
	            Publisher.C4_PMLSD,
	            channelResolver,
	            new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4AtomFeedUriExtractor()),
	            c4PCLocationPolicyIds(),
	            true
        );
		return new C4AtomBackedBrandUpdater(
		        client,
		        platform,
		        contentResolver,
		        pmlsdLastUpdatedSettingContentWriter(owlReporter),
		        extractor
        );
	}
	
	private C4AtomBackedBrandUpdater xboxPmlsdBrandFetcher(Optional<Platform> platform, Optional<String> platformParam, OwlReporter owlReporter) {
        C4AtomApiClient client = new C4AtomApiClient(c4HttpsClient(), ATOZ_BASE, platformParam);
        C4BrandExtractor extractor = new C4BrandExtractor(
                client,
                platform,
                Publisher.C4_PMLSD_P06,
                channelResolver,
                new SourceSpecificContentFactory<>(Publisher.C4_PMLSD_P06, new C4AtomFeedUriExtractor()),
                c4XBoxLocationPolicyIds(),
                false
        );
        return new C4AtomBackedBrandUpdater(
                client,
                platform,
                contentResolver,
                pmlsdLastUpdatedSettingContentWriter(owlReporter),
                extractor
        );
    }
	
	private C4BrandUpdateController c4BrandUpdateController(OwlReporter owlReporter) {
	    return new C4BrandUpdateController(
	            pcPmlsdBrandFetcher(Optional.empty(), Optional.empty(), owlReporter),
	            ImmutableMap.of(
	                    Platform.XBOX,
	                    xboxPmlsdBrandFetcher(Optional.of(Platform.XBOX), Optional.of(P06_PLATFORM), owlReporter)
                )
        );
	}
	
	private C4EpgChannelDayUpdateController c4EpgChannelDayUpdateController(OwlReporter owlReporter) {
	    return new C4EpgChannelDayUpdateController(atomPmlsdApi(), pcC4PlmsdEpgChannelDayUpdater(owlReporter));
	}
	
    private LastUpdatedSettingContentWriter pmlsdLastUpdatedSettingContentWriter(OwlReporter owlReporter) {
        return new LastUpdatedSettingContentWriter(contentResolver, new LastUpdatedCheckingContentWriter(contentWriter), owlReporter);
    }
    
    @Bean protected C4LocationPolicyIds c4PCLocationPolicyIds() {
        return C4LocationPolicyIds.builder()
                    .withPlayerId(fourODPlayerId)
                    .withWebServiceId(webServiceId)
                    .withIosServiceId(iOsServiceId)
                    .build();
    }
    
    @Bean protected C4LocationPolicyIds c4XBoxLocationPolicyIds() {
        return C4LocationPolicyIds.builder()
                    .build();
    }

    @Bean protected C4PirateForceIngestController c4PirateForceIngestController() {
        return new C4PirateForceIngestController(
                contentWriter,
                C4PirateItemTransformer.create(),
                C4PirateClient.create(c4PirateUser, c4PiratePass, c4PirateUrl)
        );
    }

    private OwlReporter getOwlReporter() {
        return new OwlReporter(
                OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                        OwlTelescopeReporters.CHANNEL_4_INGEST_UPDATER, Event.Type.INGEST));
    }

    private OwlReporter getAtoZOwlReporter() {
        return new OwlReporter(
                OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                        OwlTelescopeReporters.CHANNEL_4_INGEST_ATOZ, Event.Type.INGEST));
    }
    
}
