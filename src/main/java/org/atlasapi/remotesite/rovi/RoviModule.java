package org.atlasapi.remotesite.rovi;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.metabroadcast.MongoSchedulingStore;
import org.atlasapi.remotesite.rovi.indexing.MapBasedKeyedFileIndexer;
import org.atlasapi.remotesite.rovi.model.RoviEpisodeSequenceLine;
import org.atlasapi.remotesite.rovi.model.RoviProgramDescriptionLine;
import org.atlasapi.remotesite.rovi.model.RoviProgramLine;
import org.atlasapi.remotesite.rovi.model.RoviSeasonHistoryLine;
import org.atlasapi.remotesite.rovi.parsers.RoviEpisodeSequenceLineParser;
import org.atlasapi.remotesite.rovi.parsers.RoviProgramDescriptionLineParser;
import org.atlasapi.remotesite.rovi.parsers.RoviProgramLineParser;
import org.atlasapi.remotesite.rovi.parsers.RoviSeasonHistoryLineParser;
import org.atlasapi.remotesite.rovi.populators.ScheduleLineBroadcastExtractor;
import org.atlasapi.remotesite.rovi.processing.AuxiliaryCacheSupplier;
import org.atlasapi.remotesite.rovi.processing.ItemBroadcastUpdater;
import org.atlasapi.remotesite.rovi.processing.RoviDeltaIngestProcessor;
import org.atlasapi.remotesite.rovi.processing.RoviFullIngestProcessor;
import org.atlasapi.remotesite.rovi.processing.ScheduleFileProcessor;
import org.atlasapi.remotesite.rovi.processing.restartable.DefaultIngestStatusStore;
import org.atlasapi.remotesite.rovi.processing.restartable.IngestStatusStore;
import org.atlasapi.remotesite.rovi.tasks.RoviIngestTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

import static org.atlasapi.persistence.MongoModule.OWL_DATABASED_MONGO;

@Configuration
public class RoviModule {

    private static final boolean FULL_INGEST = true;
    private static final boolean DELTA_INGEST = false;

    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired @Qualifier(OWL_DATABASED_MONGO) DatabasedMongo mongo;
    
    @Bean
    public MapBasedKeyedFileIndexer<String, RoviProgramDescriptionLine> descriptionsIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviProgramDescriptionLineParser());
    }

    @Bean
    public MapBasedKeyedFileIndexer<String, RoviEpisodeSequenceLine> episodeSequenceIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviEpisodeSequenceLineParser());
    }

    @Bean
    public MapBasedKeyedFileIndexer<String, RoviSeasonHistoryLine> seasonHistoryIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviSeasonHistoryLineParser());
    }

    @Bean
    public MapBasedKeyedFileIndexer<String, RoviProgramLine> programIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviProgramLineParser());
    }

    @Bean
    public IngestStatusStore ingestStatusPersistor() {
        return new DefaultIngestStatusStore(new MongoSchedulingStore(mongo));
    }

    @Bean
    public RoviContentWriter roviContentWriter() {
        return new RoviContentWriter(contentWriter);
    }
    
    @Bean
    public RoviFullIngestProcessor fullIngestProcessor() {
        return new RoviFullIngestProcessor(
                descriptionsIndexer(),
                episodeSequenceIndexer(),
                roviContentWriter(),
                contentResolver,
                fullIngestScheduleProcessor(),
                auxCacheSupplier(),
                ingestStatusPersistor());
    }

    @Bean
    public RoviDeltaIngestProcessor deltaIngestProcessor() {
        return new RoviDeltaIngestProcessor(
                programIndexer(),
                descriptionsIndexer(),
                episodeSequenceIndexer(),
                roviContentWriter(),
                contentResolver,
                deltaIngestScheduleProcessor(),
                auxCacheSupplier());
    }
    
    @Bean
    public AuxiliaryCacheSupplier auxCacheSupplier() {
        return new AuxiliaryCacheSupplier(contentResolver);
    }
    
    @Bean
    public RoviIngestTask roviFullIngestTask() {
        return new RoviIngestTask(
                fullIngestProcessor(),
                FileSupplier.fullProgramFile(),
                FileSupplier.fullSeasonHistoryFile(),
                FileSupplier.fullScheduleFile(),
                FileSupplier.fullProgramDescriptionsFile(),
                FileSupplier.fullEpisodeSequenceFile(),
                "Rovi Full Ingest Task");
    }
    
    @Bean
    public RoviIngestTask roviDeltaIngestTask() {
        return new RoviIngestTask(
                deltaIngestProcessor(),
                FileSupplier.deltaProgramFile(),
                FileSupplier.deltaSeasonHistoryFile(),
                FileSupplier.deltaScheduleFile(),
                FileSupplier.deltaProgramDescriptionsFile(),
                FileSupplier.deltaEpisodeSequenceFile(),
                "Rovi Delta Ingest Task");      
    }
    
    @Bean
    public ScheduleFileProcessor fullIngestScheduleProcessor() {
        return scheduleProcessor(FULL_INGEST);
    }

    @Bean
    public ScheduleFileProcessor deltaIngestScheduleProcessor() {
        return scheduleProcessor(DELTA_INGEST);
    }

    private ScheduleFileProcessor scheduleProcessor(boolean fullIngest) {
        return new ScheduleFileProcessor(
                new ItemBroadcastUpdater(contentResolver, contentWriter),
                new ScheduleLineBroadcastExtractor(channelResolver),
                fullIngest);
    }
    
    @PostConstruct
    public void init() {
        scheduler.schedule(roviFullIngestTask(), RepetitionRules.NEVER);
        scheduler.schedule(roviDeltaIngestTask(), RepetitionRules.NEVER);
    }

}
