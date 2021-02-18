package org.atlasapi.remotesite.barb.channels;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;

import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.ImageModelTransformer;
import org.atlasapi.media.channel.ChannelWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BarbChannelsModule {

    private static final Logger log = LoggerFactory.getLogger(BarbChannelsModule.class);

    private static final RepetitionRule MAS_INGEST_REPETITION = RepetitionRules.NEVER;

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules().registerModule(new Jdk8Module());
    private final ChannelModelTransformer modelTransformer;

    @Autowired
    private ChannelWriter channelWriter;
//    @Autowired
//    SimpleScheduler scheduler;

//    @Value("${barb.ftp.username}")
//    private String ftpUsername;
//    @Value("${barb.ftp.password}")
//    private String ftpPassword;
//    @Value("${barb.ftp.host}")
//    private String ftpHost;

    public BarbChannelsModule() {
        modelTransformer = ChannelModelTransformer.create(
                SubstitutionTableNumberCodec.lowerCaseOnly(),
                ImageModelTransformer.create()
        );
    }

    @Bean
    protected BarbChannelIngestController barbChannelForceIngestController() {
        return new BarbChannelIngestController(channelWriter, modelTransformer, mapper);
    }

//    @PostConstruct
//    public void scheduleTask() {
//        ScheduledTask barbTask = BarbChannelDataIngester
//                .create(buildFtpClient())
//                .withName("Barb .MAS Channel Updater");
//
//        scheduler.schedule(barbTask, MAS_INGEST_REPETITION);
//    }
//
//    private FTPClient buildFtpClient() {
//        try {
//            FTPClient ftpClient = new FTPClient();
//            ftpClient.user(ftpUsername);
//            ftpClient.pass(ftpPassword);
//            ftpClient.connect(ftpHost);
//
//            return ftpClient;
//        } catch (IOException e) {
//            log.error("Failed to connect to ftp host", e);
//            throw Throwables.propagate(e);
//        }
//    }

}
