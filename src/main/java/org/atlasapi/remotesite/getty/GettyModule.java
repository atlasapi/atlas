package org.atlasapi.remotesite.getty;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;
import org.atlasapi.remotesite.metabroadcast.MongoSchedulingStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

import static org.atlasapi.persistence.MongoModule.OWL_DATABASED_MONGO;

@Configuration
public class GettyModule {

    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentLister contentLister;
    private @Autowired TopicGuesser topicGuesser;
    private @Autowired @Qualifier(OWL_DATABASED_MONGO) DatabasedMongo mongo;

    @Value("${getty.client.id}") private String clientId;
    @Value("${getty.client.secret}") private String clientSecret;
    @Value("${getty.client.user}") private String clientUsername;
    @Value("${getty.client.password}") private String clientPassword;

    @Value("${getty.pagination}") private String gettyPagination;
    @Value("${getty.idlistfile}") private String idsFileName;

    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(gettyUpdater().withName("Getty Updater"), RepetitionRules.NEVER);
    }

    private GettyUpdateTask gettyUpdater() {
        return new GettyUpdateTask(gettyClient(), new GettyAdapter(), contentLister,
                new DefaultGettyDataHandler(contentResolver, contentWriter, new GettyContentExtractor(topicGuesser)),
                idsFileName,
                Integer.valueOf(gettyPagination), new RestartStatusSupplier.StoreProvidedStatus(new MongoSchedulingStore(mongo)));
    }

    private GettyClient gettyClient() {
        return new GettyClient(
                new GettyTokenFetcher(clientId, clientSecret, clientUsername, clientPassword)
        );
    }

}
