package org.atlasapi;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class ChannelGroupIngestTask implements Runnable {
    
    private final Gson gson = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();
    private final ChannelGroupWriter channelGroupWriter;
    private final ChannelResolver channelResolver;
    private final IdGenerator idGenerator;
    
    public ChannelGroupIngestTask(ChannelGroupWriter channelGroupWriter, ChannelResolver channelResolver, IdGenerator idGenerator) {
        this.channelGroupWriter = channelGroupWriter;
        this.channelResolver = channelResolver;
        this.idGenerator = idGenerator;
        
    }

    @Override
    public void run() {
        
        try {
            URL resource = Resources.getResource("channel-groups.json");
            IngestDocument document = gson.fromJson(Resources.toString(resource, Charsets.UTF_8), IngestDocument.class);
            
            for (Platform platform : document.platforms) {
                createPlatform(platform, Publisher.METABROADCAST);
            }
            
            for (Platform platform : document.bbcRegions) {
                createRegion(platform, Publisher.BBC);
            }
            
            for (Platform platform : document.itvRegions) {
                createRegion(platform, Publisher.ITV);
            }
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private void createPlatform(Platform platform, Publisher publisher) {
        ChannelGroup channelGroup = new org.atlasapi.media.channel.Platform();
        channelGroup.setId(idGenerator.generateRaw());
        channelGroup.addTitle(platform.name);
        channelGroup.setChannels(Iterables.transform(platform.channels, TO_CHANNEL_ID));
        channelGroup.setPublisher(publisher);
        channelGroup.setAvailableCountries(ImmutableSet.of(Countries.GB));
        
        channelGroupWriter.createOrUpdate(channelGroup);
    }
    
    private void createRegion(Platform platform, Publisher publisher) {
        ChannelGroup channelGroup = new Region();
        channelGroup.setId(idGenerator.generateRaw());
        channelGroup.addTitle(platform.name);
        channelGroup.setChannels(Iterables.transform(platform.channels, TO_CHANNEL_ID));
        channelGroup.setPublisher(publisher);
        channelGroup.setAvailableCountries(ImmutableSet.of(Countries.GB));
        
        channelGroupWriter.createOrUpdate(channelGroup);
    }
    
    private Function<String, Long> TO_CHANNEL_ID = new Function<String, Long>() {
        @Override
        public Long apply(String input) {
            return channelResolver.fromUri(Channel.fromFieldName(input).get().uri()).requireValue().getId();
        }
    };
    
    public static void main(String[] args) throws UnknownHostException, MongoException {
        DatabasedMongo mongo = new DatabasedMongo(new Mongo(Configurer.get("mongo.host").get()), Configurer.get("mongo.dbName").get());
        
        MongoChannelGroupStore store = new MongoChannelGroupStore(mongo);
        
        new ChannelGroupIngestTask(new MongoChannelGroupStore(mongo), new MongoChannelStore(mongo, store, store), new MongoSequentialIdGenerator(mongo, "channelGroup")).run();
    }
    
    private static class IngestDocument {
        private List<Platform> platforms;
        private List<Platform> bbcRegions;
        private List<Platform> itvRegions;
    }
    
    private static class Platform {
        String name;
        List<String> channels;
    }
}