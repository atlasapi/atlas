package org.atlasapi.system.Health;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.probes.HttpProbe;
import com.metabroadcast.common.health.probes.JsonHttpProbe;
import com.metabroadcast.common.health.probes.MongoProbe;
import com.metabroadcast.common.health.probes.Probe;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.atlasapi.media.channel.Channel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HealthModule {

    private static final String HEALTH_URI = "http://atlas.metabroadcast.com/3.0/channels.json";
    private static final String JSON_URI = "http://atlas.metabroadcast.com/3.0/channels/999.json";
    private static final HttpClient HTTP_CLIENT = HttpClientBuilder.create().build();

    @Autowired private Mongo mongo;

    private HealthModule() {
    }

    public static HealthModule create() {
        return new HealthModule();
    }

    @Bean
    public Health health() {
        return Health.create(getProbes());
    }

    private Iterable<Probe> getProbes() {
        return ImmutableList.of(
                HttpProbe.create(HEALTH_URI, HTTP_CLIENT),
                JsonHttpProbe.builder(Channel.class)
                        .withHealthy(channel -> channel.getId().equals(999L))
                        .withUri(JSON_URI)
                        .withClient(HTTP_CLIENT)
                        .build(),
                MongoProbe.create((MongoClient) mongo)
        );
    }

}
