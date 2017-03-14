package org.atlasapi.logging;

import java.util.Collection;

import javax.annotation.PostConstruct;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.probes.HttpProbe;
import com.metabroadcast.common.health.probes.JsonHttpProbe;
import com.metabroadcast.common.health.probes.MetricsProbe;
import com.metabroadcast.common.health.probes.MongoProbe;
import com.metabroadcast.common.health.probes.Probe;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.persistence.MongoContentPersistenceModule;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.system.Health.K8HealthController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.DiskSpaceProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.persistence.mongo.health.MongoConnectionPoolProbe;
import com.metabroadcast.common.webapp.health.HealthController;

@Configuration
public class HealthModule {

    private static final String HEALTH_URI = "http://atlas.metabroadcast.com/3.0/channels.json";
    private static final String JSON_URI = "http://atlas.metabroadcast.com/3.0/channels/999.json";
    private static final HttpClient HTTP_CLIENT = HttpClientBuilder.create().build();
    private static final ObjectMapper mapper = JsonFactory.makeJsonMapper();

    private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
			new MemoryInfoProbe(),
			new DiskSpaceProbe(),
			new MongoConnectionPoolProbe()
	);
	
	private @Autowired Collection<HealthProbe> probes;
	private @Autowired Mongo mongo;
	private @Autowired HealthController healthController;
	private @Autowired K8HealthController k8HealthController;

	public @Bean HealthController healthController() {
		return new HealthController(systemProbes);
	}
	
	public @Bean org.atlasapi.system.HealthController threadController() {
		return new org.atlasapi.system.HealthController();
	}

    @Bean
    public K8HealthController k8HealthController() {
        return K8HealthController.create(mapper);
    }


	@PostConstruct
	public void addProbes() {
		healthController.addProbes(probes);

        k8HealthController.registerHealth("api", Health.create(getApiProbes()));
	}

    private Iterable<Probe> getApiProbes() {
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
