package org.atlasapi.logging;

import java.util.Collection;

import javax.annotation.PostConstruct;

import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.probes.MongoProbe;
import com.metabroadcast.common.health.probes.Probe;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.atlasapi.remotesite.health.RemoteSiteHealthModule;
import org.atlasapi.system.health.ApiHealthController;
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

    private static final boolean IS_PROCESSING = Boolean.parseBoolean(
            System.getProperty("processing.config")
    );

    private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
			new MemoryInfoProbe(),
			new DiskSpaceProbe(),
			new MongoConnectionPoolProbe()
	);

	@Autowired private Collection<HealthProbe> probes;
	@Autowired private Mongo mongo;
	@Autowired private HealthController healthController;

	@Autowired private ApiHealthController apiHealthController;
	@Autowired private RemoteSiteHealthModule remoteSiteHealthModule;

    @Bean
	public HealthController healthController() {
		return new HealthController(systemProbes);
	}

    @Bean
	public org.atlasapi.system.HealthController threadController() {
		return new org.atlasapi.system.HealthController();
	}

    @Bean
    public ApiHealthController apiHealthController() {
        return ApiHealthController.create(Health.create(getProbes()));
    }


	@PostConstruct
	public void addProbes() {
		healthController.addProbes(probes);

	}

	private Iterable<Probe> getProbes() {
        return IS_PROCESSING ? getRemoteSiteProbes() : getApiProbes();
    }

    private Iterable<Probe> getApiProbes() {
        return ImmutableList.of(
                MongoProbe.create("mongo", (MongoClient) mongo)
        );
    }

    private Iterable<Probe> getRemoteSiteProbes() {
		return ImmutableList.<Probe>builder()
                .addAll(getApiProbes())
				.addAll(remoteSiteHealthModule.scheduleLivenessProbes())
				.add(remoteSiteHealthModule.bbcContentProbe())
	            .add(remoteSiteHealthModule.bbcScheduleHealthProbe())
				.build();
    }

}
