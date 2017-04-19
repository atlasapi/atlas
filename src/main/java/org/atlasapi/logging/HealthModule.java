package org.atlasapi.logging;

import java.util.Collection;
import java.util.List;

import javax.annotation.PostConstruct;

import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.probes.HttpProbe;
import com.metabroadcast.common.health.probes.MongoProbe;
import com.metabroadcast.common.health.probes.Probe;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.atlasapi.remotesite.health.RemoteSiteHealthModule;
import org.atlasapi.system.health.K8HealthController;
import org.atlasapi.system.health.probes.BroadcasterContentProbe;
import org.atlasapi.system.health.probes.ScheduleProbe;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.DiskSpaceProbe;
import com.metabroadcast.common.health.probes.MemoryInfoProbe;
import com.metabroadcast.common.persistence.mongo.health.MongoConnectionPoolProbe;
import com.metabroadcast.common.webapp.health.HealthController;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ RemoteSiteHealthModule.class })
public class HealthModule {

    private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
			new MemoryInfoProbe(),
			new DiskSpaceProbe(),
			new MongoConnectionPoolProbe()
	);

	@Autowired private Collection<HealthProbe> probes;
	@Autowired private Mongo mongo;
	@Autowired private HealthController healthController;

	@Autowired private K8HealthController k8HealthController;
	@Autowired private List<HttpProbe> httpProbes;
	@Autowired private BroadcasterContentProbe broadcasterContentProbe;
	@Autowired private ScheduleProbe scheduleProbe;

    @Bean
	public HealthController healthController() {
		return new HealthController(systemProbes);
	}

    @Bean
	public org.atlasapi.system.HealthController threadController() {
		return new org.atlasapi.system.HealthController();
	}

    @Bean
    public K8HealthController k8HealthController() {
        return K8HealthController.create();
    }


	@PostConstruct
	public void addProbes() {
		healthController.addProbes(probes);

		k8HealthController.registerHealth("owl-api", Health.create(getApiProbes()));
		k8HealthController.registerHealth("remote-site", Health.create(getRemoteSiteProbes()));
	}

    private Iterable<Probe> getApiProbes() {
        return ImmutableList.of(
                MongoProbe.create("mongo", (MongoClient) mongo)
        );
    }

    private Iterable<Probe> getRemoteSiteProbes() {
		return ImmutableList.<Probe>builder()
				.addAll(httpProbes)
				.add(broadcasterContentProbe)
	            .add(scheduleProbe)
				.build();
    }

}
