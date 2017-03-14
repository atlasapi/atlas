package org.atlasapi.logging;

import java.util.Collection;

import javax.annotation.PostConstruct;

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
	
	private final ImmutableList<HealthProbe> systemProbes = ImmutableList.of(
			new MemoryInfoProbe(),
			new DiskSpaceProbe(),
			new MongoConnectionPoolProbe()
	);
	
	private @Autowired Collection<HealthProbe> probes;
	private @Autowired HealthController healthController;

	public @Bean HealthController healthController() {
		return new HealthController(systemProbes);
	}
	
	public @Bean org.atlasapi.system.HealthController threadController() {
		return new org.atlasapi.system.HealthController();
	}
	
	@PostConstruct
	public void addProbes() {
		healthController.addProbes(probes);
	}
}
