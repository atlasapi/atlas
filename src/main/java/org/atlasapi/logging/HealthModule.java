package org.atlasapi.logging;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.probes.*;
import com.metabroadcast.common.persistence.mongo.health.MongoConnectionPoolProbe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.webapp.health.HealthController;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.atlasapi.remotesite.health.RemoteSiteHealthModule;
import org.atlasapi.system.health.K8HealthController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;

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
    public K8HealthController apiHealthController() {
        return K8HealthController.create(Health.create(getProbes()));
    }


	@PostConstruct
	public void addProbes() {
		healthController.addProbes(probes);

	}

	private Iterable<Probe> getProbes() {
        return IS_PROCESSING ? getProcessingProbes() : getApiProbes();
    }

    private Iterable<Probe> getApiProbes() {
        return ImmutableList.of(
                MongoProbe.create("mongo", (MongoClient) mongo)
        );
    }

    private Iterable<Probe> getProcessingProbes() {
		return ImmutableList.<Probe>builder()
                .addAll(metricProbesFor(getApiProbes()))
				.add(metricProbeFor(remoteSiteHealthModule.scheduleLivenessProbe()))
				.build();
    }

    private List<MetricsProbe> metricProbesFor(Iterable<Probe> probes) {
        return StreamSupport.stream(probes.spliterator(), false)
                .map(this::metricProbeFor)
                .collect(MoreCollectors.toImmutableList());
    }

    private MetricsProbe metricProbeFor(Probe probe) {
        return MetricsProbe.builder()
                .withIdentifier(probe.getIdentifier() + "Metrics")
                .withDelegate(probe)
                .withMetricRegistry(new MetricRegistry())
                .withMetricPrefix("atlas-owl-" + (IS_PROCESSING ? "processing" : "api"))
                .build();
    }

}
