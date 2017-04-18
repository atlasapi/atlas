package org.atlasapi.system.health.probes;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

public class BroadcasterContentProbe extends Probe {

    public static final Logger log = LoggerFactory.getLogger(BroadcasterContentProbe.class);


    private final Clock clock = new SystemClock();
    private final Duration maxStaleness = Duration.standardHours(30);

    private final Publisher publisher;
    private final ContentResolver contentResolver;
    private final Iterable<String> uris;

    private BroadcasterContentProbe(
            String identifier,
            Publisher publisher,
            Iterable<String> uris,
            ContentResolver contentResolver
    ) {
        super(identifier);
        this.publisher = checkNotNull(publisher);
        this.contentResolver = checkNotNull(contentResolver);
        this.uris = ImmutableList.copyOf(checkNotNull(uris));

    }

    public static BroadcasterContentProbe create(
            String identifier,
            Publisher publisher,
            Iterable<String> uris,
            ContentResolver contentResolver
    ) {
        return new BroadcasterContentProbe(identifier, publisher, uris, contentResolver);
    }

    @Override
    public Callable<ProbeResult> createRequest() {
        return () -> {
            StringBuilder errors = new StringBuilder();

            contentResolver.findByCanonicalUris(uris).asMap()
                    .entrySet()
                    .forEach(entry -> {
                        Optional<Identified> content = entry.getValue().toOptional();
                        if (content.isPresent() && content.get() instanceof Described) {
                            Described playlist = (Described) content.get();
                            if (!playlist.getLastFetched().isAfter(clock.now().minus(maxStaleness))) {
                                errors.append(buildErrorString(playlist, "stale"));
                            }
                        } else {
                            errors.append(buildErrorString(entry.getKey(), "not found"));
                        }
                });

            return errors.length() == 0 ? ProbeResult.healthy(identifier)
                                        : ProbeResult.unhealthy(identifier, errors.toString());
        };
    }

    public String title() {
        return publisher.title();
    }

    private String buildErrorString(Described playlist, String message) {
        return buildErrorString(
                Objects.firstNonNull(playlist.getTitle(), playlist.getCanonicalUri()),
                message
        );
    }

    private String buildErrorString(String item, String message) {
        return String.format("{ %s: %s} ", item, message);
    }

}
