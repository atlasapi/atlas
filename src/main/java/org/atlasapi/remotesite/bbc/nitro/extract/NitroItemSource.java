package org.atlasapi.remotesite.bbc.nitro.extract;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Version;

/**
 * A source which contains all the data required for extracting an
 * {@link org.atlasapi.media.entity.Item Item}, including {@link Availability}s
 * and {@link Broadcast}s.
 *
 * @param <T> - the type of {@link com.metabroadcast.atlas.glycerin.model.Programme Programme}
 */
public class NitroItemSource<T> {

    private static final Function<Broadcast, String> BROADCAST_TO_VERSION_PID = new Function<Broadcast, String>() {

        @Override
        public String apply(Broadcast input) {
            return NitroUtil.versionPid(input).getPid();
        }
    };

    /**
     * Create a source for the given programme and availabilities.
     *
     * @param programme      - the programme.
     * @param availabilities - the availabilities.
     * @return a {@code NitroItemSource} for the programme and availabilities.
     */
    public static <T> NitroItemSource<T> valueOf(T programme, Iterable<Availability> availabilities) {
        return new NitroItemSource<T>(programme, 
            availabilities, 
            ImmutableList.<Broadcast>of(),
            ImmutableList.<Version>of()
        );
    }

    /**
     * Create a source for the given programme, availabilities and broadcasts.
     *
     * @param programme      - the programme.
     * @param availabilities - the availabilities.
     * @param broadcasts     - the broadcasts.
     * @param versions       - the versions.
     * @return a {@code NitroItemSource} for the programme, availabilities and broadcasts.
     */
    public static <T> NitroItemSource<T> valueOf(T programme, List<Availability> availabilities, List<Broadcast> broadcasts, List<Version> versions) {
        return new NitroItemSource<T>(programme, 
            availabilities, 
            broadcasts,
            versions
        );
    }

    private final T programme;
    private final ImmutableList<Availability> availabilities;
    private final ImmutableList<Broadcast> broadcasts;
    private final ImmutableList<Version> versions;

    private NitroItemSource(T programme, Iterable<Availability> availabilities,
            Iterable<Broadcast> broadcasts,
            Iterable<Version> versions) {
        this.programme = checkNotNull(programme);
        this.availabilities = ImmutableList.copyOf(availabilities);
        this.broadcasts = ImmutableList.copyOf(broadcasts);
        this.versions = ImmutableList.copyOf(versions);
    }

    /**
     * Get the programme related to this source.
     *
     * @return - the programme
     */
    public T getProgramme() {
        return programme;
    }

    /**
     * Get the availabilities related to this source.
     *
     * @return - the availabilities
     */
    public ImmutableList<Availability> getAvailabilities() {
        return availabilities;
    }

    /**
     * Get the broadcasts related to this source.
     *
     * @return - the broadcasts
     */
    public ImmutableList<Broadcast> getBroadcasts() {
        return broadcasts;
    }

    /**
     * Get the versions related to this source.
     *
     * @return - the versions
     */
    public ImmutableList<Version> getVersions() {
        return ImmutableList.copyOf(versions);
    }

}
