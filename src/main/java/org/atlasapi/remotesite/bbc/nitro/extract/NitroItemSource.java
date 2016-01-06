package org.atlasapi.remotesite.bbc.nitro.extract;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions;


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
     * @param availableVersions - the availabilities.
     * @return a {@code NitroItemSource} for the programme and availabilities.
     */
    public static <T> NitroItemSource<T> valueOf(T programme,
            AvailableVersions availableVersions,
            String programmeId) {
        return new NitroItemSource<T>(programme,
                availableVersions,
            ImmutableList.<Broadcast>of(),
            programmeId
        );
    }

    /**
     * Create a source for the given programme, availabilities and broadcasts.
     *
     * @param programme      - the programme.
     * @param availableVersions - the availabilities.
     * @param broadcasts     - the broadcasts.
     * @param programmeId       - the programmeId.
     * @return a {@code NitroItemSource} for the programme, availabilities and broadcasts.
     */
    public static <T> NitroItemSource<T> valueOf(T programme,
            AvailableVersions availableVersions,
            List<Broadcast> broadcasts,
            String programmeId) {
        return new NitroItemSource<T>(programme, 
            availableVersions,
            broadcasts,
            programmeId
        );
    }

    private final T programme;
    private final AvailableVersions availableVersions;
    private final ImmutableList<Broadcast> broadcasts;
    private final String programmeId;

    private NitroItemSource(T programme,
            AvailableVersions availableVersions,
            List<Broadcast> broadcasts,
            String programmeId) {
        this.programme = checkNotNull(programme);
        this.availableVersions = checkNotNull(availableVersions);
        this.broadcasts = ImmutableList.copyOf(broadcasts);
        this.programmeId = checkNotNull(programmeId);
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
     * @return - the availableVersions
     */
    public AvailableVersions getAvailableVersions() {
        return availableVersions;
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
     * Get the programme id of this source.
     *
     * @return - the programmeId
     */
    public String getProgrammeId() {
        return programmeId;
    }

}
