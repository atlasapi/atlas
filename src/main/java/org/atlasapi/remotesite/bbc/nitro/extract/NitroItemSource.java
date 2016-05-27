package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.List;

import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A source which contains all the data required for extracting an
 * {@link org.atlasapi.media.entity.Item Item}, including {@link Availability}s
 * and {@link Broadcast}s.
 *
 * @param <T> - the type of {@link com.metabroadcast.atlas.glycerin.model.Programme Programme}
 */
public class NitroItemSource<T> {

    /**
     * Create a source for the given programme and availabilities.
     *
     * @param programme      - the programme.
     * @return a {@code NitroItemSource} for the programme and availabilities.
     */
    public static <T> NitroItemSource<T> valueOf(T programme) {
        return new NitroItemSource<>(programme, ImmutableList.<Broadcast>of());
    }

    /**
     * Create a source for the given programme, availabilities and broadcasts.
     *
     * @param programme      - the programme.
     * @param broadcasts     - the broadcasts.
     * @return a {@code NitroItemSource} for the programme, availabilities and broadcasts.
     */
    public static <T> NitroItemSource<T> valueOf(
            T programme,
            List<Broadcast> broadcasts
    ) {
        return new NitroItemSource<>(programme, broadcasts);
    }

    private final T programme;
    private final ImmutableList<Broadcast> broadcasts;

    private NitroItemSource(
            T programme,
            Iterable<Broadcast> broadcasts
    ) {
        this.programme = checkNotNull(programme);
        this.broadcasts = ImmutableList.copyOf(broadcasts);
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
     * Get the broadcasts related to this source.
     *
     * @return - the broadcasts
     */
    public ImmutableList<Broadcast> getBroadcasts() {
        return broadcasts;
    }
}
