package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;

import java.util.List;

/**
 * Adapter for fetching data from Nitro by {@link PidReference}.  
 */
public interface NitroContentAdapter {

    /**
     * Fetch and transform data for the given ref into a {@link Brand}.
     * 
     * @param ref
     *            - the brand PID references, must have result type "brand".
     * @return - a set of {@link Brand}s representing the fetched data.
     * @throws NitroException
     *             - if there was an error fetching data from Nitro.
     * @throws IllegalArgumentException
     *             - if any of the {@code refs} is not for a brand.
     */
    ImmutableSet<ModelWithPayload<Brand>> fetchBrands(Iterable<PidReference> ref) throws NitroException;

    /**
     * Fetch and transform data for the given ref into a {@link Series}.
     * 
     * @param refs
     *            - the series PID references, must have result type "series".
     * @return - a set of {@link Series} representing the fetched data.
     * @throws NitroException
     *             - if there was an error fetching data from Nitro.
     * @throws IllegalArgumentException
     *             - if any of the {@code refs} is not for a series.
     */
    ImmutableSet<ModelWithPayload<Series>> fetchSeries(Iterable<PidReference> refs) throws NitroException;

    /**
     * Fetch and transform data for the given ref into a {@link Item}.

     * @param refs
     *            - the PID references of the episode to be fetched, must have
     *            result type "episode".
     * @return - a set of {@link Item}s representing the fetched data.
     * @throws NitroException
     *             - if there was an error fetching data from Nitro.
     * @throws IllegalArgumentException
     *             - if any of the {@code refs} is not for an episode.
     */
    Iterable<List<ModelWithPayload<Item>>> fetchEpisodes(Iterable<PidReference> refs) throws NitroException;

    // TODO: MBST-15521
    Iterable<List<ModelWithPayload<Item>>> fetchEpisodes(
            Iterable<PidReference> refs,
            ImmutableListMultimap<String, Broadcast> broadcasts
    ) throws NitroException;

    /**
     * Fetch and transform data for the given ref into a {@link Item}.
     * <p>
     *
     * @param query - a query to execute to get the episodes
     * @return - a set of {@link Item}s representing the fetched data.
     * @throws NitroException           - if there was an error fetching data from Nitro.
     * @throws IllegalArgumentException - if any of the {@code refs} is not for an episode.
     */
    Iterable<List<ModelWithPayload<Item>>> fetchEpisodes(
            ProgrammesQuery query,
            ImmutableListMultimap<String, Broadcast> broadcasts
    ) throws NitroException;
}
