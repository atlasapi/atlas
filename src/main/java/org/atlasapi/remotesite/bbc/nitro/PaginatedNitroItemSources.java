package org.atlasapi.remotesite.bbc.nitro;

import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.*;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.BroadcastsQuery;
import com.metabroadcast.atlas.glycerin.queries.VersionsQuery;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class PaginatedNitroItemSources implements Iterable<NitroItemSource<Episode>> {

    private Iterable<Episode> episodes;
    private int nitroBatchSize;
    private int pageSize;
    private ListeningExecutorService executor;
    private final Glycerin glycerin;

    public PaginatedNitroItemSources(Iterable<Episode> episodes, int nitroBatchSize, int pageSize,
                                     ListeningExecutorService executor, Glycerin glycerin) {
        this.episodes = episodes;
        this.nitroBatchSize = nitroBatchSize;
        this.pageSize = pageSize;
        this.executor = executor;
        this.glycerin = glycerin;
    }

    @Override
    public Iterator iterator() {
        return new NitroItemSourceIterator(episodes, nitroBatchSize, pageSize, executor, glycerin);
    }

    private static class NitroItemSourceIterator implements Iterator<NitroItemSource<Episode>> {

        private Iterator<Episode> episodes;
        private Episode episode;
        private final int pageSize;
        private final ListeningExecutorService executor;
        private final Glycerin glycerin;

        public NitroItemSourceIterator(Iterable<Episode> episodes, int nitroBatchSize, int pageSize,
                                       ListeningExecutorService executor, Glycerin glycerin) {
            this.episodes = episodes.iterator();
            this.pageSize = pageSize;
            this.executor = executor;
            this.glycerin = glycerin;
        }

        @Override
        public boolean hasNext() {
            if (episodes.hasNext()) {
                return true;
            }
            return false;
        }

        @Override
        public NitroItemSource<Episode> next() {
            episode = episodes.next();

            ListenableFuture<ImmutableList<Availability>> availabilities;
            try {
                availabilities = availabilities(episode);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }

            ListenableFuture<ImmutableList<Broadcast>> broadcasts;
            try {
                broadcasts = broadcasts(episode);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }

            ListenableFuture<ImmutableList<Version>> versions;
            try {
                versions = versions(episode);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }

            try {
                return NitroItemSource.valueOf(
                        episode,
                        availabilities.get(),
                        broadcasts.get(),
                        versions.get()
                );
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof GlycerinException) {
                    try {
                        throw (GlycerinException) e.getCause();
                    } catch (GlycerinException e1) {
                        Throwables.propagate(e1);
                    }
                }
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private ListenableFuture<ImmutableList<Availability>> availabilities(Episode episode) throws GlycerinException {
            AvailabilityQuery query = AvailabilityQuery.builder()
                    .withDescendantsOf(episode.getPid())
                    .withPageSize(pageSize)
                    .withMediaSet("apple-iphone4-ipad-hls-3g",
                            "apple-iphone4-hls",
                            "pc",
                            "iptv-all",
                            "captions")
                    .build();

            return executor.submit(exhaustingAvailabilityCallable(query));
        }

        private ListenableFuture<ImmutableList<Broadcast>> broadcasts(Episode episode) throws GlycerinException {
            BroadcastsQuery query = BroadcastsQuery.builder()
                    .withDescendantsOf(episode.getPid())
                    .withPageSize(pageSize)
                    .build();

            return executor.submit(exhaustingBroadcastsCallable(query));
        }

        private ListenableFuture<ImmutableList<Version>> versions(Episode episode) throws GlycerinException {
            VersionsQuery query = VersionsQuery.builder()
                    .withDescendantsOf(episode.getPid())
                    .withPageSize(pageSize)
                    .build();

          return executor.submit(exhaustingVersionsCallable(query));
        }

        private Callable<ImmutableList<Version>> exhaustingVersionsCallable(final VersionsQuery query) {

            return new Callable<ImmutableList<Version>>() {

                @Override
                public ImmutableList<Version> call() throws Exception {
                    return exhaust(glycerin.execute(query));
                }
            };
        }

        private Callable<ImmutableList<Broadcast>> exhaustingBroadcastsCallable(
                final BroadcastsQuery query) {

            return new Callable<ImmutableList<Broadcast>>() {

                @Override
                public ImmutableList<Broadcast> call() throws Exception {
                    return exhaust(glycerin.execute(query));
                }
            };
        }

        private Callable<ImmutableList<Availability>> exhaustingAvailabilityCallable(
                final AvailabilityQuery query) {

            return new Callable<ImmutableList<Availability>>() {

                @Override
                public ImmutableList<Availability> call() throws Exception {
                    return exhaust(glycerin.execute(query));
                }
            };
        }

        private <T> ImmutableList<T> exhaust(GlycerinResponse<T> resp) throws GlycerinException {
            ImmutableList.Builder<T> programmes = ImmutableList.builder();
            ImmutableList<T> results = resp.getResults();
            programmes.addAll(results);
            while (resp.hasNext()) {
                resp = resp.getNext();
                programmes.addAll(resp.getResults());
            }
            return programmes.build();
        }
    }
}
