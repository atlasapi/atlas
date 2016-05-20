package org.atlasapi.remotesite.bbc.nitro;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.BroadcastsQuery;
import com.metabroadcast.atlas.glycerin.queries.VersionsQuery;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class PaginatedNitroItemSources implements Iterable<Item> {

    private Iterable<List<Episode>> episodes;
    private ListeningExecutorService executor;
    private final Glycerin glycerin;
    private final int pageSize;
    private final NitroEpisodeExtractor itemExtractor;
    private final GlycerinNitroClipsAdapter clipsAdapter;

    public PaginatedNitroItemSources(Iterable<List<Episode>> episodes, ListeningExecutorService executor,
            Glycerin glycerin, int pageSize, NitroEpisodeExtractor itemExtractor,
            GlycerinNitroClipsAdapter clipsAdapter) {
        this.episodes = episodes;
        this.executor = executor;
        this.glycerin = glycerin;
        this.pageSize = pageSize;
        this.itemExtractor = itemExtractor;
        this.clipsAdapter = clipsAdapter;
    }

    @Override
    public Iterator iterator() {
        return new NitroItemSourceIterator(episodes, executor, glycerin, pageSize, itemExtractor,
                clipsAdapter);
    }

    private static class NitroItemSourceIterator implements Iterator<Item> {

        private Iterator<List<Episode>> episodes;
        private final ListeningExecutorService executor;
        private final Glycerin glycerin;
        private Iterator<Episode> currentEpisodes;
        private Episode episode;
        private int pageSize;
        private final NitroEpisodeExtractor itemExtractor;
        private GlycerinNitroClipsAdapter clipsAdapter;


        public NitroItemSourceIterator(Iterable<List<Episode>> episodes,
                ListeningExecutorService executor, Glycerin glycerin,
                int pageSize, NitroEpisodeExtractor itemExtractor,
                GlycerinNitroClipsAdapter clipsAdapter) {
            this.episodes = episodes.iterator();
            this.executor = executor;
            this.glycerin = glycerin;
            this.pageSize = pageSize;
            this.itemExtractor = itemExtractor;
            this.clipsAdapter = clipsAdapter;
        }

        @Override
        public boolean hasNext() {
            if (currentEpisodes == null && episodes.hasNext()) { // Get first list of episodes
                currentEpisodes = episodes.next().iterator();
                if (currentEpisodes.hasNext()) {
                    return true;
                } else {
                    return false;
                }
            } else if (currentEpisodes.hasNext()) { // Check for next episode
                return true;
            } else if (episodes.hasNext()) { // Get next list of episodes
                currentEpisodes = episodes.next().iterator();
                if (currentEpisodes.hasNext()) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        @Override
        public Item next() {
            Episode glycerinEpisode = currentEpisodes.next();

            ListenableFuture<ImmutableList<Availability>> availabilities = getAvailabilities(glycerinEpisode);
            ListenableFuture<ImmutableList<Broadcast>> broadcasts = getBroadcasts(glycerinEpisode);
            ListenableFuture<ImmutableList<Version>> versions = getVersions(glycerinEpisode);

            try {
                NitroItemSource<Episode> nitroItemSource = NitroItemSource.valueOf(
                        glycerinEpisode,
                        availabilities.get(),
                        broadcasts.get(),
                        versions.get()
                );

                Item item = itemExtractor.extract(nitroItemSource);
                List<Clip> clips = getClips(nitroItemSource);
                item.setClips(clips);
                return item;
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

        private List<Clip> getClips(NitroItemSource<Episode> nitroItemSource) {
            return getClips(
                                nitroItemSource.getProgramme().getPid(),
                                nitroItemSource.getProgramme().getUri()
                        );
        }

        private ListenableFuture<ImmutableList<Version>> getVersions(Episode episode) {
            ListenableFuture<ImmutableList<Version>> versions;
            try {
                versions = versions(episode);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
            return versions;
        }

        private ListenableFuture<ImmutableList<Broadcast>> getBroadcasts(Episode episode) {
            ListenableFuture<ImmutableList<Broadcast>> broadcasts;
            try {
                broadcasts = broadcasts(episode);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
            return broadcasts;
        }

        private ListenableFuture<ImmutableList<Availability>> getAvailabilities(Episode episode) {
            ListenableFuture<ImmutableList<Availability>> availabilities;
            try {
                availabilities = availabilities(episode);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
            return availabilities;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private ListenableFuture<ImmutableList<Availability>> availabilities(Episode episode)
                throws GlycerinException {

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

        /**
         * Returns a list of clips for the given episode, that later is used
         * to set the Atlas item clips.
         * @param pid - the episode programme PID.
         * @param uri - the episode prgramme URI.
         * @return List of Atlas clips.
         */
        public List<Clip> getClips(String pid, String uri) {
            PidReference pidReference = new PidReference();
            pidReference.setPid(pid);
            pidReference.setHref(uri);

            try {
                return clipsAdapter.clipsFor(pidReference);
            } catch (NitroException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
