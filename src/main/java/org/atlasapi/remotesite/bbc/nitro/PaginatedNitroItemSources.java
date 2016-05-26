package org.atlasapi.remotesite.bbc.nitro;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil;

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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaginatedNitroItemSources implements Iterable<List<Item>> {

    private Iterable<List<Episode>> episodes;
    private ListeningExecutorService executor;
    private final Glycerin glycerin;
    private final int pageSize;
    private final NitroEpisodeExtractor itemExtractor;
    private final GlycerinNitroClipsAdapter clipsAdapter;

    public PaginatedNitroItemSources(Iterable<List<Episode>> episodes, ListeningExecutorService executor,
            Glycerin glycerin, int pageSize, NitroEpisodeExtractor itemExtractor,
            GlycerinNitroClipsAdapter clipsAdapter) {
        this.episodes = checkNotNull(episodes);
        this.executor = checkNotNull(executor);
        this.glycerin = checkNotNull(glycerin);
        this.pageSize = checkNotNull(pageSize);
        this.itemExtractor = checkNotNull(itemExtractor);
        this.clipsAdapter = checkNotNull(clipsAdapter);
    }

    @Override
    public Iterator iterator() {
        return new NitroItemSourceIterator(episodes, executor, glycerin, pageSize, itemExtractor,
                clipsAdapter);
    }

    private static class NitroItemSourceIterator implements Iterator<List<Item>> {

        private Iterator<List<Episode>> episodesIterator;
        private final ListeningExecutorService executor;
        private final Glycerin glycerin;
        private Iterator<Episode> currentEpisodesIterator;
        private int pageSize;
        private final NitroEpisodeExtractor itemExtractor;
        private GlycerinNitroClipsAdapter clipsAdapter;

        public NitroItemSourceIterator(Iterable<List<Episode>> episodesIterator,
                ListeningExecutorService executor, Glycerin glycerin,
                int pageSize, NitroEpisodeExtractor itemExtractor,
                GlycerinNitroClipsAdapter clipsAdapter) {
            this.episodesIterator = episodesIterator.iterator();
            this.executor = executor;
            this.glycerin = glycerin;
            this.pageSize = pageSize;
            this.itemExtractor = itemExtractor;
            this.clipsAdapter = clipsAdapter;
        }

        @Override
        public boolean hasNext() {
            return episodesIterator.hasNext();
        }

        @Override
        public List<Item> next() {
            List<Episode> episodes = episodesIterator.next();
            ImmutableList.Builder<Item> items = ImmutableList.builder();

            ImmutableListMultimap<String, Availability> availabilities = getAvailabilities(episodes);
            ImmutableListMultimap<String, Broadcast> broadcasts = getBroadcasts(episodes);
            ImmutableListMultimap<String, Version> versions = getVersions(episodes);

            for (Episode glycerinEpisode : episodes) {
                NitroItemSource<Episode> nitroItemSource = NitroItemSource.valueOf(
                        glycerinEpisode,
                        availabilities.get(glycerinEpisode.getPid()),
                        broadcasts.get(glycerinEpisode.getPid()),
                        versions.get(glycerinEpisode.getPid())
                );

                Item item = itemExtractor.extract(nitroItemSource);
                List<Clip> clips = getClips(nitroItemSource);
                item.setClips(clips);
                items.add(item);
            }

            return items.build();
        }

        private List<Clip> getClips(NitroItemSource<Episode> nitroItemSource) {
            return getClips(
                    nitroItemSource.getProgramme().getPid(),
                    nitroItemSource.getProgramme().getUri()
            );
        }

        private ImmutableListMultimap<String, Version> getVersions(List<Episode> episodes) {
            ImmutableListMultimap<String, Version> versions;
            try {
                return versions(episodes);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
        }

        private ImmutableListMultimap<String, Broadcast> getBroadcasts(List<Episode> episodes) {
            ImmutableListMultimap<String, Broadcast> broadcasts;
            try {
                return broadcasts(episodes);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
        }

        private ImmutableListMultimap<String, Availability> getAvailabilities(List<Episode> episodes) {
            ImmutableListMultimap<String, Availability> availabilities;
            try {
                return availabilities(episodes);
            } catch (GlycerinException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private ImmutableListMultimap<String, Availability> availabilities(List<Episode> episodes)
                throws GlycerinException {

            List<ListenableFuture<ImmutableList<Availability>>> futures = Lists.newArrayList();
            AvailabilityQuery query = AvailabilityQuery.builder()
                    .withDescendantsOf(toPids(episodes))
                    .withPageSize(pageSize)
                    .withMediaSet("apple-iphone4-ipad-hls-3g",
                            "apple-iphone4-hls",
                            "pc",
                            "iptv-all",
                            "captions")
                    .build();

            futures.add(executor.submit(exhaustingAvailabilityCallable(query)));


            ListenableFuture<List<ImmutableList<Availability>>> all = Futures.allAsList(futures);
            Iterable<Availability> list;
            try {
                list = Iterables.concat(all.get());
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof GlycerinException) {
                    throw (GlycerinException) e.getCause();
                }
                throw Throwables.propagate(e);
            }

            return Multimaps.index(list, new Function<Availability, String>() {
                @Override
                public String apply(Availability input) {
                    return NitroUtil.programmePid(input);
                }
            });
        }

        private ImmutableListMultimap<String, Broadcast> broadcasts(List<Episode> episodes) throws GlycerinException {
            List<ListenableFuture<ImmutableList<Broadcast>>> futures = Lists.newArrayList();

            BroadcastsQuery query = BroadcastsQuery.builder()
                    .withDescendantsOf(toPids(episodes))
                    .withPageSize(pageSize)
                    .build();

            futures.add(executor.submit(exhaustingBroadcastsCallable(query)));
            ListenableFuture<List<ImmutableList<Broadcast>>> all = Futures.allAsList(futures);
            try {
                return Multimaps.index(Iterables.concat(all.get()),
                        new Function<Broadcast, String>() {

                            @Override
                            public String apply(Broadcast input) {
                                return NitroUtil.programmePid(input).getPid();
                            }
                        });
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof GlycerinException) {
                    throw (GlycerinException) e.getCause();
                }
                throw Throwables.propagate(e);
            }
        }

        private ImmutableListMultimap<String, Version> versions(List<Episode> episode)
                throws GlycerinException {
            List<ListenableFuture<ImmutableList<Version>>> futures = Lists.newArrayList();

            VersionsQuery query = VersionsQuery.builder()
                    .withDescendantsOf(toPids(episode))
                    .withPageSize(pageSize)
                    .build();

            futures.add(executor.submit(exhaustingVersionsCallable(query)));
            ListenableFuture<List<ImmutableList<Version>>> all = Futures.allAsList(futures);

            try {
                return Multimaps.index(Iterables.concat(all.get()), new Function<Version, String>() {

                    @Override
                    public String apply(Version input) {
                        return NitroUtil.programmePid(input).getPid();
                    }
                });
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof GlycerinException) {
                    throw (GlycerinException) e.getCause();
                }
                throw Throwables.propagate(e);
            }
        }

        private Iterable<String> toPids(List<Episode> episodes) {
            return Iterables.transform(episodes, new Function<Episode, String>() {

                @Override
                public String apply(Episode input) {
                    return input.getPid();
                }
            });
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