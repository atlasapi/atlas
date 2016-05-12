package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroItemSource;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class LazyNitroEpisodeExtractor implements Iterable<Item> {

    private Iterable<NitroItemSource<Episode>> episodes;
    private final NitroEpisodeExtractor itemExtractor;
    private final GlycerinNitroClipsAdapter clipsAdapter;

    public LazyNitroEpisodeExtractor(Iterable<NitroItemSource<Episode>> episodes, NitroEpisodeExtractor itemExtractor,
                             GlycerinNitroClipsAdapter clipsAdapter) {
        this.episodes = episodes;
        this.itemExtractor = itemExtractor;
        this.clipsAdapter = clipsAdapter;
    }

    @Override
    public Iterator<Item> iterator() {
        return new EpisodesIterator(episodes, itemExtractor, clipsAdapter);
    }

    private static class EpisodesIterator implements Iterator<Item> {

        private Iterator<NitroItemSource<Episode>> episodes;
        private NitroItemSource<Episode> episode;
        private final NitroEpisodeExtractor itemExtractor;
        private final GlycerinNitroClipsAdapter clipsAdapter;

        public EpisodesIterator(Iterable<NitroItemSource<Episode>> episodes, NitroEpisodeExtractor itemExtractor,
                                GlycerinNitroClipsAdapter clipsAdapter) {
            this.episodes = episodes.iterator();
            this.itemExtractor = itemExtractor;
            this.clipsAdapter = clipsAdapter;
        }

        @Override
        public boolean hasNext() {
            return episodes.hasNext();
        }

        @Override
        public Item next() {
            this.episode = episodes.next();
            Item item = itemExtractor.extract(episode);
            List<Clip> clips = getClips(
                    episode.getProgramme().getPid(),
                    episode.getProgramme().getUri()
            );
            item.setClips(clips);
            return item;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public List<Clip> getClips(String pid, String uri) {
            PidReference pidReference = new PidReference();
            pidReference.setPid(pid);
            pidReference.setHref(uri);

            List<Clip> clips;
            try {
                clips = clipsAdapter.clipsFor(pidReference);
            } catch (NitroException e) {
                throw Throwables.propagate(e);
            }
            return clips;
        }
    }
}
