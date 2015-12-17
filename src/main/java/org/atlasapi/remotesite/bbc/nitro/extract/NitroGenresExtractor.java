package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroGenre;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroGenreGroup;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

public class NitroGenresExtractor implements ContentExtractor<List<NitroGenreGroup>, Set<String>> {

    private static final String PREFIX = "http://www.bbc.co.uk/programmes/genres/";
    private static final String ID_PREFIX = "http://nitro.bbc.co.uk/genres/";

    @Override
    public Set<String> extract(List<NitroGenreGroup> genreGroups) {
        ImmutableSet.Builder<String> genres = ImmutableSet.builder();
        for (NitroGenreGroup genreGroup : genreGroups) {
            extractGenres(genres, genreGroup);
        }
        return genres.build();
    }

    private Iterable<String> extractGenres(ImmutableSet.Builder<String> genres,
            NitroGenreGroup genreGroup) {
        String parent = null;
        List<NitroGenre> groupGenres = genreGroup.getGenres();
        for (NitroGenre nitroGenre : groupGenres.subList(0, Math.min(groupGenres.size(), 2))) {
            parent = extractGenre(nitroGenre, parent);
            genres.add(PREFIX + parent);
            genres.add(ID_PREFIX + nitroGenre.getId());
        }
        return null;
    }

    private String extractGenre(NitroGenre nitroGenre, String parent) {
        String unescaped = StringEscapeUtils.unescapeHtml(nitroGenre.getTitle());
        String adapted = unescaped.toLowerCase().replaceAll("&", "and").replaceAll(" ", "");
        if (!Strings.isNullOrEmpty(parent)) {
            adapted = String.format("%s/%s", parent, adapted);
        }
        return adapted;
    }

}
