package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.atlas.glycerin.model.Genre;
import com.metabroadcast.atlas.glycerin.model.GenreGroup;

public class NitroGenresExtractor implements ContentExtractor<List<GenreGroup>, Set<String>> {
    private static final String PREFIX = "http://www.bbc.co.uk/programmes/genres/";
    private static final String ID_PREFIX = "http://nitro.bbc.co.uk/genres/";

    @Override
    public Set<String> extract(List<GenreGroup> genreGroups) {
        ImmutableSet.Builder<String> genres = ImmutableSet.builder();
        for(GenreGroup genreGroup : genreGroups) {
            extractGenres(genres, genreGroup);
        }
        return genres.build();
    }

    private void extractGenres(ImmutableSet.Builder<String> genres, GenreGroup genreGroup) {
        String parent = null;
        List<Genre> groupGenres = genreGroup.getGenres().getGenre();
        for(Genre genre : groupGenres.subList(0, Math.min(groupGenres.size(), 2))) {
            parent = extractGenre(genre, parent);
            genres.add(PREFIX + parent);
            genres.add(ID_PREFIX + genre.getId());
        }
    }

    private String extractGenre(Genre genre, String parent) {
        String unescaped = StringEscapeUtils.unescapeHtml(genre.getValue());
        String adapted = unescaped.toLowerCase().replaceAll("&", "and").replaceAll(" ", "");
        if(!Strings.isNullOrEmpty(parent)) {
            adapted = String.format("%s/%s", parent, adapted);
        }
        return adapted;
    }
}

