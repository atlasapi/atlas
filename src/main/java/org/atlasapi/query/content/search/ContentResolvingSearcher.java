package org.atlasapi.query.content.search;

import java.util.List;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.ContentQueryBuilder;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.query.content.fuzzy.FuzzySearcher;
import org.atlasapi.search.model.Search;
import org.atlasapi.search.model.SearchResults;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.query.Selection;

public class ContentResolvingSearcher implements SearchResolver {
    private final FuzzySearcher fuzzySearcher;
    private final KnownTypeQueryExecutor contentResolver;

    public ContentResolvingSearcher(FuzzySearcher fuzzySearcher, KnownTypeQueryExecutor contentResolver) {
        this.fuzzySearcher = fuzzySearcher;
        this.contentResolver = contentResolver;
    }

    @Override
    public List<Identified> search(Search search, Iterable<Publisher> publishers, ApplicationConfiguration appConfig, Selection selection) {
        SearchResults searchResults = fuzzySearcher.contentSearch(search.query(), selection, publishers);
        if (searchResults.toUris().isEmpty()) {
            return ImmutableList.of();
        }

        ContentQuery query = ContentQueryBuilder.query().isAnEnumIn(Attributes.DESCRIPTION_PUBLISHER, ImmutableList.<Enum<Publisher>>copyOf(publishers)).withSelection(selection).build();
        List<Identified> content = contentResolver.executeUriQuery(searchResults.toUris(), query.copyWithApplicationConfiguration(appConfig));
        return filterOutSubItems(content);
    }

    private List<Identified> filterOutSubItems(Iterable<Identified> contents) {
        ImmutableList.Builder<Identified> filteredContent = ImmutableList.builder();
        
        for (Identified identified: contents) {
            Identified filtered = null;
            if (identified instanceof Brand) {
                Brand brand = (Brand) ((Brand) identified).copy();
                brand.setContents(ImmutableList.<Episode>of());
                brand.setClips(ImmutableList.<Clip>of());
                filtered = brand;
            } else if (identified instanceof Series) {
                Series series = (Series) ((Series) identified).copy();
                series.setContents(ImmutableList.<Episode>of());
                series.setClips(ImmutableList.<Clip>of());
                filtered = series;
            } else if (identified instanceof Content){
                Content content = (Content) identified;
                filtered = content.copy();
            } else if (identified instanceof Person) {
                Person person = (Person) ((Person) identified).copy();
                person.setContents(ImmutableList.<Item>of());
                filtered = person;
            }
            
            if (filtered != null) {
                filteredContent.add(filtered);
            }
        }
        return filteredContent.build();
    }
}
