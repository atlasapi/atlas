package org.atlasapi.query.content.search;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.ContentQueryBuilder;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.media.entity.simple.ContentIdentifier.PersonIdentifier;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.search.ContentSearcher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.MoreMaps;
import com.metabroadcast.common.collect.DedupingIterator;

public class ContentResolvingSearcher implements SearchResolver {
    private final ContentSearcher fuzzySearcher;
    private PeopleQueryResolver peopleQueryResolver;
    private KnownTypeQueryExecutor contentResolver;

    public ContentResolvingSearcher(ContentSearcher fuzzySearcher, KnownTypeQueryExecutor contentResolver, PeopleQueryResolver peopleQueryResolver) {
        this.fuzzySearcher = checkNotNull(fuzzySearcher);
        this.contentResolver = contentResolver;
        this.peopleQueryResolver = peopleQueryResolver;
    }

    @Override
    public List<Identified> search(SearchQuery query, ApplicationConfiguration appConfig) {
        SearchResults searchResults = fuzzySearcher.search(query);
        Iterable<ContentIdentifier> ids = query.getSelection().apply(searchResults.contentIdentifiers());
        if (Iterables.isEmpty(ids)) {
            return ImmutableList.of();
        }

        Map<String, List<Identified>> content = resolveContent(query, appConfig, ids); 
        Map<String, Person> people = resolvePeople(appConfig, ids);
        
        List<Identified> hydrated = Lists.newArrayListWithExpectedSize(Iterables.size(ids));
        for (ContentIdentifier id : ids) {
            List<Identified> identified = content.get(id.getUri());
            if (identified == null) {
                Person person = people.get(id.getUri());
                if (person != null) {
                    identified = ImmutableList.<Identified>of(person);
                }
            }
            if (identified != null) {
                hydrated.addAll(identified);
            }
        }
        
        return DedupingIterator.dedupeIterable(hydrated);
    }

    private Map<String, Person> resolvePeople(ApplicationConfiguration appConfig,
            Iterable<ContentIdentifier> ids) {
        
        List<String> people = ImmutableList.copyOf(Iterables.transform(Iterables.filter(ids, PEOPLE), ContentIdentifier.TO_URI));
        
        if (!people.isEmpty()) {
            return Maps.uniqueIndex(DedupingIterator.dedupeIterable(peopleQueryResolver.people(people, appConfig)), TO_URI);
        } else {
            return ImmutableMap.of();
        }
    }

    private Map<String, List<Identified>> resolveContent(SearchQuery query,
            ApplicationConfiguration appConfig, Iterable<ContentIdentifier> ids) {
        
        List<String> contentIds = ImmutableList.copyOf(Iterables.transform(Iterables.filter(ids, Predicates.not(PEOPLE)), 
                ContentIdentifier.TO_URI));
        
        if (!contentIds.isEmpty()) {
            ContentQuery contentQuery = ContentQueryBuilder.query().isAnEnumIn(Attributes.DESCRIPTION_PUBLISHER, 
                    ImmutableList.<Enum<Publisher>> copyOf(query.getIncludedPublishers())).withSelection(query.getSelection()).build();
            
            return contentResolver.executeUriQuery(contentIds,
                    contentQuery.copyWithApplicationConfiguration(appConfig));
        } else {
            return ImmutableMap.of();
        }
    }

    public void setExecutor(KnownTypeQueryExecutor queryExecutor) {
        this.contentResolver = queryExecutor;
    }
    
    public void setPeopleQueryResolver(PeopleQueryResolver peopleQueryResolver) {
        this.peopleQueryResolver = peopleQueryResolver;
    }
    
    private static Predicate<ContentIdentifier> PEOPLE = new Predicate<ContentIdentifier>() {

        @Override
        public boolean apply(ContentIdentifier input) {
            return input instanceof PersonIdentifier;
        }
        
    };
    
    private static Function<Person, String> TO_URI = new Function<Person, String>() {

        @Override
        public String apply(Person input) {
            return input.getCanonicalUri();
        }
        
    };
}
