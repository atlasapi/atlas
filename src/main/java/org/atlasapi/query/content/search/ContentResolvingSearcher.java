package org.atlasapi.query.content.search;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import com.metabroadcast.applications.client.model.internal.Application;
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
import com.metabroadcast.common.collect.DedupingIterator;

public class ContentResolvingSearcher implements SearchResolver {
    private final ContentSearcher fuzzySearcher;
    private KnownTypeQueryExecutor contentResolver;
    private PeopleQueryResolver peopleQueryResolver;

    public ContentResolvingSearcher(
            ContentSearcher fuzzySearcher,
            KnownTypeQueryExecutor contentResolver,
            PeopleQueryResolver peopleQueryResolver
    ) {
        this.fuzzySearcher = checkNotNull(fuzzySearcher);
        this.contentResolver = contentResolver;
        this.peopleQueryResolver = peopleQueryResolver;
    }

    @Override
    public List<Identified> search(SearchQuery query, Application application) {
        SearchResults searchResults = fuzzySearcher.search(query);
        Iterable<ContentIdentifier> ids = query.getSelection().apply(searchResults.contentIdentifiers());
        if (Iterables.isEmpty(ids)) {
            return ImmutableList.of();
        }

        Map<String, List<Identified>> content = resolveContent(query, application, ids);
        Map<String, Person> people = resolvePeople(application, ids);

        List<Identified> hydrated = Lists.newArrayListWithExpectedSize(Iterables.size(ids));
        for (ContentIdentifier id : ids) {
            List<Identified> identified = content.get(id.getUri());
            if (identified == null) {
                Person person = people.get(id.getUri());
                if (person != null) {
                    identified = ImmutableList.of(person);
                }
            }
            if (identified != null) {
                hydrated.addAll(identified);
            }
        }

        return DedupingIterator.dedupeIterable(hydrated);
    }

    private Map<String, Person> resolvePeople(
            Application application,
            Iterable<ContentIdentifier> ids
    ) {
        
        List<String> people = ImmutableList.copyOf(Iterables.transform(Iterables.filter(ids, PEOPLE), ContentIdentifier.TO_URI));
        
        if (!people.isEmpty()) {
            return Maps.uniqueIndex(DedupingIterator.dedupeIterable(peopleQueryResolver.people(people, application)), TO_URI);
        } else {
            return ImmutableMap.of();
        }
    }

    private Map<String, List<Identified>> resolveContent(
            SearchQuery query,
            Application application,
            Iterable<ContentIdentifier> ids
    ) {
        
        List<String> contentUris = ImmutableList.copyOf(Iterables.transform(Iterables.filter(ids, Predicates.not(PEOPLE)),
                ContentIdentifier.TO_URI));
        
        if (!contentUris.isEmpty()) {
            ContentQuery contentQuery = ContentQueryBuilder.query()
                    .isAnEnumIn(Attributes.DESCRIPTION_PUBLISHER, ImmutableList.<Enum<Publisher>> copyOf(query.getIncludedPublishers()))
                    .withSelection(query.getSelection())
                    .withApplication(application)
                    .build();
            
            return contentResolver.executeUriQuery(contentUris, contentQuery);
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
    
    private static Predicate<ContentIdentifier> PEOPLE = input -> input instanceof PersonIdentifier;
    
    private static Function<Person, String> TO_URI = Identified::getCanonicalUri;
}
