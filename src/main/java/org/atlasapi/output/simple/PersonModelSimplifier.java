package org.atlasapi.output.simple;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class PersonModelSimplifier extends DescribedModelSimplifier<Person, org.atlasapi.media.entity.simple.Person> {

    private final UpcomingItemsResolver upcomingResolver;
    private final AvailableItemsResolver availableResolver;

    public PersonModelSimplifier(ImageSimplifier imageSimplifier, UpcomingItemsResolver upcomingResolver, AvailableItemsResolver availableResolver) {
        super(imageSimplifier, SubstitutionTableNumberCodec.lowerCaseOnly(), null);
        this.upcomingResolver = checkNotNull(upcomingResolver);
        this.availableResolver = checkNotNull(availableResolver);
    }
    
    @Override
    public org.atlasapi.media.entity.simple.Person simplify(Person fullPerson, Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Person person = new org.atlasapi.media.entity.simple.Person();

        person.setType(Person.class.getSimpleName());
        copyBasicDescribedAttributes(fullPerson, person, annotations);

        person.setName(fullPerson.getTitle());
        // TODO new alias
        person.setProfileLinks(fullPerson.getAliasUrls());
        person.setContent(simpleContentListFrom(fullPerson.getContents()));
        person.setGivenName(fullPerson.getGivenName());
        person.setFamilyName(fullPerson.getFamilyName());
        person.setGender(fullPerson.getGender());
        person.setBirthDate(fullPerson.getBirthDate());
        person.setBirthPlace(fullPerson.getBirthPlace());
        person.setQuotes(fullPerson.getQuotes());
        person.setAwards(fullPerson.getAwards());
        
        if (annotations.contains(Annotation.UPCOMING)) {
            ImmutableSet<String> upcomingUris = ImmutableSet.copyOf(Iterables.transform(
                    upcomingResolver.upcomingItemsFor(fullPerson), ChildRef.TO_URI));
            person.setUpcomingContent(simpleContentListFrom(filterContent(fullPerson, upcomingUris)));
        }

        if (annotations.contains(Annotation.AVAILABLE_LOCATIONS)) {
            ImmutableSet<String> availableUris = ImmutableSet.copyOf(Iterables.transform(
                    availableResolver.availableItemsFor(fullPerson, config), ChildRef.TO_URI));
            
            person.setAvailableContent(simpleContentListFrom(filterContent(fullPerson, availableUris)));
        }
        
        return person;
    }

    private Iterable<ChildRef> filterContent(Person fullPerson, ImmutableSet<String> validUris) {
        return Iterables.filter(fullPerson.getContents(),
            MorePredicates.transformingPredicate(ChildRef.TO_URI, Predicates.in(validUris))
        );
    }

    private List<ContentIdentifier> simpleContentListFrom(Iterable<ChildRef> contents) {
        List<ContentIdentifier> contentList = Lists.newArrayList();
        for (ChildRef ref : contents) {
            contentList.add(ContentIdentifier.identifierFor(ref, idCodec));
        }
        return contentList;
    }
}
