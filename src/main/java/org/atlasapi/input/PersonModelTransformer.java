package org.atlasapi.input;

import java.util.Set;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.simple.Person;
import org.atlasapi.media.entity.simple.SameAs;
import org.atlasapi.persistence.content.PeopleResolver;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.Clock;

public class PersonModelTransformer extends DescribedModelTransformer<Person, org.atlasapi.media.entity.Person> {

    private PeopleResolver resolver;

    public PersonModelTransformer(Clock clock, PeopleResolver resolver) {
        super(clock);
        this.resolver = resolver;
    }
    
    @Override
    protected org.atlasapi.media.entity.Person createDescribedOutput(Person simple, DateTime now) {
        org.atlasapi.media.entity.Person person = new org.atlasapi.media.entity.Person();
        
        person.setGivenName(simple.getGivenName());
        person.setFamilyName(simple.getFamilyName());
        person.withName(simple.getName());
        person.setGender(simple.getGender());
        person.setBirthDate(simple.getBirthDate());
        person.setBirthPlace(simple.getBirthPlace());
        person.setQuotes(simple.getQuotes());
        
        return person;
    }
    
    @Override
    protected Set<LookupRef> resolveEquivalents(Set<String> sameAs) {
        return ImmutableSet.copyOf(Iterables.transform(Optional.presentInstances(Iterables.transform(sameAs,
                new Function<String, Optional<org.atlasapi.media.entity.Person>>() {
                    @Override
                    public Optional<org.atlasapi.media.entity.Person> apply(String input) {
                        return resolver.person(input);
                    }
                })),
                LookupRef.FROM_DESCRIBED));
    }

    @Override
    protected Set<LookupRef> resolveSameAs(Set<SameAs> sameAs) {
        return ImmutableSet.copyOf(Iterables.transform(Optional.presentInstances(Iterables.transform(sameAs,
                new Function<SameAs, Optional<org.atlasapi.media.entity.Person>>() {
                    @Override
                    public Optional<org.atlasapi.media.entity.Person> apply(SameAs input) {
                        return resolver.person(input.getUri());
                    }
                })),
                LookupRef.FROM_DESCRIBED));
    }

}
