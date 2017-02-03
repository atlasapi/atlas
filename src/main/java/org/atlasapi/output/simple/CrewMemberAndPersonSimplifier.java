package org.atlasapi.output.simple;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Person;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class CrewMemberAndPersonSimplifier implements
        ModelSimplifier<CrewMemberAndPerson, org.atlasapi.media.entity.simple.Person> {
    
    private final PersonModelSimplifier personHelper;
    private final CrewMemberSimplifier crewHelper;

    public CrewMemberAndPersonSimplifier(
            ImageSimplifier imageSimplifier,
            UpcomingItemsResolver upcomingResolver,
            AvailableItemsResolver availableResolver
    ) {
        this.personHelper = new PersonModelSimplifier(imageSimplifier, upcomingResolver, availableResolver);
        this.crewHelper = new CrewMemberSimplifier();
    }
    
    @Override
    public org.atlasapi.media.entity.simple.Person simplify(
            CrewMemberAndPerson model,
            Set<Annotation> annotations,
            Application application
    ) {

        CrewMember crew = model.getMember();
        Optional<Person> possiblePerson = model.getPerson();
        
        org.atlasapi.media.entity.simple.Person simplePerson;
        if (possiblePerson.isPresent()) {
            simplePerson = personHelper.simplify(possiblePerson.get(), annotations, application);
        } else {
            simplePerson = crewHelper.simplify(crew, annotations, application);
        }
        
        if (crew instanceof Actor) {
            Actor actor = (Actor) crew;
            simplePerson.setCharacter(actor.character());
        }
        
        simplePerson.setName(crew.name());
        simplePerson.setProfileLinks(crew.profileLinks());
        if (crew.role() != null) {
            simplePerson.setRole(crew.role().key());
            simplePerson.setDisplayRole(crew.role().title());
        }
        
        //remove references to content on people on content.
        simplePerson.setContent(ImmutableList.of());
        
        return simplePerson;
    }
    
}
