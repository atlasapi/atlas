package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.media.entity.Person;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class SimplePersonModelSimplifier {

    public org.atlasapi.media.entity.simple.Person simplify(Person fullPerson) {
        org.atlasapi.media.entity.simple.Person person = new org.atlasapi.media.entity.simple.Person();

        person.setType(Person.class.getSimpleName());

        person.setName(fullPerson.getTitle());
        person.setProfileLinks(fullPerson.getAliasUrls());
        person.setGivenName(fullPerson.getGivenName());
        person.setFamilyName(fullPerson.getFamilyName());
        person.setGender(fullPerson.getGender());
        person.setBirthDate(fullPerson.getBirthDate());
        person.setBirthPlace(fullPerson.getBirthPlace());
        person.setQuotes(fullPerson.getQuotes());

        person.setPseudoSurname(fullPerson.getPseudoSurname());
        person.setPseudoForename(fullPerson.getPseudoForename());
        person.setAdditionalInfo(fullPerson.getAdditionalInfo());
        person.setBilling(fullPerson.getBilling());
        person.setSource(fullPerson.getSource());
        person.setSourceTitle(fullPerson.getSourceTitle());

        return person;
    }
}
