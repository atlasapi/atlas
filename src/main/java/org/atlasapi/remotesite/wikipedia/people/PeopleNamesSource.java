package org.atlasapi.remotesite.wikipedia.people;

public interface PeopleNamesSource {
    Iterable<String> getAllPeopleNames();
}
