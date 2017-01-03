package org.atlasapi.remotesite.rt.extractors;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import nu.xom.Element;
import nu.xom.Elements;

import static com.google.common.base.Preconditions.checkNotNull;

public class RtPeopleExtractor {

    private static final String CAST = "cast";
    private static final String DIRECTION = "direction";
    private static final String ACTOR = "actor";
    private static final String ROLE = "role";
    private static final String ADDITIONAL_INFO = "additional_info";
    private static final String WRITING = "writing";
    private static final String BILLING = "billing";
    private static final String SOURCE = "source";
    private static final String SOURCE_TITLE = "source_title";
    private static final String PSEUDO_SURNAME = "pseudo_surname";
    private static final String PSEUDO_FORENAME = "pseudo_forename";
    private static final String SURNAME = "surname";
    private static final String FORENAME = "forename";
    private static final String DIRECTOR = "director";

    private Element filmElement;
    private Film film;
    private final AdapterLog log;
    private List<Person> people;

    private RtPeopleExtractor(Element filmElement, Film film, AdapterLog log) {
        this.filmElement = checkNotNull(filmElement);
        this.film = checkNotNull(film);
        this.log = checkNotNull(log);
        List<Person> people = Lists.newArrayList();
    }

    public static RtPeopleExtractor create(Element filmElement, Film film, AdapterLog log) {
        return new RtPeopleExtractor(filmElement, film, log);
    }

    public List<Person> getPeople() {
        return people;
    }

    public void process() {
        List<CrewMember> otherPublisherPeople = getOtherPublisherPeople(film);

        if (otherPublisherPeople.isEmpty()) {
            film.setPeople(ImmutableList.copyOf(
                    Iterables.concat(
                            getActors(filmElement.getFirstChildElement(CAST)),
                            getDirectors(filmElement.getFirstChildElement(DIRECTION))
                    )));
        } else {
            film.setPeople(otherPublisherPeople);
        }

        people = Lists.newArrayList(
                Iterables.concat(
                        makeActors(filmElement.getFirstChildElement(CAST)),
                        makeDirectors(filmElement.getFirstChildElement(DIRECTION)),
                        makeWriters(filmElement.getFirstChildElement(WRITING))
                ));


    }

    private List<Person> makeActors(Element cast) {
        List<Person> people = Lists.newArrayList();

        if (!hasValue(cast)) {
            return people;
        }

        Elements actors = cast.getChildElements(ACTOR);

        for (int i = 0; i < actors.size(); i++) {
            Element actor = actors.get(i);
            people.add(makeActor(actor));
        }

        return people;
    }

    private List<Person> makeDirectors(Element cast) {
        List<Person> people = Lists.newArrayList();

        if (!hasValue(cast)) {
            return people;
        }

        Elements directors = cast.getChildElements(ACTOR);

        for (int i = 0; i < directors.size(); i++) {
            Element director = directors.get(i);
            people.add(makeDirector(director));
        }

        return people;
    }

    private List<Person> makeWriters(Element cast) {
        List<Person> people = Lists.newArrayList();

        if (!hasValue(cast)) {
            return people;
        }

        Elements writers = cast.getChildElements(ACTOR);

        for (int i = 0; i < writers.size(); i++) {
            Element writer = writers.get(i);
            people.add(makeWriter(writer));
        }

        return people;
    }

    private Person makeActor(Element actor) {
        Person person = makePersonWithNames(actor);

        Element additionalInfo = actor.getFirstChildElement(ADDITIONAL_INFO);
        if (hasValue(additionalInfo)) {
            person.setAdditionalInfo(additionalInfo.getValue());
        }

        Element billing = actor.getFirstChildElement(BILLING);
        if (hasValue(billing)) {
            person.setBilling(billing.getValue());
        }

        return person;
    }

    private Person makeWriter(Element writer) {
        Person person = makePersonWithNames(writer);

        Element source = writer.getFirstChildElement(SOURCE);
        if (hasValue(source)) {
            person.setSource(source.getValue());
        }

        Element sourceTitle = writer.getFirstChildElement(SOURCE_TITLE);
        if (hasValue(sourceTitle)) {
            person.setSourceTitle(sourceTitle.getValue());
        }

        return person;
    }

    private Person makeDirector(Element director) {
        Person person = makePersonWithNames(director);

        Element additionalInfo = director.getFirstChildElement(ADDITIONAL_INFO);
        if (hasValue(additionalInfo)) {
            person.setAdditionalInfo(additionalInfo.getValue());
        }

        return person;
    }

    private Person makePersonWithNames(Element element) {
        Person person = new Person();

        Element forename = element.getFirstChildElement(FORENAME);
        if (hasValue(forename)) {
            person.setGivenName(forename.getValue());
        }

        Element surname = element.getFirstChildElement(SURNAME);
        if (hasValue(surname)) {
            person.setFamilyName(surname.getValue());
        }

        Element pseudoForename = element.getFirstChildElement(PSEUDO_FORENAME);
        if (hasValue(pseudoForename)) {
            person.setPseudoForename(pseudoForename.getValue());
        }

        Element pseudoSurname = element.getFirstChildElement(PSEUDO_SURNAME);
        if (hasValue(pseudoSurname)) {
            person.setPseudoSurname(pseudoSurname.getValue());
        }

        return person;
    }

    public boolean hasValue(@Nullable Element subtitlesElement) {
        return subtitlesElement != null && !Strings.isNullOrEmpty(subtitlesElement.getValue());
    }

    private List<CrewMember> getOtherPublisherPeople(Film film) {
        return film.getPeople().stream()
                .filter(crewMember -> crewMember.publisher() != Publisher.RADIO_TIMES)
                .collect(MoreCollectors.toImmutableList());
    }

    private List<Actor> getActors(Element castElement) {
        List<Actor> actors = Lists.newArrayList();

        if (!hasValue(castElement)) {
            return actors;
        }

        Elements actorElements = castElement.getChildElements(ACTOR);

        for (int i = 0; i < actorElements.size(); i++) {
            Element actorElement = actorElements.get(i);

            String role = actorElement.getFirstChildElement(ROLE).getValue();

            Optional<String> optionalName = makeName(actorElement);
            optionalName.ifPresent(name -> actors.add(Actor.actorWithoutId(
                    name,
                    role,
                    Publisher.RADIO_TIMES
            )));

        }

        return actors;
    }

    private List<CrewMember> getDirectors(Element directionElement) {
        List<CrewMember> actors = Lists.newArrayList();

        if (!hasValue(directionElement)) {
            return actors;
        }

        Elements directorElements = directionElement.getChildElements(DIRECTOR);

        for (int i = 0; i < directorElements.size(); i++) {
            Element directorElement = directorElements.get(i);

            String role = directorElement.getFirstChildElement(ROLE).getValue();
            role = role.trim().replace(" ", "_").toLowerCase();

            Optional<String> optionalName = makeName(directorElement);

            if (optionalName.isPresent()) {
                if (CrewMember.Role.fromPossibleKey(role).isNothing()) {
                    log.record(new AdapterLogEntry(
                            AdapterLogEntry.Severity.WARN
                    ).withSource(getClass()).withDescription(String.format(
                            "Ignoring crew member with unrecognised role: %s", role
                            ))
                    );
                } else {
                    actors.add(CrewMember.crewMemberWithoutId(
                            optionalName.get(),
                            role,
                            Publisher.RADIO_TIMES
                    ));
                }
            }
        }

        return actors;
    }

    private Optional<String> makeName(Element personElement) {

        Element forename = personElement.getFirstChildElement(FORENAME);
        Element surname = personElement.getFirstChildElement(SURNAME);

        if (forename == null && surname == null) {
            log.record(new AdapterLogEntry(AdapterLogEntry.Severity.WARN)
                    .withDescription(String.format(
                            "Person found with no makeName: %s",
                            personElement.toXML()
                    )).withSource(getClass()));
            return Optional.empty();
        }

        if (forename != null && surname != null) {
            return  Optional.of(String.format("%s %s",forename.getValue(), surname.getValue()));
        } else {
            return Optional.of(forename != null ? forename.getValue() : surname.getValue());
        }
    }
}
