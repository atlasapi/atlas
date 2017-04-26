package org.atlasapi.remotesite.pa.people;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.PeopleResolver;
import org.atlasapi.persistence.content.people.PersonWriter;
import org.atlasapi.remotesite.pa.profiles.bindings.Name;
import org.atlasapi.remotesite.pa.profiles.bindings.Picture;
import org.atlasapi.remotesite.pa.profiles.bindings.Pictures;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

public class PaPeopleProcessor {

    private static final String PERSON_URI_PREFIX = "http://people.atlasapi.org/people.pressassociation.com/";
    private static final String PA_PERSON_URI_PREFIX = "http://people.atlasapi.org/pressassociation.com/";
    
    private static final String BASE_IMAGE_URL = "http://images.atlas.metabroadcast.com/people.pressassociation.com/";
    
    private final PeopleResolver personResolver;
    private final PersonWriter personWriter;
    private final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.date().withZoneUTC();

    public PaPeopleProcessor(PeopleResolver personResolver, PersonWriter personWriter) {
        this.personResolver = personResolver;
        this.personWriter = personWriter;
    }
    
    public void process(org.atlasapi.remotesite.pa.profiles.bindings.Person paPerson) {
        Person person = ingestPerson(paPerson);
        Optional<Person> existing = personResolver.person(person.getCanonicalUri());
        if (!existing.isPresent()) {
            personWriter.createOrUpdatePerson(person);
        } else {
            merge(existing.get(), person);
            personWriter.createOrUpdatePerson(existing.get());
        }
    }
    
    private void merge(Person existing, Person newPerson) {
        existing.withName(newPerson.name());
        existing.setGivenName(newPerson.getGivenName());
        existing.setFamilyName(newPerson.getFamilyName());
        existing.setGender(newPerson.getGender());
        existing.setBirthDate(newPerson.getBirthDate());
        existing.setBirthPlace(newPerson.getBirthPlace());
        existing.setDescription(newPerson.getDescription());
        existing.setQuotes(newPerson.getQuotes());
        existing.setPublisher(Publisher.PA_PEOPLE);
        existing.setImages(newPerson.getImages());
        existing.setImage(newPerson.getImage());
    }

    private Person ingestPerson(org.atlasapi.remotesite.pa.profiles.bindings.Person paPerson) {
        Person person = new Person();
        person.setCanonicalUri(PERSON_URI_PREFIX + paPerson.getId());
        
        Name name = paPerson.getName();
        // First and Last name are optional in the dtd so check both are
        // non-null to avoid strange names.
        if (!Strings.isNullOrEmpty(name.getFirstname()) 
            && !Strings.isNullOrEmpty(name.getLastname())) {
            person.withName(name.getFirstname() + " " + name.getLastname());
        }
        person.setGivenName(name.getFirstname());
        person.setFamilyName(name.getLastname());
        
        person.setGender(paPerson.getGender());
        if (paPerson.getBorn() != null) {
            person.setBirthDate(dateTimeFormatter.parseDateTime(paPerson.getBorn()));
        }
        person.setBirthPlace(paPerson.getBornIn());
        person.setDescription(paPerson.getEarlyLife() + "\n\n" + paPerson.getCareer());
        person.addQuote(paPerson.getQuote());
        person.setPublisher(Publisher.PA_PEOPLE);
        person.setImages(extractImages(paPerson.getPictures()));
        person.setImage(getPrimary(person.getImages()));
        setDirectEquivalentToPAPerson(person, paPerson.getId());
        return person;
    }
    
    private String getPrimary(Set<Image> images) {
        for (Image image : images) {
            if (ImageType.PRIMARY.equals(image.getType())) {
                return image.getCanonicalUri();
            }
        }
        return null;
    }

    private ImmutableSet<Image> extractImages(Pictures pictures) {
        return pictures != null ? extractImages(pictures.getPicture())
                                : ImmutableSet.<Image>of();
    }

    private ImmutableSet<Image> extractImages(List<Picture> pictures) {
        ImmutableSet.Builder<Image> images = ImmutableSet.builder();
        for (Picture picture : pictures) {
            if (!Strings.isNullOrEmpty(picture.getvalue())) {
                images.add(extractImage(picture));
            }
        }
        return images.build();
    }

    private Image extractImage(Picture picture) {
        Image image = new Image(BASE_IMAGE_URL + picture.getvalue());
        image.setWidth(Ints.tryParse(picture.getWidth()));
        image.setHeight(Ints.tryParse(picture.getHeight()));
        image.setType(ImageType.PRIMARY);
        return image;
    }

    /**
     * PA People are ingested separately from PA biogs people. Therefore
     * we set a direct equivalence on the PA person if they exist. In the 
     * future this will change to an equivalence job so the equivalence
     * will be asserted at a later stage even if the PA person doesn't
     * exist at the time when the PA biog person is ingested.
     * 
     * @param person
     */
    private void setDirectEquivalentToPAPerson(Person person, String id) {
        Optional<Person> paPerson = personResolver.person(PA_PERSON_URI_PREFIX + id);
        if(paPerson.isPresent()) {
            person.setEquivalentTo(ImmutableSet.of(LookupRef.from(paPerson.get())));
        }
    }
}
