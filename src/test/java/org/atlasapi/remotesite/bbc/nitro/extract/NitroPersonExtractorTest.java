package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.base.Optional;
import com.metabroadcast.atlas.glycerin.model.Brand;

public class NitroPersonExtractorTest {

    private NitroPersonExtractor extractor = new NitroPersonExtractor();

    @Test
    public void testPersonExtraction() {
        Brand.People.Contribution contribution = new Brand.People.Contribution();
        contribution.setContributionBy("p01fvgrh");
        contribution.setCharacterName("Irene");

        Brand.People.Contribution.ContributorName name = new Brand.People.Contribution.ContributorName();
        name.setGiven("Carey");
        name.setFamily("Mulligan");

        contribution.setContributorName(name);

        Optional<Person> optionalPerson = extractor.extract(contribution);
        assertTrue(optionalPerson.isPresent());

        Person person = optionalPerson.get();
        assertEquals(name.getGiven(), person.getGivenName());
        assertEquals(name.getFamily(), person.getFamilyName());
        assertEquals("http://nitro.bbc.co.uk/people/p01fvgrh", person.getCanonicalUri());
        assertEquals("nitro:bbc:person_p01fvgrh", person.getCurie());
        assertEquals(Publisher.BBC_NITRO, person.getPublisher());
    }

}