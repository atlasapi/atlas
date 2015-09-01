package org.atlasapi.remotesite.bbc.nitro.extract;

import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.base.Optional;
import com.metabroadcast.atlas.glycerin.model.Brand;

public class NitroPersonExtractor implements
        ContentExtractor<Brand.People.Contribution, Optional<Person>> {

    @Override
    public Optional<Person> extract(Brand.People.Contribution contribution) {
        Brand.People.Contribution.ContributorName contributorName = contribution.getContributorName();

        if (contributorName == null) {
            return Optional.absent();
        }

        Person person = new Person();
        person.setCanonicalUri(NitroUtil.uriFor(contribution));
        person.setCurie(NitroUtil.curieFor(contribution));
        person.setGivenName(contributorName.getGiven());
        person.setFamilyName(contributorName.getFamily());
        person.setPublisher(Publisher.BBC_NITRO);

        return Optional.of(person);
    }
}
