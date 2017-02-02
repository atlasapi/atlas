package org.atlasapi.output.simple;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;


public class OrganisationModelSimplifier extends DescribedModelSimplifier<Organisation, org.atlasapi.media.entity.simple.Organisation> {

    private final PersonModelSimplifier personSimplifier;

    public OrganisationModelSimplifier(ImageSimplifier imageSimplifier, PersonModelSimplifier personSimplifier, NumberToShortStringCodec codec) {
        super(imageSimplifier, codec, null);
        this.personSimplifier = checkNotNull(personSimplifier);
    }

    @Override
    public org.atlasapi.media.entity.simple.Organisation simplify(Organisation model,
            final Set<Annotation> annotations, final ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Organisation organisation = new org.atlasapi.media.entity.simple.Organisation();
        
        organisation.setType(Organisation.class.getSimpleName());
        copyBasicDescribedAttributes(model, organisation, annotations);
        
        organisation.setMembers(simplifyMembers(model.members(), annotations, config));
        
        return organisation;
    }

    private Iterable<org.atlasapi.media.entity.simple.Person> simplifyMembers(List<Person> members,
            final Set<Annotation> annotations, final ApplicationConfiguration config) {
        return Iterables.transform(members, new Function<Person, org.atlasapi.media.entity.simple.Person>() {
            @Override
            public org.atlasapi.media.entity.simple.Person apply(Person input) {
                return personSimplifier.simplify(input, annotations, config);
            }
        });
    }

}
