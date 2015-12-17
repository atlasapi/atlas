package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil.curieFor;
import static org.atlasapi.remotesite.bbc.nitro.extract.NitroUtil.uriFor;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import com.metabroadcast.atlas.glycerin.model.Brand;

public class NitroCrewMemberExtractor implements
        ContentExtractor<Brand.Contributions.Contribution, Optional<CrewMember>> {

    private static final Publisher PUBLISHER = Publisher.BBC_NITRO;

    @Override
    public Optional<CrewMember> extract(Brand.Contributions.Contribution contribution) {
        Optional<CrewMember.Role> role = CrewMember.Role.fromPossibleTvaCode(contribution.getCreditRole()
                .getId());

        if (!role.isPresent()) {
            return Optional.absent();
        }

        CrewMember crewMember = crewMemberFor(contribution, role.get()).withRole(role.get());

        if (contribution.getContributor().getName() != null) {
            crewMember.withName(fullName(contribution.getContributor().getName()));
        }

        return Optional.of(crewMember);
    }

    private static CrewMember crewMemberFor(Brand.Contributions.Contribution contribution,
            CrewMember.Role role) {
        CrewMember crewMember;
        if (CrewMember.Role.ACTOR.equals(role)) {
            crewMember = createActor(contribution);
        } else {
            crewMember = createCrewMember(contribution);
        }
        return crewMember;
    }

    private static CrewMember createCrewMember(Brand.Contributions.Contribution contribution) {
        return new CrewMember(uriFor(contribution), curieFor(contribution), PUBLISHER);
    }

    private static CrewMember createActor(Brand.Contributions.Contribution contribution) {
        return new Actor(uriFor(contribution), curieFor(contribution), PUBLISHER)
                .withCharacter(contribution.getCharacterName());
    }

    private static String fullName(Brand.Contributions.Contribution.Contributor.Name name) {
        return Joiner.on(" ").skipNulls().join(
                Strings.emptyToNull(name.getGiven()),
                Strings.emptyToNull(name.getFamily())
        );
    }
}
