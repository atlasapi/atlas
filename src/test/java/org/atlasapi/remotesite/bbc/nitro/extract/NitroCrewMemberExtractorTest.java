package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.atlasapi.media.entity.CrewMember.Role.ACTOR;
import static org.atlasapi.media.entity.CrewMember.Role.PRODUCTION_COMPANY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.junit.Test;

import com.metabroadcast.atlas.glycerin.model.Brand;

public class NitroCrewMemberExtractorTest {

    private NitroCrewMemberExtractor extractor = new NitroCrewMemberExtractor();

    @Test
    public void testNonActorExtraction() {
        Brand.Contributions.Contribution contribution = companyContribution();

        CrewMember crewMember = extractor.extract(contribution).get();

        assertThat(crewMember, not(instanceOf(Actor.class)));
        assertEquals(PRODUCTION_COMPANY, crewMember.role());
        assertEquals("So Television", crewMember.name());
    }

    @Test
    public void testActorExtraction() {
        Brand.Contributions.Contribution contribution = actorContribution();

        CrewMember crewMember = extractor.extract(contribution).get();

        assertThat(crewMember, is(instanceOf(Actor.class)));
        assertEquals(ACTOR, crewMember.role());
        assertEquals("Carey Mulligan", crewMember.name());
    }

    private Brand.Contributions.Contribution companyContribution() {
        Brand.Contributions.Contribution contribution = new Brand.Contributions.Contribution();

        Brand.Contributions.Contribution.Contributor contributor = new Brand.Contributions.Contribution.Contributor();
        contributor.setHref("p029hdv9");

        Brand.Contributions.Contribution.Contributor.Name name = new Brand.Contributions.Contribution.Contributor.Name();
        name.setFamily("So Television");
        contributor.setName(name);

        contribution.setContributor(contributor);

        Brand.Contributions.Contribution.CreditRole creditRole = new Brand.Contributions.Contribution.CreditRole();
        creditRole.setId("V20");
        creditRole.setValue("Production Company");
        contribution.setCreditRole(creditRole);

        return contribution;
    }

    private Brand.Contributions.Contribution actorContribution() {
        Brand.Contributions.Contribution contribution = new Brand.Contributions.Contribution();
        contribution.setCharacterName("Irene");

        Brand.Contributions.Contribution.Contributor contributor = new Brand.Contributions.Contribution.Contributor();
        contributor.setHref("p01fvgrh");

        Brand.Contributions.Contribution.Contributor.Name name = new Brand.Contributions.Contribution.Contributor.Name();
        name.setGiven("Carey");
        name.setFamily("Mulligan");
        contributor.setName(name);

        contribution.setContributor(contributor);

        Brand.Contributions.Contribution.CreditRole creditRole = new Brand.Contributions.Contribution.CreditRole();
        creditRole.setId("ACTOR");
        creditRole.setValue("Actor");
        contribution.setCreditRole(creditRole);

        return contribution;
    }

}