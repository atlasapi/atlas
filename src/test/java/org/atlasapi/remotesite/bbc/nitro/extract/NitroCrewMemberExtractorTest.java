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
        Brand.People.Contribution contribution = companyContribution();

        CrewMember crewMember = extractor.extract(contribution).get();

        assertThat(crewMember, not(instanceOf(Actor.class)));
        assertEquals(PRODUCTION_COMPANY, crewMember.role());
        assertEquals("So Television", crewMember.name());
    }

    @Test
    public void testActorExtraction() {
        Brand.People.Contribution contribution = actorContribution();

        CrewMember crewMember = extractor.extract(contribution).get();

        assertThat(crewMember, is(instanceOf(Actor.class)));
        assertEquals(ACTOR, crewMember.role());
        assertEquals("Carey Mulligan", crewMember.name());
    }

    private Brand.People.Contribution companyContribution() {
        Brand.People.Contribution contribution = new Brand.People.Contribution();
        contribution.setContributionBy("p029hdv9");
        contribution.setCreditRole("Production Company");
        contribution.setCreditRoleId("V20");

        Brand.People.Contribution.ContributorName name = new Brand.People.Contribution.ContributorName();
        name.setFamily("So Television");
        contribution.setContributorName(name);

        return contribution;
    }

    private Brand.People.Contribution actorContribution() {
        Brand.People.Contribution contribution = new Brand.People.Contribution();
        contribution.setContributionBy("p01fvgrh");
        contribution.setCharacterName("Irene");
        contribution.setCreditRole("Actor");
        contribution.setCreditRoleId("ACTOR");

        Brand.People.Contribution.ContributorName name = new Brand.People.Contribution.ContributorName();
        name.setGiven("Carey");
        name.setFamily("Mulligan");
        contribution.setContributorName(name);

        return contribution;
    }

}