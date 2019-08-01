package org.atlasapi.equiv.update.metadata;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EquivToTelescopeResultTest {

    private EquivToTelescopeResult equivToTelescopeResult;

    @Before
    public void setUp() {
        equivToTelescopeResult = EquivToTelescopeResult.create("id", "publisher");
    }

    @Test
    public void jsonOutput() {
        EquivToTelescopeComponent generator = EquivToTelescopeComponent.create();
        generator.addComponentResult("id", "5.0");
        generator.addComponentResult(23848, "0.0");
        equivToTelescopeResult.addGeneratorResult(generator);

        EquivToTelescopeComponent scorer1 = EquivToTelescopeComponent.create();
        scorer1.addComponentResult("scorer1id", "2");
        scorer1.addComponentResult(28238, "3");
        scorer1.addComponentResult("scorer1id2", "22");
        scorer1.addComponentResult(282382, "32");
        equivToTelescopeResult.addScorerResult(scorer1);

        EquivToTelescopeComponent scorer2 = EquivToTelescopeComponent.create();
        scorer2.addComponentResult("scorer2id", "4");
        scorer2.addComponentResult(23232, "4");
        equivToTelescopeResult.addScorerResult(scorer2);


        assertEquals(equivToTelescopeResult.getGenerators().get(0), generator);
        assertEquals(equivToTelescopeResult.getScorers().size(), 2);
    }

}
