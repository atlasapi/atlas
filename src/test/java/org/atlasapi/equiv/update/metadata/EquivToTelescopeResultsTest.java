package org.atlasapi.equiv.update.metadata;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EquivToTelescopeResultsTest {

    private EquivToTelescopeResults equivToTelescopeResults;

    @Before
    public void setUp() {
        equivToTelescopeResults = EquivToTelescopeResults.create("id", "publisher");
    }

    @Test
    public void jsonOutput() {
        EquivToTelescopeComponent generator = EquivToTelescopeComponent.create();
        generator.addComponentResult("id", "5.0");
        generator.addComponentResult(23848, "0.0");
        equivToTelescopeResults.addGeneratorResult(generator);

        EquivToTelescopeComponent scorer1 = EquivToTelescopeComponent.create();
        scorer1.addComponentResult("scorer1id", "2");
        scorer1.addComponentResult(28238, "3");
        scorer1.addComponentResult("scorer1id2", "22");
        scorer1.addComponentResult(282382, "32");
        equivToTelescopeResults.addScorerResult(scorer1);

        EquivToTelescopeComponent scorer2 = EquivToTelescopeComponent.create();
        scorer2.addComponentResult("scorer2id", "4");
        scorer2.addComponentResult(23232, "4");
        equivToTelescopeResults.addScorerResult(scorer2);


        assertEquals(equivToTelescopeResults.getGenerators().get(0), generator);
        assertEquals(equivToTelescopeResults.getScorers().size(), 2);


        // print json at the end to be checked visually
        Gson gson = new Gson();
        JsonElement equivResultsJson = gson.toJsonTree(equivToTelescopeResults);
        System.out.println(equivResultsJson);
    }

}