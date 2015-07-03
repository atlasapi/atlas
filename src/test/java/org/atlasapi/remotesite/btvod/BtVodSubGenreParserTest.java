package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class BtVodSubGenreParserTest {


    private final BtVodSubGenreParser objectUnderTest = new BtVodSubGenreParser();

    @Test
    public void testParseSubGenresString() throws Exception {

        String subGenresString = "[\"Comedy\",\"Action\",\"Crime\"]";


        assertThat(
                objectUnderTest.parse(subGenresString),
                is((List)ImmutableList.of("Comedy", "Action", "Crime"))
        );
    }


}