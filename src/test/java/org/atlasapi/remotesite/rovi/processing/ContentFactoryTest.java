package org.atlasapi.remotesite.rovi.processing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.remotesite.rovi.model.RoviShowType;
import org.junit.Test;


public class ContentFactoryTest {

    @Test
    public void testContentCreation() {
        Content content;
        
        content = ContentFactory.createContent(RoviShowType.MOVIE);
        assertThat(content, is(Film.class));
        
        content = ContentFactory.createContent(RoviShowType.OTHER);
        assertThat(content, is(Item.class));
        
        content = ContentFactory.createContent(RoviShowType.SERIES_EPISODE);
        assertThat(content, is(Episode.class));

        content = ContentFactory.createContent(RoviShowType.SERIES_MASTER);
        assertThat(content, is(Brand.class));
    }
    
    @Test
    public void testHasTheRightType() {
        Content content;
        
        content = new Film();
        assertTrue(ContentFactory.hasCorrectType(content, RoviShowType.MOVIE));
        
        content = new Brand();
        assertTrue(ContentFactory.hasCorrectType(content, RoviShowType.SERIES_MASTER));
        
        content = new Episode();
        assertTrue(ContentFactory.hasCorrectType(content, RoviShowType.SERIES_EPISODE));

        content = new Item();
        assertTrue(ContentFactory.hasCorrectType(content, RoviShowType.OTHER));
        
        content = new Episode();
        assertFalse(ContentFactory.hasCorrectType(content, RoviShowType.MOVIE));
        
        content = new Episode();
        assertFalse(ContentFactory.hasCorrectType(content, RoviShowType.OTHER));

        content = new Item();
        assertFalse(ContentFactory.hasCorrectType(content, RoviShowType.MOVIE));
    }
    
    @Test
    public void testConsistency() {
        for (RoviShowType showType: RoviShowType.values()) {
            Content content = ContentFactory.createContent(showType);
            assertTrue(ContentFactory.hasCorrectType(content, showType));
        }
    }
    
}
