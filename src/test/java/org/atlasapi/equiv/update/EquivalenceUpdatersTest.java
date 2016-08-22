package org.atlasapi.equiv.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EquivalenceUpdatersTest {

    
    @Test
    public void test() {
        
        EquivalenceUpdater<Item> itemUpdater = mockedUpdater("item");
        EquivalenceUpdater<Container> containerUpdater = mockedUpdater("container"); 
        
        EquivalenceUpdaters updaters = new EquivalenceUpdaters();
        
        Episode ep = new Episode("uri", "curie", Publisher.BBC);
        
        updaters.register(Publisher.BBC, SourceSpecificEquivalenceUpdater.builder(Publisher.BBC)
                .withItemUpdater(itemUpdater)
                .withTopLevelContainerUpdater(containerUpdater)
                .build());
        
        updaters.updateEquivalences(ep, null, null);
        verify(itemUpdater).updateEquivalences(ep, null, null);
        
    }

    @SuppressWarnings("unchecked")
    private <T> EquivalenceUpdater<T> mockedUpdater(String name) {
        return (EquivalenceUpdater<T>) mock(EquivalenceUpdater.class, name);
    }

}
