package org.atlasapi.equiv.update;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MultipleSourceEquivalenceUpdaterTest {

    @Mock private OwlTelescopeReporter telescope = mock(OwlTelescopeReporter.class);

    @Test
    public void test() {
        
        EquivalenceUpdater<Item> itemUpdater = mockedUpdater("item");
        EquivalenceUpdater<Container> containerUpdater = mockedUpdater("container"); 
        
        MultipleSourceEquivalenceUpdater updaters = MultipleSourceEquivalenceUpdater.create();
        
        Episode ep = new Episode("uri", "curie", Publisher.BBC);
        
        updaters.register(Publisher.BBC, SourceSpecificEquivalenceUpdater.builder(Publisher.BBC)
                .withItemUpdater(itemUpdater)
                .withTopLevelContainerUpdater(containerUpdater)
                .build());
        
        updaters.updateEquivalences(ep, telescope);
        verify(itemUpdater).updateEquivalences(ep, telescope);
        
    }

    @SuppressWarnings("unchecked")
    private <T> EquivalenceUpdater<T> mockedUpdater(String name) {
        return (EquivalenceUpdater<T>) mock(EquivalenceUpdater.class, name);
    }

}
