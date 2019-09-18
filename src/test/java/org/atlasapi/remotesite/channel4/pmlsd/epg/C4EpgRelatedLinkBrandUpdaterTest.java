package org.atlasapi.remotesite.channel4.pmlsd.epg;

import org.atlasapi.remotesite.channel4.pmlsd.C4BrandUpdater;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class C4EpgRelatedLinkBrandUpdaterTest {

    private final C4BrandUpdater backingUpdater = mock(C4BrandUpdater.class);
    
    private final C4EpgRelatedLinkBrandUpdater updater = new C4EpgRelatedLinkBrandUpdater(backingUpdater);
    
    @Test(expected=IllegalArgumentException.class)
    public void testDoesntUpdateInvalidUriAndThrowsException() {
        updater.createOrUpdateBrand(new ModelWithPayload<>(
                "this is not a URI",
                null));
        verify(backingUpdater, never()).createOrUpdateBrand(anyModelWithPayload());
    }

    @Test()
    public void testUpdatesBrandForBrandOnlyRelatedLink() {
        checkUpdates("http://pmlsc.channel4.com/pmlsd/freshly-squeezed.atom", 
            "http://pmlsc.channel4.com/pmlsd/freshly-squeezed");
    }
    
    @Test()
    public void testUpdatesBrandForSeriesRelatedLink() {
        checkUpdates("http://pmlsc.channel4.com/pmlsd/freshly-squeezed/episode-guide/series-1.atom", 
                "http://pmlsc.channel4.com/pmlsd/freshly-squeezed");
    }
    @Test()
    public void testUpdatesBrandForEpisodeRelatedLink() {
        checkUpdates("http://pmlsc.channel4.com/pmlsd/freshly-squeezed/episode-guide/series-1/episode-3.atom", 
                "http://pmlsc.channel4.com/pmlsd/freshly-squeezed");
    }

    private void checkUpdates(String input, final String delegate) {
        updater.createOrUpdateBrand(new ModelWithPayload<>(
                input,
                null));
        verify(backingUpdater, atMost(1)).createOrUpdateBrand(anyModelWithPayload());
        verify(backingUpdater, atLeast(1)).createOrUpdateBrand(anyModelWithPayload());
    }

    static <T> ModelWithPayload<T> anyModelWithPayload() {
        return argThat(new ArgumentMatcher<ModelWithPayload<T>>() {

            @Override
            public boolean matches(Object o) {
                if (!(o instanceof ModelWithPayload)) {
                    return false;
                }
                return true;
            }
        });
    }

}
