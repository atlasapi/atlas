package org.atlasapi.remotesite.metabroadcast.similar;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.SimilarContentRef;
import org.atlasapi.media.entity.testing.BrandTestDataBuilder;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


@RunWith(MockitoJUnitRunner.class)
public class DefaultSimilarContentProviderTest {

    private static final int SIMILAR_CONTENT_LIMIT = 10;
    private static final Publisher PUBLISHER = Publisher.BBC;
    private static final Set<String> GENRE_SET_A = ImmutableSet.of("http://genres.com/1", "http://genres.com/2");
    private static final Set<String> GENRE_SET_B = ImmutableSet.of("http://genres.com/2", "http://genres.com/3");
    private static final Set<Integer> HASHES_FOR_SET_A = ImmutableSet.of(1, 2);
    private static final Set<Integer> HASHES_FOR_SET_B = ImmutableSet.of(2, 3);
    
    private final ContentLister contentLister = mock(ContentLister.class);
    private final TraitHashCalculator traitHashCalculator = mock(TraitHashCalculator.class);
    private final AvailableItemsResolver availableItemsResolver = mock(AvailableItemsResolver.class);
    private final UpcomingItemsResolver upcomingItemsResolver = mock(UpcomingItemsResolver.class);
    
    private final DefaultSimilarContentProvider similarContentProvider 
        = new DefaultSimilarContentProvider(contentLister, PUBLISHER, SIMILAR_CONTENT_LIMIT, 
                traitHashCalculator, availableItemsResolver, upcomingItemsResolver);
    
    @Test
    public void testInitialise() {
        ContentListingCriteria expectedCriteria = ContentListingCriteria
                                                    .defaultCriteria()
                                                    .forPublisher(PUBLISHER)
                                                    .forContent(ContentCategory.TOP_LEVEL_CONTENT)
                                                    .build();
        
        when(contentLister.listContent(expectedCriteria)).thenReturn(ImmutableSet.<Content>of().iterator());
        similarContentProvider.initialise();
        verify(contentLister).listContent(expectedCriteria);
    }
    
    @Test
    /**
     * Test that ranking occurs and those brands that are most similar are
     * retained, according to the number of similar brands that should be
     * kept
     */
    public void testSimilarTo() {
        Set<Content> brands = Sets.newHashSet();
        Set<Content> expectedBrands = Sets.newHashSet();
        for (int i = 0; i < SIMILAR_CONTENT_LIMIT + 1; i++) {
            brands.add(testBrand(i, GENRE_SET_A));
        }
        Brand target = (Brand) Iterables.getFirst(brands, null);
        
        // All brands with the same genres should be returned as similar content, save for
        // the target brand itself
        expectedBrands.addAll(Sets.difference(brands, ImmutableSet.of(target)));
        
        for (int i = 0; i < 10; i++) {
            brands.add(testBrand(i + expectedBrands.size(), GENRE_SET_B));
        }   
        
        ContentListingCriteria expectedCriteria = ContentListingCriteria
                .defaultCriteria()
                .forPublisher(PUBLISHER)
                .forContent(ContentCategory.TOP_LEVEL_CONTENT)
                .build();
        
        when(contentLister.listContent(expectedCriteria)).thenReturn(brands.iterator());
        
        //TODO make these mocks better
        when(upcomingItemsResolver.upcomingItemsByPublisherFor((Container) anyObject()))
                .thenReturn(ImmutableMultimap.<Publisher, ChildRef>of());
        when(availableItemsResolver.availableItemsByPublisherFor((Container) anyObject(), (ApplicationConfiguration) anyObject()))
                .thenReturn(ImmutableMultimap.<Publisher, ChildRef>of());
        
        for (Content c : brands) {
            when(traitHashCalculator.traitHashesFor(c)).thenReturn(hashesFor(c));
        }
        similarContentProvider.initialise();
        
        Set<SimilarContentRef> expected = ImmutableSet.copyOf(Iterables.transform(expectedBrands, TO_SIMILAR_CONTENT_REF));
        assertThat(ImmutableSet.copyOf(similarContentProvider.similarTo(target)), is(expected));
        
    }
    
    private Set<Integer> hashesFor(Content b) {
        if (b.getGenres().equals(GENRE_SET_A)) {
            return HASHES_FOR_SET_A;
        } else if (b.getGenres().equals(GENRE_SET_B)) {
            return HASHES_FOR_SET_B;
        } else {
            throw new IllegalArgumentException();
        }
    }
    
    private Brand testBrand(int id, Iterable<String> genres) {
        return BrandTestDataBuilder
                    .brand()
                    .withCanonicalUri(String.format("http://brand.com/%d", id))
                    .withGenres(genres)
                    .withId(id)
                    .build();
    }
    
    private static Function<Content, SimilarContentRef> TO_SIMILAR_CONTENT_REF = new Function<Content, SimilarContentRef>() {

        @Override
        public SimilarContentRef apply(Content c) {
            return SimilarContentRef.builder()
                                    .withEntityType(EntityType.from(c))
                                    .withId(c.getId())
                                    .withUri(c.getCanonicalUri())
                                    .withScore(3)
                                    .build();
        }
        
    };
    
}
