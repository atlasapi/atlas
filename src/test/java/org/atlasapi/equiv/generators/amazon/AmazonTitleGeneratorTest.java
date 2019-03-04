package org.atlasapi.equiv.generators.amazon;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AmazonTitleGeneratorTest {

    private final AmazonTitleIndexStore amazonTitleIndexStore = mock(AmazonTitleIndexStore.class);
    private final ContentResolver resolver = mock(ContentResolver.class);

    private AmazonTitleGenerator<Content> generator = new AmazonTitleGenerator<>(
            amazonTitleIndexStore,
            resolver,
            Item.class,
            Publisher.AMAZON_UNBOX
    );

    @Test
    public void testGenerationFiltersOutSelf() {
        String title = "title";
        Item item1 = item("uri1", title);
        Item item2 = item("uri2", title);
        Item item3 = item("uri3", title);

        when(amazonTitleIndexStore.getIndexEntry(title))
                .thenReturn(new AmazonTitleIndexEntry(title, ImmutableSet.of("uri1", "uri2", "uri3")));

        when(resolver.findByCanonicalUris(ImmutableSet.of("uri1", "uri2", "uri3")))
                .thenReturn(ResolvedContent.builder()
                        .put("uri1", item1)
                        .put("uri2", item2)
                        .put("uri3", item3)
                        .build()
                );

        ScoredCandidates<Content> scoredCandidates = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResults.create("contentId", Publisher.AMAZON_UNBOX.key())
        );

        assertThat(scoredCandidates.candidates().keySet(), is(ImmutableSet.of(item2, item3)));
    }

    @Test
    public void testGenerationExcludesDifferentTitles() {
        String title = "title";
        Item subject = item("uri", title);
        Item item1 = item("uri1", title);
        Item item2 = item("uri2", title);
        Item item3 = item("uri3", "other-" + title);

        when(amazonTitleIndexStore.getIndexEntry(title))
                .thenReturn(new AmazonTitleIndexEntry(title, ImmutableSet.of("uri1", "uri2", "uri3")));

        when(resolver.findByCanonicalUris(ImmutableSet.of("uri1", "uri2", "uri3")))
                .thenReturn(ResolvedContent.builder()
                        .put("uri1", item1)
                        .put("uri2", item2)
                        .put("uri3", item3)
                        .build()
                );

        ScoredCandidates<Content> scoredCandidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("contentId", Publisher.AMAZON_UNBOX.key())
        );

        assertThat(scoredCandidates.candidates().keySet(), is(ImmutableSet.of(item1, item2)));
    }

    @Test
    public void testGenerationExcludesDifferentPublisher() {
        String title = "title";
        Item subject = item("uri", title);
        Item item1 = item("uri1", title);
        Item item2 = item("uri2", title);
        Item item3 = item("uri3", title);
        item3.setPublisher(Publisher.BBC);

        when(amazonTitleIndexStore.getIndexEntry(title))
                .thenReturn(new AmazonTitleIndexEntry(title, ImmutableSet.of("uri1", "uri2", "uri3")));

        when(resolver.findByCanonicalUris(ImmutableSet.of("uri1", "uri2", "uri3")))
                .thenReturn(ResolvedContent.builder()
                        .put("uri1", item1)
                        .put("uri2", item2)
                        .put("uri3", item3)
                        .build()
                );

        ScoredCandidates<Content> scoredCandidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("contentId", Publisher.AMAZON_UNBOX.key())
        );

        assertThat(scoredCandidates.candidates().keySet(), is(ImmutableSet.of(item1, item2)));
    }


    private Item item(String uri, String title) {
        Item item = new Item();
        item.setCanonicalUri(uri);
        item.setTitle(title);
        item.setPublisher(Publisher.AMAZON_UNBOX);
        return item;
    }
}