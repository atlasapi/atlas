package org.atlasapi.equiv.generators.amazon;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.junit.Test;
import org.mockito.Mock;

import javax.annotation.Nullable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AmazonTitleGeneratorTest {

    private @Mock final AmazonTitleIndexStore amazonTitleIndexStore = mock(AmazonTitleIndexStore.class);
    private @Mock final ContentResolver resolver = mock(ContentResolver.class);

    private AmazonTitleGenerator<Item> itemGenerator = new AmazonTitleGenerator<>(
            amazonTitleIndexStore,
            resolver,
            Item.class,
            Publisher.AMAZON_UNBOX
    );

    private AmazonTitleGenerator<Content> contentGenerator = new AmazonTitleGenerator<>(
            amazonTitleIndexStore,
            resolver,
            Content.class,
            Publisher.AMAZON_UNBOX
    );

    private AmazonTitleGenerator<Content> topLevelContentGenerator = new AmazonTitleGenerator<>(
            amazonTitleIndexStore,
            resolver,
            Content.class,
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

        ScoredCandidates<Item> scoredCandidates = itemGenerator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("contentId", Publisher.AMAZON_UNBOX.key())
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

        ScoredCandidates<Item> scoredCandidates = itemGenerator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("contentId", Publisher.AMAZON_UNBOX.key())
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

        ScoredCandidates<Item> scoredCandidates = itemGenerator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("contentId", Publisher.AMAZON_UNBOX.key())
        );

        assertThat(scoredCandidates.candidates().keySet(), is(ImmutableSet.of(item1, item2)));
    }

    @Test
    public void testGenerationIgnoresDifferentContentType() {
        String title = "title";
        Item subject = item("uri", title);
        Item item1 = item("uri1", title);
        Item item2 = item("uri2", title);
        Brand brand = brand("uri3", title);

        when(amazonTitleIndexStore.getIndexEntry(title))
                .thenReturn(new AmazonTitleIndexEntry(title, ImmutableSet.of("uri1", "uri2", "uri3")));

        when(resolver.findByCanonicalUris(ImmutableSet.of("uri1", "uri2", "uri3")))
                .thenReturn(ResolvedContent.builder()
                        .put("uri1", item1)
                        .put("uri2", item2)
                        .put("uri3", brand)
                        .build()
                );

        ScoredCandidates<Item> scoredCandidates = itemGenerator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("contentId", Publisher.AMAZON_UNBOX.key())
        );

        assertThat(scoredCandidates.candidates().keySet(), is(ImmutableSet.of(item1, item2)));
    }

    @Test
    public void testGenerationOnlyIncludesTopLevelContent() {
        String title = "title";
        Item subject = item("uri", title);
        Item item1 = item("uri1", title);
        Series series2 = series("uri2", title, null);
        Brand brand3 = brand("uri3", title);
        Series series4 = series("uri4", title, brand3);
        Episode episode5 = episode("uri5", title, series2);

        when(amazonTitleIndexStore.getIndexEntry(title))
                .thenReturn(new AmazonTitleIndexEntry(title, ImmutableSet.of("uri1", "uri2", "uri3", "uri4", "uri5")));

        when(resolver.findByCanonicalUris(ImmutableSet.of("uri1", "uri2", "uri3", "uri4", "uri5")))
                .thenReturn(ResolvedContent.builder()
                        .put("uri1", item1)
                        .put("uri2", series2)
                        .put("uri3", brand3)
                        .put("uri4", series4)
                        .put("uri5", episode5)
                        .build()
                );

        ScoredCandidates<Content> scoredCandidates = topLevelContentGenerator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("contentId", Publisher.AMAZON_UNBOX.key())
        );

        assertThat(scoredCandidates.candidates().keySet(), is(ImmutableSet.of(item1, series2, brand3)));
    }

    private <T extends Content> T setCommonFields(T content, String uri, String title) {
        content.setCanonicalUri(uri);
        content.setTitle(title);
        content.setPublisher(Publisher.AMAZON_UNBOX);
        return content;
    }

    private Item item(String uri, String title) {
        Item item = setCommonFields(new Item(), uri, title);
        return item;
    }

    private Episode episode(String uri, String title, Container container) {
        Episode episode = setCommonFields(new Episode(), uri, title);
        episode.setParentRef(ParentRef.parentRefFrom(container));
        return episode;
    }

    private Series series(String uri, String title, @Nullable Brand brand) {
        Series series = setCommonFields(new Series(), uri, title);
        if(brand != null) {
            series.setParentRef(ParentRef.parentRefFrom(brand));
        }
        return series;
    }

    private Brand brand(String uri, String title) {
        Brand brand = setCommonFields(new Brand(), uri, title);
        brand.setPublisher(Publisher.AMAZON_UNBOX);
        return brand;
    }
}