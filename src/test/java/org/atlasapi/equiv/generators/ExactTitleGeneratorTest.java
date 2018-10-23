package org.atlasapi.equiv.generators;

import com.google.common.collect.ImmutableMap;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExactTitleGeneratorTest {

    private ExactTitleGenerator<Item> generator;
    private MongoContentResolver resolver = mock(MongoContentResolver.class);

    @Before
    public void setUp() {
        generator = new ExactTitleGenerator<>(
                resolver,
                Item.class,
                true,
                Publisher.AMAZON_UNBOX
        );
    }

    @Test
    public void doesNotReturnOriginalContent() {
        Item item1 = makeItem(1L, "ite", Publisher.AMAZON_UNBOX);
        Item item2 = makeItem(2L, "ite", Publisher.AMAZON_UNBOX);
        Item item3 = makeItem(3L, "ite", Publisher.AMAZON_UNBOX);

        ResolvedContent retList = ResolvedContent.builder().putAll(
                ImmutableMap.of(
                        item1.getCanonicalUri(), item1,
                        item2.getCanonicalUri(), item2,
                        item3.getCanonicalUri(), item3
                )
        ).build();

        Item subject = makeItem(2L, "ite", Publisher.AMAZON_UNBOX);

        when(resolver.getExactTitleMatches(any(Content.class))).thenReturn(retList);

        ScoredCandidates<Item> candidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("c", "unbox.amazon.co.uk")
        );

        Set<Item> candMap = candidates.candidates().keySet();

        assertThat(candMap.contains(item1), is(true));
        assertThat(candMap.contains(item2), is(false));
        assertThat(candMap.contains(item3), is(true));
    }

    @Test
    public void discardsNonExactTitles() {
        Item item1 = makeItem(1L, "ite", Publisher.AMAZON_UNBOX);
        Item item2 = makeItem(2L, "ite", Publisher.AMAZON_UNBOX);
        Item item3 = makeItem(3L, "iTe", Publisher.AMAZON_UNBOX);
        Item item4 = makeItem(4L, "ITE", Publisher.AMAZON_UNBOX);

        ResolvedContent retList = ResolvedContent.builder().putAll(
                ImmutableMap.of(
                        item1.getCanonicalUri(), item1,
                        item2.getCanonicalUri(), item2,
                        item3.getCanonicalUri(), item3,
                        item4.getCanonicalUri(), item4
                )
        ).build();

        Item subject = makeItem(2L, "ite", Publisher.AMAZON_UNBOX);

        when(resolver.getExactTitleMatches(any(Content.class))).thenReturn(retList);

        ScoredCandidates<Item> candidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("c", "unbox.amazon.co.uk")
        );

        Set<Item> candMap = candidates.candidates().keySet();

        assertThat(candMap.contains(item1), is(true));
        assertThat(candMap.contains(item2), is(false));
        assertThat(candMap.contains(item3), is(false));
        assertThat(candMap.contains(item4), is(false));
    }

    @Test
    public void runsWhenDetectsNonStandardCharacters() {
        Item item1 = makeItem(1L, "日の出の海と瞑想音楽", Publisher.AMAZON_UNBOX);
        Item item2 = makeItem(2L, "日の出の海と瞑想音楽", Publisher.AMAZON_UNBOX);
        Item item3 = makeItem(3L, "日の", Publisher.AMAZON_UNBOX);

        ResolvedContent retList = ResolvedContent.builder().putAll(
                ImmutableMap.of(
                        item1.getCanonicalUri(), item1,
                        item2.getCanonicalUri(), item2
                )
        ).build();

        Item subject = makeItem(2L, "日の出の海と瞑想音楽", Publisher.AMAZON_UNBOX);

        when(resolver.getExactTitleMatches(any(Content.class))).thenReturn(retList);

        ScoredCandidates<Item> candidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("c", "unbox.amazon.co.uk")
        );

        Set<Item> candMap = candidates.candidates().keySet();

        assertThat(candMap.contains(item1), is(true));
        assertThat(candMap.contains(item2), is(false));
        assertThat(candMap.contains(item3), is(false));
    }

    @Test
    public void doesNotRunWhenSubjectContainsOnlyStandardCharsOverLengthOfThree() {
        Item subject = makeItem(2L, "itemitemitem", Publisher.AMAZON_UNBOX);

        ScoredCandidates<Item> candidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("c", "unbox.amazon.co.uk")
        );

        assertThat(candidates.candidates().isEmpty(), is(true));
        verify(resolver, never()).getExactTitleMatches(any(Content.class));
    }

    @Test
    public void doesNotRunWhenSubjectTitleIsEmpty() {
        Item subject = makeItem(2L, "", Publisher.AMAZON_UNBOX);

        ScoredCandidates<Item> candidates = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResults.create("c", "unbox.amazon.co.uk")
        );

        assertThat(candidates.candidates().isEmpty(), is(true));
        verify(resolver, never()).getExactTitleMatches(any(Content.class));
    }

    private Item makeItem(long id, String title, Publisher publisher) {
        Item item = new Item();
        item.setCanonicalUri(title+id);
        item.setId(id);
        item.setTitle(title);
        item.setPublisher(publisher);

        return item;
    }

}