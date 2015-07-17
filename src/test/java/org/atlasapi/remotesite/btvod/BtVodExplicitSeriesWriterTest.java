package org.atlasapi.remotesite.btvod;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BtVodExplicitSeriesWriterTest {

    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String PRODUCT_ID = "1234";
    private static final String SERIES_TITLE = "Brand1 Season 1";


    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandWriter brandExtractor = mock(BtVodBrandWriter.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageExtractor imageExtractor = mock(ImageExtractor.class);
    private final TopicCreatingTopicResolver topicResolver = mock(TopicCreatingTopicResolver.class);
    private final TopicWriter topicWriter = mock(TopicWriter.class);
    private final BtVodContentMatchingPredicate newTopicContentMatchingPredicate = mock(BtVodContentMatchingPredicate.class);
    private final BtVodSeriesUriExtractor seriesUriExtractor = mock(BtVodSeriesUriExtractor.class);

    private final BtVodDescribedFieldsExtractor describedFieldsExtractor = mock(BtVodDescribedFieldsExtractor.class);

    private BtVodExplicitSeriesWriter seriesExtractor;

    @Before
    public void setUp() {
        seriesExtractor = new BtVodExplicitSeriesWriter(
                contentWriter,
                contentResolver,
                brandExtractor,
                PUBLISHER,
                contentListener,
                describedFieldsExtractor,
                Sets.<String>newHashSet(),
                seriesUriExtractor,
                new BtVodVersionsExtractor(new BtVodPricingAvailabilityGrouper(), "prefix"),
                new TitleSanitiser()
        );
    }


    @Test
    public void testDoesntExtractsSeriesFromEntryWhichIsNotSeason() {
        BtVodEntry entry = row();
        entry.setProductType("episode");

        seriesExtractor.process(entry);

        verifyNoMoreInteractions(contentWriter);
    }

    @Test
    public void testExtractsSeriesFromSeasonEntries() {
        BtVodEntry entry = row();
        entry.setProductType("season");// "Apr 30 2014 12:00AM"
        ParentRef brandRef = mock(ParentRef.class);
        Alias alias1 = mock(Alias.class);
        Alias alias2 = mock(Alias.class);
        String genre = "genre1";


        when(contentResolver.findByCanonicalUris(ImmutableSet.of("seriesUri"))).thenReturn(ResolvedContent.builder().build());

        when(seriesUriExtractor.seriesUriFor(entry)).thenReturn(Optional.of("seriesUri"));
        when(seriesUriExtractor.extractSeriesNumber(entry)).thenReturn(Optional.of(1));
        when(brandExtractor.getBrandRefFor(entry)).thenReturn(Optional.of(brandRef));
        when(describedFieldsExtractor.aliasesFrom(entry)).thenReturn(ImmutableSet.of(alias1, alias2));
        when(describedFieldsExtractor.btGenreStringsFrom(entry)).thenReturn(ImmutableSet.of(genre));

        ArgumentCaptor<Series> captor = ArgumentCaptor.forClass(Series.class);

        seriesExtractor.process(entry);

        verify(contentWriter).createOrUpdate(captor.capture());
        Series series = captor.getValue();

        verify(describedFieldsExtractor).setDescribedFieldsFrom(entry, series);

        assertThat(series.getCanonicalUri(), is("seriesUri"));
        assertThat(series.getSeriesNumber(), is(1));
        assertThat(series.getParent(), is(brandRef));
        assertThat(series.getAliases(), CoreMatchers.<Set<Alias>>is(ImmutableSet.of(alias1, alias2)));
        assertThat(series.getGenres(), CoreMatchers.<Set<String>>is(ImmutableSet.of(genre)));
        assertThat(seriesExtractor.getExplicitSeries().get(PRODUCT_ID), is(series));

    }

    private BtVodEntry row() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setId("12345");
        entry.setTitle(SERIES_TITLE);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductTags(ImmutableList.<BtVodPlproduct$productTag>of());
        entry.setProductScopes(ImmutableList.<BtVodProductScope>of());
        return entry;
    }
}