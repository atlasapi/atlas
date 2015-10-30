package org.atlasapi.remotesite.btvod;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BtVodExplicitSeriesExtractorTest {

    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String PRODUCT_ID = "1234";
    private static final String SERIES_TITLE = "Brand1 Season 1";
    private static final String BT_VOD_VERSION_GUID_NAMESPACE = "version:guid:namespace";
    private static final String BT_VOD_VERSION_ID_NAMESPACE = "version:id:namespace";


    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandProvider brandProvider = mock(BtVodBrandProvider.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageExtractor imageExtractor = mock(ImageExtractor.class);
    private final BtVodSeriesUriExtractor seriesUriExtractor = mock(BtVodSeriesUriExtractor.class);

    private final BtVodDescribedFieldsExtractor describedFieldsExtractor = mock(BtVodDescribedFieldsExtractor.class);
    private final TopicRef newTopic = mock(TopicRef.class);

    private BtVodExplicitSeriesExtractor seriesExtractor;

    @Before
    public void setUp() {
        seriesExtractor = new BtVodExplicitSeriesExtractor(
                brandProvider,
                PUBLISHER,
                contentListener,
                describedFieldsExtractor,
                Sets.<String>newHashSet(),
                seriesUriExtractor,
                new BtVodVersionsExtractor(
                        new BtVodPricingAvailabilityGrouper(),
                        "prefix",
                        BT_VOD_VERSION_GUID_NAMESPACE,
                        BT_VOD_VERSION_ID_NAMESPACE,
                        null,
                        null
                ),
                new TitleSanitiser(),
                imageExtractor,
                ImmutableSet.of(newTopic)
        );
    }


    @Test
    public void testDoesntExtractsSeriesFromEntryWhichIsNotSeason() {
        BtVodEntry entry = row();
        entry.setProductType("episode");

        seriesExtractor.process(entry);
        assertThat(seriesExtractor.getExplicitSeries().isEmpty(), is(true));

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
        when(brandProvider.brandRefFor(entry)).thenReturn(Optional.of(brandRef));
        when(describedFieldsExtractor.aliasesFrom(entry)).thenReturn(ImmutableSet.of(alias1, alias2));
        when(describedFieldsExtractor.btGenreStringsFrom(entry)).thenReturn(ImmutableSet.of(genre));


        seriesExtractor.process(entry);

        Series series = Iterables.getOnlyElement(seriesExtractor.getExplicitSeries().values());

        verify(describedFieldsExtractor).setDescribedFieldsFrom(entry, series);

        assertThat(series.getCanonicalUri(), is("seriesUri"));
        assertThat(series.getSeriesNumber(), is(1));
        assertThat(series.getParent(), is(brandRef));
        assertThat(series.getAliases(), CoreMatchers.<Set<Alias>>is(ImmutableSet.of(alias1, alias2)));
        assertThat(series.getGenres(), CoreMatchers.<Set<String>>is(ImmutableSet.of(genre)));
        assertThat(seriesExtractor.getExplicitSeries().get(PRODUCT_ID), is(series));
    }

    @Test
    public void testDeduplicatesSeries() {
        String series1Id = "GUID1";
        String series2Id = "GUID2";
        String seriesUri = "seriesUri";

        BtVodEntry series1 = row();
        series1.setProductType("season");
        series1.setParentGuid(series1Id);
        series1.setGuid(series1Id);

        BtVodEntry series2 = row();
        series2.setProductType("season");
        series2.setParentGuid(series2Id);
        series2.setGuid(series2Id);

        Alias alias1 = mock(Alias.class);
        Alias alias2 = mock(Alias.class);

        ParentRef brandRef = mock(ParentRef.class);

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(seriesUri))).thenReturn(ResolvedContent.builder().build());

        when(seriesUriExtractor.seriesUriFor(series1)).thenReturn(Optional.of(seriesUri));
        when(seriesUriExtractor.extractSeriesNumber(series1)).thenReturn(Optional.of(1));
        when(brandProvider.brandRefFor(series1)).thenReturn(Optional.of(brandRef));

        when(seriesUriExtractor.seriesUriFor(series2)).thenReturn(Optional.of(seriesUri));
        when(seriesUriExtractor.extractSeriesNumber(series2)).thenReturn(Optional.of(1));
        when(brandProvider.brandRefFor(series2)).thenReturn(Optional.of(brandRef));



        when(describedFieldsExtractor.aliasesFrom(series1)).thenReturn(ImmutableSet.of(alias1));
        when(describedFieldsExtractor.btGenreStringsFrom(series1)).thenReturn(ImmutableSet.<String>of());

        when(describedFieldsExtractor.aliasesFrom(series2)).thenReturn(ImmutableSet.of(alias2));
        when(describedFieldsExtractor.btGenreStringsFrom(series2)).thenReturn(ImmutableSet.<String>of());

        ArgumentCaptor<Series> seriesCaptor = ArgumentCaptor.forClass(Series.class);

        ArgumentCaptor<BtVodEntry> entryCaptor = ArgumentCaptor.forClass(BtVodEntry.class);

        seriesExtractor.process(series1);
        seriesExtractor.process(series2);

        verify(describedFieldsExtractor, times(2)).setDescribedFieldsFrom(entryCaptor.capture(), seriesCaptor.capture());



        Series savedSeries = seriesCaptor.getAllValues().get(0);
        Series savedSeries2 = seriesCaptor.getAllValues().get(1);

        assertThat(savedSeries, sameInstance(savedSeries2));
        assertThat(seriesExtractor.getExplicitSeries().get(series1Id), is(savedSeries));
        assertThat(seriesExtractor.getExplicitSeries().get(series2Id), is(savedSeries));
        assertThat(entryCaptor.getAllValues().get(0), is(series1));
        assertThat(entryCaptor.getAllValues().get(1), is(series2));
        assertThat(savedSeries.getAliases(), CoreMatchers.<Set<Alias>>is(ImmutableSet.of(alias1, alias2)));

    }

    @Test
    public void testPropagatesNewTagToBrand() {
        BtVodEntry entry = row();
        entry.setProductType("season");// "Apr 30 2014 12:00AM"
        ParentRef brandRef = mock(ParentRef.class);
        Alias alias1 = mock(Alias.class);
        Alias alias2 = mock(Alias.class);
        String genre = "genre1";
        Brand brand = mock(Brand.class);


        when(contentResolver.findByCanonicalUris(ImmutableSet.of("seriesUri"))).thenReturn(ResolvedContent.builder().build());

        when(seriesUriExtractor.seriesUriFor(entry)).thenReturn(Optional.of("seriesUri"));
        when(seriesUriExtractor.extractSeriesNumber(entry)).thenReturn(Optional.of(1));
        when(brandProvider.brandRefFor(entry)).thenReturn(Optional.of(brandRef));
        when(brandProvider.brandFor(entry)).thenReturn(Optional.of(brand));
        when(describedFieldsExtractor.aliasesFrom(entry)).thenReturn(ImmutableSet.of(alias1, alias2));
        when(describedFieldsExtractor.btGenreStringsFrom(entry)).thenReturn(ImmutableSet.of(genre));
        when(describedFieldsExtractor.topicsFrom(Matchers.<VodEntryAndContent>anyObject())).thenReturn(ImmutableSet.of(newTopic));


        seriesExtractor.process(entry);


        Series series = Iterables.getOnlyElement(seriesExtractor.getExplicitSeries().values());

        verify(describedFieldsExtractor).setDescribedFieldsFrom(entry, series);

        assertThat(series.getTopicRefs().contains(newTopic), is(true));

        verify(brand).addTopicRef(newTopic);

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