package org.atlasapi.remotesite.btvod;


import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$productTag;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BtVodSynthesizedSeriesWriterTest {

    private static final String BRAND_TITLE = "Brand Title";
    private static final String PRODUCT_ID = "1234";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = BRAND_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;
    private static final String SERIES_GUID = "series_guid";
    private static final Topic NEW_TOPIC = new Topic(123L);

    private final MergingContentWriter contentWriter = mock(MergingContentWriter.class);
    private final BtVodBrandWriter brandExtractor = mock(BtVodBrandWriter.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageExtractor imageExtractor = mock(ImageExtractor.class);
    private final TopicCreatingTopicResolver topicResolver = mock(TopicCreatingTopicResolver.class);
    private final TopicWriter topicWriter = mock(TopicWriter.class);
    private final BtVodContentMatchingPredicate newTopicContentMatchingPredicate = mock(BtVodContentMatchingPredicate.class);
    private final BtVodSeriesUriExtractor seriesUriExtractor = mock(BtVodSeriesUriExtractor.class);

    private final BtVodDescribedFieldsExtractor describedFieldsExtractor = new BtVodDescribedFieldsExtractor(
            topicResolver,
            topicWriter,
            Publisher.BT_VOD,
            newTopicContentMatchingPredicate,
            NEW_TOPIC
    );

    private final TopicRef newTopicRef = new TopicRef(
            NEW_TOPIC,
            1.0f,
            false,
            TopicRef.Relationship.ABOUT
    );

    private final BtVodSynthesizedSeriesWriter seriesExtractor = new BtVodSynthesizedSeriesWriter(
            brandExtractor,
            PUBLISHER,
            contentListener,
            describedFieldsExtractor, 
            Sets.<String>newHashSet(),
            seriesUriExtractor,
            ImmutableSet.of(SERIES_GUID),
            imageExtractor,
            newTopicRef,
            contentWriter
    );

    @Test
    public void testExtractsSeriesFromEpisode() {
        BtVodEntry entry = row();

        String brandUri = "http://brand-uri.com";
        ParentRef brandRef = mock(ParentRef.class);

        when(brandExtractor.brandUriFor(entry)).thenReturn(Optional.of(brandUri));
        when(brandExtractor.getBrandRefFor(entry)).thenReturn(Optional.of(brandRef));
        when(seriesUriExtractor.extractSeriesNumber(entry)).thenReturn(Optional.of(1));
        when(seriesUriExtractor.seriesUriFor(entry)).thenReturn(Optional.of(brandUri + "/series/1"));

        ArgumentCaptor<Series> captor = ArgumentCaptor.forClass(Series.class);

        seriesExtractor.process(entry);

        verify(contentWriter).write(captor.capture());
        Series series = captor.getValue();
        assertThat(series.getCanonicalUri(), is(brandUri + "/series/1"));
        assertThat(series.getSeriesNumber(), is(1));
        assertThat(series.getParent(), is(brandRef));
        assertThat(seriesExtractor.getSeriesFor(entry).get().getCanonicalUri(), is(brandUri + "/series/1"));
    }

    @Test
    public void testDoesntExtractsSeriesFromEpisodeWhichAlreadyHasExplicitSeriesExtracted() {
        BtVodEntry entry = row();
        entry.setParentGuid(SERIES_GUID);
        when(seriesUriExtractor.extractSeriesNumber(entry)).thenReturn(Optional.of(1));
        when(seriesUriExtractor.seriesUriFor(entry)).thenReturn(Optional.of("URI"));

        seriesExtractor.process(entry);

        verifyNoMoreInteractions(contentWriter);
    }



    @Test
    public void testDoesntExtractSeriesFromNonEpisode() {
        BtVodEntry entry = row();
        entry.setProductType("film");

        String brandUri = "http://brand-uri.com";
        ParentRef brandRef = mock(ParentRef.class);

        when(brandExtractor.brandUriFor(entry)).thenReturn(Optional.of(brandUri));
        when(brandExtractor.getBrandRefFor(entry)).thenReturn(Optional.of(brandRef));

        seriesExtractor.process(entry);

        verify(contentWriter, never()).write(Mockito.any(Series.class));
    }

    @Test
    public void testPropagatesNewTagToBrand() {
        BtVodEntry entry = row();

        String brandUri = "http://brand-uri.com";
        ParentRef brandRef = mock(ParentRef.class);

        when(brandExtractor.brandUriFor(entry)).thenReturn(Optional.of(brandUri));
        when(brandExtractor.getBrandRefFor(entry)).thenReturn(Optional.of(brandRef));
        when(seriesUriExtractor.extractSeriesNumber(entry)).thenReturn(Optional.of(1));
        when(seriesUriExtractor.seriesUriFor(entry)).thenReturn(Optional.of(brandUri + "/series/1"));
        when(newTopicContentMatchingPredicate.apply(org.mockito.Matchers.<VodEntryAndContent>anyObject())).thenReturn(true);

        ArgumentCaptor<Series> captor = ArgumentCaptor.forClass(Series.class);

        seriesExtractor.process(entry);

        verify(contentWriter).write(captor.capture());
        Series series = captor.getValue();
        assertThat(series.getTopicRefs().contains(newTopicRef), is(true));

        verify(brandExtractor).addTopicTo(entry, newTopicRef);
    }


    private BtVodEntry row() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setId("12345");
        entry.setTitle(FULL_EPISODE_TITLE);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductType("episode");// "Apr 30 2014 12:00AM"
        entry.setProductTags(ImmutableList.<BtVodPlproduct$productTag>of());
        entry.setProductScopes(ImmutableList.<BtVodProductScope>of());
        return entry;
    }
}