package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Optional;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaUpdatesProcessorTest {

    @Mock private ProgData progData;
    @Mock private DateTimeZone dateTimeZone;
    private Timestamp timestamp = Timestamp.of(DateTime.now());

    @Mock private Item item;
    @Mock private Episode episode;
    @Mock private Brand brand;
    @Mock private Series series;
    @Mock private Brand brandSummary;
    @Mock private Series seriesSummary;

    @Mock private PaProgDataUpdatesProcessor processor;
    @Mock private ContentWriter contentWriter;

    private PaUpdatesProcessor updatesProcessor;

    @Before
    public void setUp() throws Exception {
        updatesProcessor = PaUpdatesProcessor.create(processor, contentWriter);
    }

    @Test
    public void writeSummaries() throws Exception {
        setupMocks(
                item,
                Optional.<Brand>absent(),
                Optional.<Series>absent(),
                Optional.of(brandSummary),
                Optional.of(seriesSummary)
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(contentWriter).createOrUpdate(brandSummary);
        verify(contentWriter).createOrUpdate(seriesSummary);
    }

    @Test
    public void doNotFailIfSummariesAreAbsent() throws Exception {
        setupMocks(
                item,
                Optional.<Brand>absent(),
                Optional.<Series>absent(),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(contentWriter, never()).createOrUpdate(brandSummary);
        verify(contentWriter, never()).createOrUpdate(seriesSummary);
    }

    @Test
    public void writeItem() throws Exception {
        setupMocks(
                item,
                Optional.<Brand>absent(),
                Optional.<Series>absent(),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(contentWriter).createOrUpdate(item);
    }

    @Test
    public void writeItemWithBrandAndSeries() throws Exception {
        setupMocks(
                item,
                Optional.of(brand),
                Optional.of(series),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(item).setContainer(brand);
        verify(series).setParent(brand);

        verify(contentWriter).createOrUpdate(item);
        verify(contentWriter).createOrUpdate(brand);
        verify(contentWriter).createOrUpdate(series);
    }

    @Test
    public void writeItemWithOnlyBrand() throws Exception {
        setupMocks(
                item,
                Optional.of(brand),
                Optional.<Series>absent(),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(item).setContainer(brand);

        verify(contentWriter).createOrUpdate(item);
        verify(contentWriter).createOrUpdate(brand);
    }

    @Test
    public void writeItemWithOnlySeries() throws Exception {
        setupMocks(
                item,
                Optional.<Brand>absent(),
                Optional.of(series),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(item).setContainer(series);

        verify(contentWriter).createOrUpdate(item);
        verify(contentWriter).createOrUpdate(series);
    }

    @Test
    public void writeEpisodeWithBrandAndSeries() throws Exception {
        setupMocks(
                episode,
                Optional.of(brand),
                Optional.of(series),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(episode).setContainer(brand);
        verify(series).setParent(brand);
        verify(episode).setSeries(series);

        verify(contentWriter).createOrUpdate(episode);
        verify(contentWriter).createOrUpdate(brand);
        verify(contentWriter).createOrUpdate(series);
    }

    @Test
    public void writeEpisodeWithOnlyBrand() throws Exception {
        setupMocks(
                episode,
                Optional.of(brand),
                Optional.<Series>absent(),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(episode).setContainer(brand);

        verify(contentWriter).createOrUpdate(episode);
        verify(contentWriter).createOrUpdate(brand);
    }

    @Test
    public void writeEpisodeWithOnlySeries() throws Exception {
        setupMocks(
                episode,
                Optional.<Brand>absent(),
                Optional.of(series),
                Optional.<Brand>absent(),
                Optional.<Series>absent()
        );

        updatesProcessor.process(progData, dateTimeZone, timestamp);

        verify(episode).setContainer(series);
        verify(episode, never()).setSeries(series);

        verify(contentWriter).createOrUpdate(episode);
        verify(contentWriter).createOrUpdate(series);
    }

    private void setupMocks(Item item, Optional<Brand> brand, Optional<Series> series,
            Optional<Brand> brandSummary, Optional<Series> seriesSummary) {
        when(processor.process(progData, dateTimeZone, timestamp))
                .thenReturn(Optional.of(new ContentHierarchyWithoutBroadcast(
                        brand, series, item, brandSummary, seriesSummary
                )));
    }
}
