package org.atlasapi.remotesite.btvod;


import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.ContentMerger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MergingContentWriterTest {

    @Mock
    ContentWriter contentWriter;

    @Mock
    ContentResolver contentResolver;

    @Mock
    ContentMerger contentMerger;

    @InjectMocks
    MergingContentWriter mergingContentWriter;

    @Test
    public void testWritesExistingItem() {
        Item extracted = mock(Item.class);
        when(extracted.getCanonicalUri()).thenReturn("URI");


        Item existing = mock(Item.class);
        when(
                contentResolver.findByCanonicalUris(
                        ImmutableSet.of("URI")
                )
        ).thenReturn(
                ResolvedContent.builder()
                        .put("", existing)
                        .build()
        );

        Item merged = mock(Item.class);
        when(contentMerger.merge(existing, extracted)).thenReturn(merged);
        mergingContentWriter.write(extracted);

        verify(contentWriter).createOrUpdate(merged);
    }

    @Test
    public void testWritesNewItem() {
        Item extracted = mock(Item.class);
        when(extracted.getCanonicalUri()).thenReturn("URI");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of("URI"))).thenReturn(ResolvedContent.builder().build());
        mergingContentWriter.write(extracted);

        verify(contentWriter).createOrUpdate(extracted);
    }

    @Test
    public void testWritesExistingSeries() {
        Series extracted = mock(Series.class);
        when(extracted.getCanonicalUri()).thenReturn("URI");


        Series existing = mock(Series.class);
        when(
                contentResolver.findByCanonicalUris(
                        ImmutableSet.of("URI")
                )
        ).thenReturn(
                ResolvedContent.builder()
                        .put("", existing)
                        .build()
        );

        Series merged = mock(Series.class);
        when(contentMerger.merge(existing, extracted)).thenReturn(merged);
        mergingContentWriter.write(extracted);

        verify(contentWriter).createOrUpdate(merged);
    }

    @Test
    public void testWritesNewSeries() {
        Series extracted = mock(Series.class);
        when(extracted.getCanonicalUri()).thenReturn("URI");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of("URI"))).thenReturn(ResolvedContent.builder().build());
        mergingContentWriter.write(extracted);

        verify(contentWriter).createOrUpdate(extracted);
    }

    @Test
    public void testWritesExistingBrand() {
        Brand extracted = mock(Brand.class);
        when(extracted.getCanonicalUri()).thenReturn("URI");


        Brand existing = mock(Brand.class);
        when(
                contentResolver.findByCanonicalUris(
                        ImmutableSet.of("URI")
                )
        ).thenReturn(
                ResolvedContent.builder()
                        .put("", existing)
                        .build()
        );

        Brand merged = mock(Brand.class);
        when(contentMerger.merge(existing, extracted)).thenReturn(merged);
        mergingContentWriter.write(extracted);

        verify(contentWriter).createOrUpdate(merged);
    }

    @Test
    public void testWritesNewBrand() {
        Brand extracted = mock(Brand.class);
        when(extracted.getCanonicalUri()).thenReturn("URI");

        when(contentResolver.findByCanonicalUris(ImmutableSet.of("URI"))).thenReturn(ResolvedContent.builder().build());
        mergingContentWriter.write(extracted);

        verify(contentWriter).createOrUpdate(extracted);
    }

}