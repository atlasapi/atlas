package org.atlasapi.remotesite.youview;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import nu.xom.Element;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.testing.ComplexBroadcastTestDataBuilder;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.media.entity.testing.VersionTestDataBuilder;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

@RunWith( MockitoJUnitRunner.class )
public class DefaultYouViewElementProcessorTest {

    private static final String ALIAS_NAMESPACE = "namespace";
    private static final String ALIAS_VALUE = "12345";
    private @Mock ContentResolver contentResolver;
    private @Mock ContentWriter contentWriter;
    private @Mock LookupEntryStore lookupEntryStore;
    private @Mock YouViewContentExtractor extractor;
    
    private DefaultYouViewElementProcessor elementProcessor;
    
    @Before
    public void setUp() {
        when(extractor.getScheduleEventAliasNamespace())
            .thenReturn(ALIAS_NAMESPACE);
        elementProcessor = new DefaultYouViewElementProcessor(extractor, contentResolver, contentWriter, lookupEntryStore);
    }
    
    @Test
    public void testStaleScheduleEventAliasIsRemoved() {
        
        Item extractedItem = ComplexItemTestDataBuilder
                        .complexItem()
                        .withUri("http://example.org/a")
                        .withVersions(
                                VersionTestDataBuilder
                                    .version()
                                    .withBroadcasts(
                                            ComplexBroadcastTestDataBuilder
                                                .broadcast()
                                                .build()
                                            )
                                    .build())
                        .withAliases(new Alias(ALIAS_NAMESPACE, ALIAS_VALUE))
                        .build();
        
        
        
        Item itemWithStaleAlias = ComplexItemTestDataBuilder
                .complexItem()
                .withUri("http://example.org/b")
                .withAliases(new Alias(ALIAS_NAMESPACE, ALIAS_VALUE))
                .build();
        
        Item resolvedItem = ComplexItemTestDataBuilder
                .complexItem()
                .withUri(extractedItem.getCanonicalUri())
                .withAliases(new Alias(ALIAS_NAMESPACE, ALIAS_VALUE))
                .build();
        
        when(extractor.extract(any(Publisher.class), any(Element.class)))
            .thenReturn(extractedItem);
        
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(resolvedItem.getCanonicalUri())))
            .thenReturn(new ResolvedContentBuilder().put(resolvedItem.getCanonicalUri(), resolvedItem).build());
        
        when(contentResolver.findByCanonicalUris(ImmutableSet.of(itemWithStaleAlias.getCanonicalUri())))
            .thenReturn(new ResolvedContentBuilder().put(itemWithStaleAlias.getCanonicalUri(), itemWithStaleAlias).build());
        
        when(lookupEntryStore.entriesForAliases(Optional.of(ALIAS_NAMESPACE), ImmutableSet.of(ALIAS_VALUE)))
            .thenReturn(ImmutableSet.of(LookupEntry.lookupEntryFrom(itemWithStaleAlias)));
        
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        
        elementProcessor.process(Publisher.METABROADCAST, new Element("test"));
        
        verify(contentWriter, times(2)).createOrUpdate(itemCaptor.capture());
        
        Item firstSavedItem = itemCaptor.getAllValues().get(0);
        assertEquals(itemWithStaleAlias.getCanonicalUri(), firstSavedItem.getCanonicalUri());
        assertEquals(0, firstSavedItem.getAliases().size());
    }
}
