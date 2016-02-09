package org.atlasapi.remotesite.pa.archives;

import java.io.File;
import java.net.URISyntaxException;

import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.pa.PaProgrammeProcessor;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaArchivesUpdaterTest {

    @Mock
    private PaProgrammeDataStore store;
    @Mock
    private FileUploadResultStore resultStore;
    @Mock
    private ContentWriter writer;
    @Mock
    private AdapterLog log;
    @Mock
    private ContentResolver resolver;
    @Mock
    private ResolvedContent resolvedContent;
    private PaUpdatesProcessor paUpdatesProcessor;
    private PaCompleteArchivesUpdater updater;
    private File file;

    @Before
    public void setUp() throws URISyntaxException {
        file = new File(Resources.getResource(getClass(), "201601121258_1201_tvarchive.xml").toURI());
        when(resolvedContent.getFirstValue()).thenReturn(Maybe.<Identified>nothing());
        when(resolver.findByCanonicalUris(anyCollection())).thenReturn(resolvedContent);
        PaProgDataUpdatesProcessor progProcessor = new PaProgrammeProcessor(writer,resolver,log);
        paUpdatesProcessor = new PaUpdatesProcessor(progProcessor, writer);
        updater = new PaCompleteArchivesUpdater(store,resultStore,paUpdatesProcessor);
        when(store.localArchivesFiles(any(Predicate.class))).thenReturn(ImmutableList.of(file));
        when(store.copyForProcessing(file)).thenReturn(file);
    }

    @Test
    public void testRunTask() throws Exception {
        updater.runTask();
    }
}