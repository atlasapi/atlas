package org.atlasapi.equiv.update.tasks;

import static org.atlasapi.equiv.update.tasks.ContentEquivalenceUpdateTask.SAVE_EVERY_BLOCK_SIZE;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.C4;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.listing.SelectedContentLister;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.util.AlwaysBlockingQueue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

@RunWith(MockitoJUnitRunner.class)
public class ContentEquivalenceUpdateTaskTest extends TestCase {

    @SuppressWarnings("unchecked") 
    private final EquivalenceUpdater<Content> updater = mock(EquivalenceUpdater.class);
    private final ScheduleTaskProgressStore progressStore = mock(ScheduleTaskProgressStore.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    @Mock private OwlTelescopeReporter telescope = mock(OwlTelescopeReporter.class);

    private final SelectedContentLister listerForContent(final Multimap<Publisher, Content> contents) {
        return new SelectedContentLister() {

            @Override
            public List<Content> listContent(ContentListingCriteria criteria, boolean b) {
                Iterator<Content> iterator = listContent(criteria);
                List<Content> allContent = new ArrayList<>();
                iterator.forEachRemaining(allContent::add);
                return allContent;
            }

            @Override
            public Iterator<Content> listContent(ContentListingCriteria criteria) {
                return Iterators.concat(Iterators.transform(criteria.getPublishers().iterator(),
                        input -> contents.get(input).iterator()
                ));
            }
        };
    }

    public void testCallUpdateOnContent(ExecutorService executor) {
        
        Item paItemOne = new Item("pa1", "pa1c", Publisher.PA);
        Item bbcItemOne = new Item("bbc1", "bbc1c", Publisher.BBC);
        Item bbcItemTwo = new Item("bbc2", "bbc2c", Publisher.BBC);
        Item bbcItemThree = new Item("bbc3", "bbc3c", Publisher.BBC);
        Item c4ItemOne = new Item("c41", "c41c", Publisher.C4);
        
        Brand paBrand = new Brand("paBrand", "paBrand", Publisher.PA);
        Episode paEp = new Episode("episode", "episode", Publisher.PA);
        paBrand.setChildRefs(ImmutableList.of(paEp.childRef()));

        SelectedContentLister contentLister = listerForContent(ImmutableMultimap.<Publisher,Content>builder()
            .putAll(PA, paItemOne, paBrand)
            .putAll(BBC, bbcItemOne, bbcItemTwo, bbcItemThree)
            .putAll(C4, c4ItemOne)
        .build());
        
        String taskName = "pressassociation.com-bbc.co.uk-channel4.com-equivalence";
        when(progressStore.progressForTask(taskName))
            .thenReturn(ContentListingProgress.START);
        
        when(contentResolver.findByCanonicalUris(argThat(hasItem("episode"))))
            .thenReturn(ResolvedContent.builder().put(paEp.getCanonicalUri(), paEp).build());
        
        new ContentEquivalenceUpdateTask(contentLister, contentResolver, executor, progressStore, updater, ImmutableSet.of()).forPublishers(PA, BBC, C4).run();
        
        verify(updater).updateEquivalences(eq(paItemOne), any(OwlTelescopeReporter.class));
        verify(updater, times(2)).updateEquivalences(eq(paBrand), any(OwlTelescopeReporter.class));
        verify(updater).updateEquivalences(eq(paEp), any(OwlTelescopeReporter.class));
        verify(updater).updateEquivalences(eq(bbcItemOne), any(OwlTelescopeReporter.class));
        verify(updater).updateEquivalences(eq(bbcItemTwo), any(OwlTelescopeReporter.class));
        verify(updater).updateEquivalences(eq(bbcItemThree), any(OwlTelescopeReporter.class));
        verify(updater).updateEquivalences(eq(c4ItemOne), any(OwlTelescopeReporter.class));
        verify(progressStore).storeProgress(taskName, ContentListingProgress.START);
    }

    @Test
    public void testCallUpdateOnContentMultithreaded() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                3, 3,
                60, TimeUnit.SECONDS,
                new AlwaysBlockingQueue<>(SAVE_EVERY_BLOCK_SIZE)
        );
        testCallUpdateOnContent(executor);
    }

    @Test
    public void testCallUpdateOnContentOnMainThread() {
        testCallUpdateOnContent(null);
    }
}
