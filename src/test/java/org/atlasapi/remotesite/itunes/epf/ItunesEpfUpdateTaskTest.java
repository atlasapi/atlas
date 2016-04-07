package org.atlasapi.remotesite.itunes.epf;

import java.io.File;
import java.util.Set;

import com.google.common.collect.Sets;
import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.atlasapi.remotesite.util.OldContentDeactivator;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ItunesEpfUpdateTaskTest extends TestCase {

    @Mock
    private ContentWriter writer;
    @Mock
    private Supplier<EpfDataSet> dataSupplier;
    @Mock
    private OldContentDeactivator deactivator;
    @Captor
    private ArgumentCaptor<Container> containerArgumentCaptor;
    @Captor
    private ArgumentCaptor<Episode> episodeArgumentCaptor;
    @Captor
    private ArgumentCaptor<Set<String>> urisArgumentCaptor;
    @Captor
    private ArgumentCaptor<Publisher> publisherArgumentCaptor;
    @Captor
    private ArgumentCaptor<Integer> integerArgumentCaptor;
    private File parent;

    private static final String LINE_END = ((char) 2) + "\n";

    @Before
    public void setUp() throws Exception {
        Joiner joiner = Joiner.on((char) 1);

        parent = Files.createTempDir();

        Files.write(joiner.join(ImmutableList.of(
                "1320832802897",
                "102225079",
                "The Office",
                "1",
                "http://itunes.apple.com/artist/the-office/id102225079?uo=5",
                "2" + LINE_END
        )), new File(parent, "artist"), Charsets.UTF_8);

        Files.write(joiner.join(ImmutableList.of(
                "1320832802897", "102225079", "102772946", "1", "1" + LINE_END
        )), new File(parent, "artist_collection"), Charsets.UTF_8);

        Files.write(joiner.join(ImmutableList.of(
                "1320832802897",
                "102772946",
                "The Office, Season 1",
                "",
                "",
                "",
                "",
                "http://itunes.apple.com/tv-season/the-office-season-1/id102772946?uo=5",
                "http://a1311.phobos.apple.com/us/r1000/037/Features/c8/1b/65/dj.jxnmyfbk.227x170-99.jpg",
                "2005 03 24",
                "1971 05 27",
                "",
                "NBCUniversal",
                "2005 NBC Universal",
                "2005 NBC Universal",
                "4",
                "0",
                "6" + LINE_END
        )), new File(parent, "collection"), Charsets.UTF_8);

        Files.write(joiner.join(ImmutableList.of(
                "1321437625956", "102772946", "102225077", "2", "1", "" + LINE_END
        )), new File(parent, "collection_video"), Charsets.UTF_8);

        Files.write(joiner.join(ImmutableList.of("1321437625956",
                "102225077",
                "Diversity Day",
                "",
                "",
                "0",
                "The Office",
                "The Office, Season 1",
                "4",
                "http://itunes.apple.com/video/diversity-day/id102225077?uo=5",
                "http://a708.phobos.apple.com/us/r1000/021/Music/c8/1b/65/mzi.qkeekydh.133x100-99.jpg",
                "2005 03 29",
                "2005 12 02",
                "NBCUniversal",
                "NBC",
                "NBCUniversal",
                "1307519",
                "2005 NBC Studios, Inc. and Universal Network Television LLC. All Rights Reserved.",
                "",
                "Michael's offensive behavior prompts the company to sponsor a seminar on racial tolerance and diversity. Jim has trouble securing his biggest yearly commission.",
                "When a special consultant, Mr. Brown (guest star Larry Wilmore), arrives to teach a seminar on racial tolerance and diversity in the workplace, Michael (Steve Carell) implies that it was his idea while in reality his offensive behavior necessitated the training. When Mr. Brown has a staff member reenact one of Michael's past indiscretions, Michael, not satisfied with the consultant's workshop, decides to hold his own racial teach-in later that afternoon. Meanwhile, Jim (John Krasinski) is not having a good day after losing his biggest yearly commission to Dwight (Rainn Wilson). Jenna Fischer and B.J. Novak also star"
                ,
                "R1101" + LINE_END
        )), new File(parent, "video"), Charsets.UTF_8);

        Files.write(joiner.join(ImmutableList.of(
                "1453888800445",
                "102225077",
                "1.89",
                "GBP",
                "143444",
                "",
                "1.89",
                "",
                "",
                "",
                "",
                "1.89" + LINE_END
        )), new File(parent, "video_price"), Charsets.UTF_8);

        Files.write(joiner.join(ImmutableList.of(
                "1453888800445", "143444", "GBR", "UK" + LINE_END
        )), new File(parent, "storefront"), Charsets.UTF_8);
    }

    @Test
    public void testIngestsAndDeactivatesNotIngested() {

        ItunesEpfUpdateTask task = new ItunesEpfUpdateTask(
                dataSupplier,
                deactivator,
                writer,
                new NullAdapterLog()
        );
        when(dataSupplier.get()).thenReturn(new EpfDataSet(parent));
        task.run();

        verify(writer, times(2)).createOrUpdate(containerArgumentCaptor.capture());
        verify(writer, times(1)).createOrUpdate(episodeArgumentCaptor.capture());
        verify(deactivator).deactivateOldContent(
                publisherArgumentCaptor.capture(),
                urisArgumentCaptor.capture(),
                integerArgumentCaptor.capture()
        );


        for (Container container : containerArgumentCaptor.getAllValues()) {
            if (container instanceof Brand) {
                assertThat(container.getCanonicalUri(), is("http://itunes.apple.com/artist/id102225079"));
            } else {
                assertThat(container.getCanonicalUri(), is("http://itunes.apple.com/tv-season/id102772946"));
            }
        }
        assertThat(episodeArgumentCaptor.getValue().getCanonicalUri(), is("http://itunes.apple.com/video/id102225077"));
        assertThat(publisherArgumentCaptor.getValue().key(), is(Publisher.ITUNES.key()));
        Set<String> uris = Sets.newHashSet("http://itunes.apple.com/artist/id102225079",
                "http://itunes.apple.com/tv-season/id102772946",
                "http://itunes.apple.com/video/id102225077");
        assertThat(urisArgumentCaptor.getValue(), is(uris));

    }
}
