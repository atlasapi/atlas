package org.atlasapi.remotesite.itunes.epf;

import java.io.File;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

@RunWith(JMock.class)
public class ItunesEpfUpdateTaskTest extends TestCase {
    
    private final Mockery context = new Mockery();
    private final ContentWriter writer = context.mock(ContentWriter.class);
    @SuppressWarnings("unchecked")
    private final Supplier<EpfDataSet> dataSupplier = context.mock(Supplier.class);
    private File parent;

    private static final String LINE_END = ((char)2)+"\n";
    
    @Before
    public void setUp() throws Exception {
        Joiner joiner = Joiner.on((char)1);
        
        parent = Files.createTempDir();
        
        Files.write(joiner.join(ImmutableList.of(
                "1320832802897","102225079","The Office","1","http://itunes.apple.com/artist/the-office/id102225079?uo=5","2" + LINE_END
        )), new File(parent,"artist"), Charsets.UTF_8);
        
        Files.write(joiner.join(ImmutableList.of(
                "1320832802897","102225079","102772946","1","1" + LINE_END
        )), new File(parent,"artist_collection"), Charsets.UTF_8);
        
        Files.write(joiner.join(ImmutableList.of(
                "1320832802897","102772946","The Office, Season 1","","","","","http://itunes.apple.com/tv-season/the-office-season-1/id102772946?uo=5",
                "http://a1311.phobos.apple.com/us/r1000/037/Features/c8/1b/65/dj.jxnmyfbk.227x170-99.jpg","2005 03 24","1971 05 27","","NBCUniversal","2005 NBC Universal","2005 NBC Universal","4","0","6" + LINE_END
        )), new File(parent,"collection"), Charsets.UTF_8);
        
        Files.write(joiner.join(ImmutableList.of(
                "1321437625956","102772946","102225077","2","1","" + LINE_END
        )), new File(parent,"collection_video"), Charsets.UTF_8);
        
        Files.write(joiner.join(ImmutableList.of("1321437625956","102225077","Diversity Day","","","0","The Office","The Office, Season 1","4",
                "http://itunes.apple.com/video/diversity-day/id102225077?uo=5","http://a708.phobos.apple.com/us/r1000/021/Music/c8/1b/65/mzi.qkeekydh.133x100-99.jpg",
                "2005 03 29","2005 12 02","NBCUniversal","NBC","NBCUniversal","1307519","2005 NBC Studios, Inc. and Universal Network Television LLC. All Rights Reserved.",
                "","Michael's offensive behavior prompts the company to sponsor a seminar on racial tolerance and diversity. Jim has trouble securing his biggest yearly commission.",
                "When a special consultant, Mr. Brown (guest star Larry Wilmore), arrives to teach a seminar on racial tolerance and diversity in the workplace, Michael (Steve Carell) implies that it was his idea while in reality his offensive behavior necessitated the training. When Mr. Brown has a staff member reenact one of Michael's past indiscretions, Michael, not satisfied with the consultant's workshop, decides to hold his own racial teach-in later that afternoon. Meanwhile, Jim (John Krasinski) is not having a good day after losing his biggest yearly commission to Dwight (Rainn Wilson). Jenna Fischer and B.J. Novak also star"
                ,"R1101"+LINE_END
        )), new File(parent,"video"), Charsets.UTF_8);
        
        Files.write(Joiner.on("\t").join(ImmutableList.of(
                "1453888800445","102225077","1.89","GBP","143444","1.89","","","","","","1.89"
        )), new File(parent,"video_price"), Charsets.UTF_8);
    }

    @Test
    public void testUpdaterWritesBrandsFromArtistsFile() {
        
        ItunesEpfUpdateTask task = new ItunesEpfUpdateTask(dataSupplier, writer, new NullAdapterLog());
        
        context.checking(new Expectations(){{
            one(dataSupplier).get();will(returnValue(new EpfDataSet(parent)));
            one(writer).createOrUpdate(with(brand("http://itunes.apple.com/artist/id102225079")));
            one(writer).createOrUpdate(with(series("http://itunes.apple.com/tv-season/id102772946")));
            one(writer).createOrUpdate(with(item("http://itunes.apple.com/video/id102225077")));
        }});
        
        task.run();
        
    }
    
    private Matcher<Brand> brand(final String uri) {
        return new TypeSafeMatcher<Brand>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendValue("Brand with " + uri);
            }

            @Override
            public boolean matchesSafely(Brand container) {
                return container.getCanonicalUri().equals(uri);
            }
        };
    }
    
    private Matcher<Series> series(final String uri) {
        return new TypeSafeMatcher<Series>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendValue("Series with " + uri);
            }

            @Override
            public boolean matchesSafely(Series container) {
                return container.getCanonicalUri().equals(uri);
            }
        };
    }
    
    
    private Matcher<Item> item(final String uri) {
        return new TypeSafeMatcher<Item>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendValue("Series with " + uri);
            }

            @Override
            public boolean matchesSafely(Item container) {
                return container.getCanonicalUri().equals(uri);
            }
        };
    }
}
