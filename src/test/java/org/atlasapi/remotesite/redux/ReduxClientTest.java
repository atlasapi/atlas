package org.atlasapi.remotesite.redux;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.atlasapi.remotesite.redux.model.BaseReduxProgramme;
import org.atlasapi.remotesite.redux.model.FullReduxProgramme;
import org.atlasapi.remotesite.redux.model.PaginatedBaseProgrammes;
import org.atlasapi.remotesite.redux.model.ReduxMedia;

import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.query.Selection;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostSpecifier;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class ReduxClientTest {

	public static final String exampleDiskref = "5864860735858853248";
	public static final String TEST_REDUX_HOST = "devapi.bbcredux.com";
	
	private HttpBackedReduxClient reduxClient;
	
	@Before
	public void setup() throws ParseException {
		reduxClient = HttpBackedReduxClient.reduxClientForHost(HostSpecifier.from(TEST_REDUX_HOST)).build();
	}
	
	@Test
	public void testCanGetLatest() throws HttpException, Exception {
		PaginatedBaseProgrammes pbp;
		try {
			pbp = reduxClient.latest(Selection.ALL);
		} catch (ExecutionException | HttpException e) {
			System.out.print("WARNING: Failed to get a connection to redux. Test was skipped.\n");
			return;
		}
		int first = pbp.getFirst();
		int last = pbp.getLast();
		assertThat(first, lessThan(last));
		BaseReduxProgramme programme = pbp.getResults().get(0);
		assertNotNull(programme.getDiskref());
		assertNotNull(programme.getTitle());
	}
	
	@Test
    public void testCanGetLatestForChannel() throws HttpException, Exception {
        PaginatedBaseProgrammes pbp;
		try {
			pbp = reduxClient.latest(Selection.ALL, ImmutableSet.of("bbcone"));
		} catch (ExecutionException | HttpException e) {
			System.out.print("WARNING: Failed to get a connection to redux. Test was skipped.\n");
			return;
		}
        int first = pbp.getFirst();
        int last = pbp.getLast();
        assertThat(first, lessThan(last));
        BaseReduxProgramme programme = pbp.getResults().get(0);
        assertNotNull(programme.getDiskref());
        assertNotNull(programme.getTitle());
    }

	@Test
	@Ignore
	public void testCanGetProgramme() throws HttpException, Exception {
		FullReduxProgramme programme = reduxClient.programmeFor(exampleDiskref);
		assertThat(programme.getTitle(), is("The Dare Devil"));
		Map<String, ReduxMedia>mediaMap = programme.getMedia();
		assertNotNull("Media map should not be null", mediaMap);
		assertThat(mediaMap.size(), greaterThan(0));
	}
	
	@Test
	public void testCachesMediaTypes() throws HttpException, Exception {
		Map<String, ReduxMedia> mediaMap;
		try {
			mediaMap = reduxClient.getCachedMedia("radio");
		} catch (ExecutionException e) {
			System.out.print("WARNING: Failed to get a connection to redux. Test was skipped.\n");
			return;
		}
		assertNotNull("Media map should not be null", mediaMap);
		assertThat(mediaMap.size(), greaterThan(0));
		
		ReduxMedia media = mediaMap.get("mp3");
		assertNotNull(media);
		assertThat(media.getKind(), is("audio"));
		assertThat(media.getTitle(), is("MP3 audio"));
	}
	
	@Test
	@Ignore
	public void testGetsProgrammesForDay() throws HttpException, Exception {
	    List<BaseReduxProgramme> programmes = reduxClient.programmesForDay(new LocalDate());
	    
	    assertNotNull(programmes);
	    assertThat(programmes.size(), greaterThan(0));
	}
}
