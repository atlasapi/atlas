package org.atlasapi.equiv.query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.query.Selection;
import junit.framework.TestCase;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergeOnOutputQueryExecutorTest extends TestCase {
	
	private final Brand brand1 = new Brand("1", "c:1", Publisher.BBC);
	private final Brand brand2 = new Brand("2", "c:2", Publisher.BBC);
	private final Brand brand3 = new Brand("3", "c:3", Publisher.YOUTUBE);

	private final Episode item1 = new Episode("i1", "c:i1", Publisher.BBC);
	private final Episode item2 = new Episode("i2", "c:i2", Publisher.YOUTUBE);

	private final Clip clip1 = new Clip("c1", "c:c1", Publisher.YOUTUBE);
	
	private final Film film1 = new Film("f1", "f:f1", Publisher.PA);
	private final Film film2 = new Film("f2", "f:f2", Publisher.RADIO_TIMES);
	
	private final Actor actor = new Actor("a1", "a:a1", Publisher.RADIO_TIMES).withName("John Smith").withCharacter("Smith John");

	private Application mergeEpisodesApp = mock(Application.class);
	private Application mergeWithMissingPublisherApp = mock(Application.class);
	private Application mergePeopleApp = mock(Application.class);

	@Override
	protected void setUp() throws Exception {
	    item1.setContainer(brand1);
	    item2.setContainer(brand3);
	    
		brand3.addEquivalentTo(brand1);
		item1.addEquivalentTo(item2);
		item2.addClip(clip1);
		
		brand1.setId(1l);
		brand2.setId(2l);
		brand3.setId(3l);
		item1.setId(4l);
		item2.setId(5l);
		clip1.setId(6l);
		
		film1.setId(7l);
		film2.setId(8l);
		film2.addPerson(actor);
		film1.addEquivalentTo(film2);
		film2.addEquivalentTo(film1);

		when(mergeEpisodesApp.getConfiguration())
                .thenReturn(configurationWithReads(Publisher.BBC, Publisher.YOUTUBE));
		when(mergeWithMissingPublisherApp.getConfiguration())
                .thenReturn(configurationWithReads(Publisher.BBC));
		when(mergePeopleApp.getConfiguration())
                .thenReturn(configurationWithReads(Publisher.PA, Publisher.RADIO_TIMES));
	}
	
	public void testMergingEpisodes() throws Exception {

		MergeOnOutputQueryExecutor merger = new MergeOnOutputQueryExecutor(delegate(item1, item2));

		ContentQuery query = new ContentQuery(ImmutableList.of(), Selection.ALL, mergeEpisodesApp);
		Map<String, List<Identified>> merged = ImmutableMap.copyOf(merger.executeUriQuery(ImmutableList.of(item1.getCanonicalUri()), query));
		
		assertEquals(ImmutableList.of(item1), merged.get(item1.getCanonicalUri()));
		assertEquals(ImmutableList.of(clip1), ((Episode)Iterables.getOnlyElement(merged.get(item1.getCanonicalUri()))).getClips());
	}
	
	public void testMergingWithExplicitPrecedenceMissingNewPublisher() throws Exception {
	    MergeOnOutputQueryExecutor merger = new MergeOnOutputQueryExecutor(delegate(item1, item2));

	    // Let's not specify a precedence for YouTube, but content will be returned for YouTube
        ContentQuery query = new ContentQuery(ImmutableList.of(), Selection.ALL, mergeWithMissingPublisherApp);
        Map<String, List<Identified>> merged = ImmutableMap.copyOf(merger.executeUriQuery(ImmutableList.of(item1.getCanonicalUri()), query));
        
        assertEquals(ImmutableList.of(item1), merged.get(item1.getCanonicalUri()));
        assertEquals(ImmutableList.of(clip1), ((Episode)Iterables.getOnlyElement(merged.get(item1.getCanonicalUri()))).getClips());
	}
	
    public void testMergingPeople() throws Exception {
        MergeOnOutputQueryExecutor merger = new MergeOnOutputQueryExecutor(delegate(film1, film2));

        ContentQuery query = new ContentQuery(ImmutableList.of(), Selection.ALL, mergePeopleApp);
        Map<String, List<Identified>> merged = ImmutableMap.copyOf(merger.executeUriQuery(ImmutableList.of(film1.getCanonicalUri()), query));
        
        assertEquals(ImmutableList.of(film1), merged.get(film1.getCanonicalUri()));
        assertEquals(ImmutableList.of(actor), ((Film)Iterables.getOnlyElement(merged.get(film1.getCanonicalUri()))).getPeople());
    }

	private KnownTypeQueryExecutor delegate(final Content... respondWith) {
		return new KnownTypeQueryExecutor() {

			@Override
			public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris, ContentQuery query) {
				return ImmutableMap.<String, List<Identified>>of(respondWith[0].getCanonicalUri(), ImmutableList.<Identified>copyOf(respondWith));
			}

            @Override
            public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids, ContentQuery query) {
                return ImmutableMap.<String, List<Identified>>of(respondWith[0].getCanonicalUri(), ImmutableList.<Identified>copyOf(respondWith));
            }

            @Override
            public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace, Iterable<String> values,
                    ContentQuery query) {
                return ImmutableMap.<String, List<Identified>>of(respondWith[0].getCanonicalUri(), ImmutableList.<Identified>copyOf(respondWith));
            }

            @Override
            public Map<String, List<Identified>> executePublisherQuery(
                    Iterable<Publisher> publishers, ContentQuery query) {
                throw new UnsupportedOperationException();
            }
		};
	}

	private ApplicationConfiguration configurationWithReads(Publisher... publishers) {
	    return ApplicationConfiguration.builder()
                .withPrecedence(Arrays.asList(publishers))
                .withEnabledWriteSources(ImmutableList.of())
                .build();
    }
}
