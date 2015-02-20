package org.atlasapi.output;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.simple.ContentQueryResult;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.output.simple.ChannelGroupSummarySimplifier;
import org.atlasapi.output.simple.ChannelSimplifier;
import org.atlasapi.output.simple.ContainerModelSimplifier;
import org.atlasapi.output.simple.EventRefModelSimplifier;
import org.atlasapi.output.simple.ImageSimplifier;
import org.atlasapi.output.simple.ItemModelSimplifier;
import org.atlasapi.output.simple.PersonModelSimplifier;
import org.atlasapi.output.simple.ProductModelSimplifier;
import org.atlasapi.output.simple.PublisherSimplifier;
import org.atlasapi.output.simple.TopicModelSimplifier;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.persistence.output.RecentlyBroadcastChildrenResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

@RunWith(MockitoJUnitRunner.class)
public class FullToSimpleModelTranslatorTest {
    
    private final ImageSimplifier imageSimplifier = new ImageSimplifier();
    private final ContentGroupResolver contentGroupResolver = mock(ContentGroupResolver.class);
    private final TopicQueryResolver topicResolver = mock(TopicQueryResolver.class);
    private final SegmentResolver segmentResolver = mock(SegmentResolver.class);
    private final AvailableItemsResolver availableChildren = mock(AvailableItemsResolver.class);
    private final UpcomingItemsResolver upcomingChildren = mock(UpcomingItemsResolver.class);
    private final RecentlyBroadcastChildrenResolver recentChildren = mock(RecentlyBroadcastChildrenResolver.class);
    private @SuppressWarnings("unchecked") final AtlasModelWriter<ContentQueryResult> xmlOutputter = mock(AtlasModelWriter.class);
    private final ContainerSummaryResolver containerSummaryResolver = mock(ContainerSummaryResolver.class);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final PeopleQueryResolver peopleResolver = mock(PeopleQueryResolver.class);
    
    private final TopicModelSimplifier topicSimplifier = new TopicModelSimplifier("localhostName");

    private ProductModelSimplifier productSimplifier = new ProductModelSimplifier("localhostName");
    private ProductResolver productResolver = mock(ProductResolver.class); 
    private final UpcomingItemsResolver upcomingResolver = mock(UpcomingItemsResolver.class);
    private final AvailableItemsResolver availableResolver = mock(AvailableItemsResolver.class);
    private final PersonModelSimplifier personSimplifier = mock(PersonModelSimplifier.class);
    private EventRefModelSimplifier eventSimplifier = mock(EventRefModelSimplifier.class);

    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelSimplifier channelSimplifier = new ChannelSimplifier(codec, codec, Mockito.mock(ChannelResolver.class), new PublisherSimplifier(), new ImageSimplifier(), new ChannelGroupSummarySimplifier(codec, Mockito.mock(ChannelGroupResolver.class)), Mockito.mock(ChannelGroupResolver.class));
    
    private final ItemModelSimplifier itemSimplifier = new ItemModelSimplifier(
            "localhostName", 
            contentGroupResolver, 
            topicResolver, 
            productResolver, 
            segmentResolver, 
            containerSummaryResolver, 
            channelResolver, 
            new SubstitutionTableNumberCodec(), 
            new SubstitutionTableNumberCodec(), 
            imageSimplifier,
            peopleResolver,
            upcomingResolver,
            availableResolver,
            null,
            null,
            null,
            channelSimplifier,
            null,
            null,
            eventSimplifier 
    );
    private final SimpleContentModelWriter translator = new SimpleContentModelWriter(
            xmlOutputter, 
            itemSimplifier, 
            new ContainerModelSimplifier(
                    itemSimplifier, 
                    "localhostName", 
                    contentGroupResolver, 
                    topicResolver, 
                    availableChildren, 
                    upcomingChildren, 
                    productResolver, 
                    recentChildren, 
                    imageSimplifier,
                    peopleResolver,
                    containerSummaryResolver,
                    eventSimplifier
            ), 
            topicSimplifier, 
            productSimplifier, 
            imageSimplifier,
            personSimplifier
    );
    
	private StubHttpServletRequest request;
	private StubHttpServletResponse response;

	@Before
	public void setUp() throws Exception {
		this.request = new StubHttpServletRequest();
		this.response = new StubHttpServletResponse();
	}
	
	@Test
	public void testTranslatesItemsInFullModel() throws Exception {
		
		Set<Identified> graph = Sets.newHashSet();
		graph.add(new Episode());
		
        translator.writeTo(request, response, QueryResult.of(graph), ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        verify(xmlOutputter).writeTo(eq(request), eq(response), argThat(simpleGraph()), eq(ImmutableSet.<Annotation>of()), eq(ApplicationConfiguration.DEFAULT_CONFIGURATION));
	}

	protected Matcher<ContentQueryResult> simpleGraph() {
		return new TypeSafeMatcher<ContentQueryResult> () {

			@Override
			public boolean matchesSafely(ContentQueryResult result) {
				if (result.getContents().size() != 1) { return false; }
				Object bean = Iterables.getOnlyElement(result.getContents());
				if (!(bean instanceof Item)) { return false; }
				return true;
			}

			public void describeTo(Description description) {
				// TODO Auto-generated method stub
			}};
	}
	
}
