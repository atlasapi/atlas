package org.atlasapi.query.content;

import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.BBC;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.BLIP;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.BTFEATURED;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.C4;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.DM;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.FB;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.HULU;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.ITV;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.TED;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.VIM;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.WIKI;
import static org.atlasapi.query.content.PerPublisherCurieExpander.CurieAlgorithm.YT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import junit.framework.TestCase;

import com.metabroadcast.common.base.Maybe;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class PerPublisherCurieExpanderTest extends TestCase {

	CurieExpander expander = new PerPublisherCurieExpander();
	
	public void testExpandsBbcCuries() throws Exception {
		
		assertThat(expander.expand("bbc:b006mk25"), is(Maybe.just("http://www.bbc.co.uk/programmes/b006mk25")));
		assertThat(expander.expand("bbc:p006mk25"), is(Maybe.just("http://www.bbc.co.uk/programmes/p006mk25")));

		assertThat(expander.expand("bbc:atoz_a"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/atoz/a/list")));
		assertThat(expander.expand("bbc:atoz_0-9"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/atoz/0-9/list")));
		assertThat(expander.expand("bbc:bbc_one"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/bbc_one/list")));

		assertThat(expander.expand("bbc:highlights_tv"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/highlights/tv")));
		assertThat(expander.expand("bbc:popular_tv"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/popular/tv")));

		assertThat(expander.expand("bbc:highlights_radio"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/highlights/radio")));
		assertThat(expander.expand("bbc:popular_radio"), is(Maybe.just("http://feeds.bbc.co.uk/iplayer/popular/radio")));
	
	}
	
	public void testExpandsC4Curies() throws Exception {
		assertThat(expander.expand("c4:grand-designs"), is(Maybe.just("http://www.channel4.com/programmes/grand-designs")));
		assertThat(expander.expand("c4:grand-designs_2921795"), is(Maybe.just("http://www.channel4.com/programmes/grand-designs/4od#2921795")));

		assertThat(expander.expand("c4:atoz_a"), is(Maybe.just("http://www.channel4.com/programmes/atoz/a")));
		assertThat(expander.expand("c4:atoz_0-9"), is(Maybe.just("http://www.channel4.com/programmes/atoz/0-9")));

		//assertThat(expander.expand("c4:highlights"), is(Maybe.just("http://www.channel4.com/programmes/4od/highlights")));
		//assertThat(expander.expand("c4:most-popular"), is(Maybe.just("http://www.channel4.com/programmes/4od/most-popular")));

		assertThat(expander.expand("c4:ramsays-kitchen-nightmares-series-3"), is(Maybe.just("http://www.channel4.com/programmes/ramsays-kitchen-nightmares/episode-guide/series-3")));
		assertThat(expander.expand("c4:ramsays-kitchen-nightmares-series-3-episode-1"), is(Maybe.just("http://www.channel4.com/programmes/ramsays-kitchen-nightmares/episode-guide/series-3/episode-1")));
		
		assertThat(expander.expand("c4:series-of-tests"), is(Maybe.just("http://www.channel4.com/programmes/series-of-tests")));
		assertThat(expander.expand("c4:series-of-tests-series-1"), is(Maybe.just("http://www.channel4.com/programmes/series-of-tests/episode-guide/series-1")));
		assertThat(expander.expand("c4:series-of-tests-series-1-episode-3"), is(Maybe.just("http://www.channel4.com/programmes/series-of-tests/episode-guide/series-1/episode-3")));

		assertThat(expander.expand("c4:test-of-series"), is(Maybe.just("http://www.channel4.com/programmes/test-of-series")));
		assertThat(expander.expand("c4:test-of-series-series-1"), is(Maybe.just("http://www.channel4.com/programmes/test-of-series/episode-guide/series-1")));
		assertThat(expander.expand("c4:test-of-series-series-1-episode-3"), is(Maybe.just("http://www.channel4.com/programmes/test-of-series/episode-guide/series-1/episode-3")));
		
		assertThat(expander.expand("c4:32023-001"), is(Maybe.just("http://www.channel4.com/programmes/32023/001")));
	}
	
	public void testProducesC4Curies() throws Exception {
		assertThat(C4.compact("http://www.channel4.com/programmes/grand-designs"), is("c4:grand-designs"));
		assertThat(C4.compact("http://www.channel4.com/programmes/grand-designs/4od#2921795"), is("c4:grand-designs_2921795"));

		assertThat(C4.compact("http://www.channel4.com/programmes/atoz/a"), is("c4:atoz_a"));
		assertThat(C4.compact("http://www.channel4.com/programmes/atoz/0-9"), is("c4:atoz_0-9"));

		assertThat(C4.compact("http://www.channel4.com/programmes/ramsays-kitchen-nightmares/episode-guide/series-3"), is("c4:ramsays-kitchen-nightmares-series-3"));

		assertThat(C4.compact("http://www.channel4.com/programmes/ramsays-kitchen-nightmares/episode-guide/series-3/episode-1"), is("c4:ramsays-kitchen-nightmares-series-3-episode-1"));
		
		assertThat(C4.compact("http://www.channel4.com/programmes/32023/001"), is("c4:32023-001"));
	}
	
	public void testExpandsItvCuries() throws Exception {
		assertThat(expander.expand("itv:...Do%20the%20Funniest%20Things"), is(Maybe.just("http://www.itv.com/itvplayer/video/?Filter=...Do%20the%20Funniest%20Things")));
		assertThat(expander.expand("itv:123"), is(Maybe.just("http://www.itv.com/itvplayer/video/?Filter=123")));
	}
	
	public void testProducesItvCuries() throws Exception {
		assertThat(ITV.compact("http://www.itv.com/itvplayer/video/?Filter=123"), is("itv:123"));
		assertThat(ITV.compact("http://www.itv.com/itvplayer/video/?Filter=...Do%20the%20Funniest%20Things"), is("itv:...Do%20the%20Funniest%20Things"));
	}
	
	public void testExpandsYoutubeCuries() throws Exception {
		
		assertThat(expander.expand("yt:4QQkMVddwx0"), is(Maybe.just("http://www.youtube.com/watch?v=4QQkMVddwx0")));
	}
	
	public void testProducesYoutubeCuries() throws Exception {
		
		assertThat(YT.compact("http://www.youtube.com/watch?v=4QQkMVddwx0"), is("yt:4QQkMVddwx0"));
	}
	
	public void testExpandsBlipTvCuries() throws Exception {
		
		assertThat(expander.expand("blip:12345"), is(Maybe.just("http://blip.tv/file/12345")));
	}
	
	public void testProducesBlipTvCuries() throws Exception {
		
		assertThat(BLIP.compact("http://blip.tv/file/12345"), is("blip:12345"));
	}
	
	public void testExpandsVimeoCuries() throws Exception {
		
		assertThat(expander.expand("vim:12345"), is(Maybe.just("http://vimeo.com/12345")));
	}
	
	public void testProducesVimeoCuries() throws Exception {
		
		assertThat(VIM.compact("http://vimeo.com/12345"), is("vim:12345"));
	}
	
	public void testExpandsDailyMotionCuries() throws Exception {
		
		assertThat(expander.expand("dm:xbqomc_dont-do-anything_fun"), is(Maybe.just("http://www.dailymotion.com/video/xbqomc_dont-do-anything_fun")));
	}
	
	public void testProducesDailyMotionCuries() throws Exception {
		
		assertThat(DM.compact("http://www.dailymotion.com/video/xbqomc_dont-do-anything_fun"), is("dm:xbqomc_dont-do-anything_fun"));
	}
	
	public void testExpandsHuluCuries() throws Exception {
		
		assertThat(expander.expand("hulu:78417"), is(Maybe.just("http://www.hulu.com/watch/78417")));
		assertThat(expander.expand("hulu:glee"), is(Maybe.just("http://www.hulu.com/glee")));
	}
	
	public void testProducesHuluCuries() throws Exception {
		
		assertThat(HULU.compact("http://www.hulu.com/watch/78417/the-daily-show-with-jon-stewart-wed-jun-17-2009"), is("hulu:78417"));
		assertThat(HULU.compact("http://www.hulu.com/glee"), is("hulu:glee"));
	}
	
	public void testExpandsTedCuries() throws Exception {
		
		assertThat(expander.expand("ted:elizabeth_gilbert_on_genius"), is(Maybe.just("http://www.ted.com/talks/elizabeth_gilbert_on_genius.html")));
	}
	
	public void testProducesTedCuries() throws Exception {
		
		assertThat(TED.compact("http://www.ted.com/talks/elizabeth_gilbert_on_genius.html"), is("ted:elizabeth_gilbert_on_genius"));
	}
	
	
	public void testExpandsFacebookCuries() throws Exception {
		
		assertThat(expander.expand("fb:101"), is(Maybe.just("http://graph.facebook.com/101")));
	}
	
	public void testProducesFacebookCuries() throws Exception {
		
		assertThat(FB.compact("http://graph.facebook.com/101"), is("fb:101"));
	}
	
	public void testExpandsWikipediaCuries() throws Exception {
		assertThat(expander.expand("wiki:en:foo"), is(Maybe.just("http://en.wikipedia.org/foo")));

		// handy shortcut
		assertThat(expander.expand("wiki:foo"), is(Maybe.just("http://en.wikipedia.org/foo")));
	}
	
	public void testProducesWikipediaCuries() throws Exception {
		assertThat(WIKI.compact("http://en.wikipedia.org/foo"), is("wiki:en:foo"));
	}
	
	public void testExpandsBTFeaturedContentCuries() throws Exception {
	    assertThat(expander.expand("btfeatured:943507"), is(Maybe.just("http://featured.bt.com/products/943507")));
	}
	
	public void testProducesBTFeaturedContentCuries() throws Exception {
	    assertThat(BTFEATURED.compact("http://featured.bt.com/products/943507"), is("btfeatured:943507"));
	}
}
