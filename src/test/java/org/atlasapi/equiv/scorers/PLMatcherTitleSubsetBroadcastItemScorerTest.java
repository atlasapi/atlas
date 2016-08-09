package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.proposed.PLMatcherTitleSubsetBroadcastItemScorer;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.testing.StubContentResolver;

/**
 * Created by adam on 29/07/2016.
 */
public class PLMatcherTitleSubsetBroadcastItemScorerTest extends TitleSubsetBroadcastItemScorerTest {

    private static final ContentResolver resolver = new StubContentResolver();

    public PLMatcherTitleSubsetBroadcastItemScorerTest() {
        super(new PLMatcherTitleSubsetBroadcastItemScorer(resolver, Score.nullScore(), 80));
    }



}