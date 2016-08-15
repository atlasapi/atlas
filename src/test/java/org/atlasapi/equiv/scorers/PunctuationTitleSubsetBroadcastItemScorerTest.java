package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.proposedbroadcast.PunctuationTitleSubsetBroadcastItemScorer;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.testing.StubContentResolver;

/**
 * Created by adam on 29/07/2016.
 */
public class PunctuationTitleSubsetBroadcastItemScorerTest extends TitleSubsetBroadcastItemScorerTest {

    private static final ContentResolver resolver = new StubContentResolver();

    public PunctuationTitleSubsetBroadcastItemScorerTest() {
        super(new PunctuationTitleSubsetBroadcastItemScorer(resolver, Score.nullScore(), 80));
    }



}