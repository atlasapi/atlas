package org.atlasapi.equiv.generators;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbAliasEquivalenceGeneratorAndScorerTest {

    private static final Score SCORE_ON_MATCH = Score.valueOf(10.0);

    private static final String BBC_CMS_BCID_NAMESPACE = "gb:bbc:nitro:prod:version:pid";
    private static final String C4_CMS_BCID_NAMESPACE = "gb:channel4:prod:pmlsd:programmeId";
    private static final String ITV_CMS_BCID_NAMESPACE = "gb:itv:production:id";

    private static final String BG_BCID_NAMESPACE_FORMAT = "gb:barb:broadcastGroup:%d:bcid";
    private static final String BG_PARENT_VERSION_BCID_NAMESPACE_FORMAT = "gb:barb:broadcastGroup:%d:parentVersionBcid";
    private static final String OOBG_BCID_NAMESPACE_FORMAT = "gb:barb:originatingOwner:broadcastGroup:%d:bcid";

    private static final Set<Publisher> INCLUDED_PUBLISHERS = ImmutableSet.of(
            Publisher.BBC_NITRO,
            Publisher.BARB_TRANSMISSIONS
    );
    private static final boolean INCLUDE_UNPUBLISHED_CONTENT = false;

    private static final ImmutableMap<Integer, String> CMS_BCID_NAMESPACE_MAP = ImmutableMap.of(
            1, BBC_CMS_BCID_NAMESPACE,
            2, ITV_CMS_BCID_NAMESPACE,
            3, C4_CMS_BCID_NAMESPACE
    );

    // force equivalence
    private BarbAliasEquivalenceGeneratorAndScorer generator;
    private final ContentResolver resolver = mock(ContentResolver.class);
    private final MongoLookupEntryStore lookupEntryStore = mock(MongoLookupEntryStore.class);

    // alias equivalence
    private final ContentResolver aliasResolver = mock(ContentResolver.class);
    private final MongoLookupEntryStore aliasLookupEntryStore = mock(MongoLookupEntryStore.class);
    private BarbAliasEquivalenceGeneratorAndScorer<Content> aliasGenerator =
            new BarbAliasEquivalenceGeneratorAndScorer<>(
                    aliasLookupEntryStore,
                    aliasResolver,
                    INCLUDED_PUBLISHERS,
                    SCORE_ON_MATCH,
                    INCLUDE_UNPUBLISHED_CONTENT
            );

    Content aliasIdentified1;
    Content aliasIdentified2;

    ResultDescription desc;

    @Before
    public void setUp() {
        desc = new DefaultDescription();

    }

    private void setupAliasEquivalenceTests() {
        Set<Alias> aliasesForaliasIdentified1 = ImmutableSet.of(
                new Alias("namespaceOne", "someBcid"),
                new Alias("namespaceTwo", "someOtherBcid")
        );

        aliasIdentified1 = new Item();
        aliasIdentified1.setCanonicalUri("Uri for alias test");
        aliasIdentified1.setAliases(aliasesForaliasIdentified1);

        aliasIdentified2 = new Item();
        aliasIdentified2.setCanonicalUri("Another uri for alias test");
        aliasIdentified2.setAliases(ImmutableSet.of(
                new Alias("namespaceOne", "someBcid"),
                new Alias("namespaceThree", "someOtherOtherBcid")
        ));

        ResolvedContent aliasResolvedContent = new ResolvedContent.ResolvedContentBuilder()
                .put("Uri for alias test", aliasIdentified1)
                .build();

        DefaultScoredCandidates.Builder aliasEquivalents =
                DefaultScoredCandidates.fromSource("Barb Alias Matching");

        LookupEntry lookupEntry = new LookupEntry(
                "Uri for alias test",
                22L,
                new LookupRef("Uri for alias test", 23L, Publisher.BARB_TRANSMISSIONS, ContentCategory.CHILD_ITEM),
                ImmutableSet.of("Uri for alias test"),
                aliasesForaliasIdentified1,
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                DateTime.now(),
                DateTime.now(),
                true
        );

        aliasResolvedContent.getAllResolvedResults().forEach(
                aliasIdentified ->
                        aliasEquivalents.addEquivalent(aliasIdentified, Score.ONE));

        when(aliasResolver.findByCanonicalUris(Matchers.anyCollection()))
                .thenReturn(aliasResolvedContent);

        when(aliasLookupEntryStore.entriesForAliases(
                Optional.of("namespaceOne"),
                ImmutableSet.of("someBcid"),
                INCLUDE_UNPUBLISHED_CONTENT
        )).thenReturn(ImmutableSet.of(lookupEntry));
    }

    private void setupForceEquivalenceTests() {
        List<String> forceTargetUrisOne = new ArrayList<>();
        forceTargetUrisOne.add("http://txlogs.barb.co.uk/episode/00000000000226905561");
        forceTargetUrisOne.add("http://itv.com/episode/1_5576_0002");

        DefaultScoredCandidates.Builder equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        Content identified = new Item();
        identified.setCanonicalUri("http://txlogs.barb.co.uk/episode/00000000000226905561");
        Content identified1 = new Item();
        identified1.setCanonicalUri("http://itv.com/episode/1_5576_0002");

        ResolvedContent resolvedContent = new ResolvedContent.ResolvedContentBuilder()
                .put("nothing1", identified)
                .put("nothing2", identified1)
                .build();

        resolvedContent.getAllResolvedResults().forEach(i ->
                equivalents.addEquivalent(i, Score.ONE));

        when(resolver.findByCanonicalUris(Matchers.anyCollection())).thenReturn(resolvedContent);
    }

    //@Test TODO: create a new test now that the hardcoded content was removed
    public void generatorFindsHardcodedContent() {
        Content subject = new Item();
        subject.setCanonicalUri("http://cdmf.barb.co.uk/episode/219060");

        ScoredCandidates scoredCandidates = generator.generate(
                subject,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );

        Content identified = new Item();
        identified.setCanonicalUri("http://txlogs.barb.co.uk/episode/00000000000226905561");
        Content identified1 = new Item();
        identified1.setCanonicalUri("http://itv.com/episode/1_5576_0002");

        DefaultScoredCandidates.Builder equivalents =
                DefaultScoredCandidates.fromSource("Barb Alias");

        ResolvedContent resolvedContent = new ResolvedContent.ResolvedContentBuilder()
                .put("nothing1", identified)
                .put("nothing2", identified1)
                .build();

        resolvedContent.getAllResolvedResults().forEach(i -> equivalents.addEquivalent(
                i,
                Score.valueOf(1.0)
        ));

        assertEquals(scoredCandidates, equivalents.build());
    }

    @Test
    public void aliasGeneratorFindsByAlias() {
        setupAliasEquivalenceTests();

        ScoredCandidates scoredCandidates = aliasGenerator.generate(
                aliasIdentified2,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );

        System.out.println(desc.toString());
        System.out.println(scoredCandidates.toString());

        assertFalse(scoredCandidates.candidates().isEmpty());

        for (Object scoredCandidate : scoredCandidates.candidates().keySet()) {
            assertEquals(scoredCandidate, aliasIdentified1);
        }

    }

    private void setUpContentResolving(Collection<Content> contents) {
        for(Content content : contents) {
            when(aliasLookupEntryStore.entriesForAliases(
                    Optional.of(getAlias(content).getNamespace()),
                    ImmutableSet.of(getAlias(content).getValue()),
                    INCLUDE_UNPUBLISHED_CONTENT
            )).thenReturn(ImmutableList.of(lookupEntryForContent(content)));
        }

        Set<Set<Content>> powerSet = Sets.powerSet(ImmutableSet.copyOf(contents));

        for(Set<Content> subset : powerSet) {
            ResolvedContent.ResolvedContentBuilder resolvedContentBuilder = ResolvedContent.builder();
            int count = 0;
            for(Content content : subset) {
                count++;
                resolvedContentBuilder.put("q" + count, content);
            }
            when(aliasResolver.findByCanonicalUris(
                    subset.stream()
                            .map(Content::getCanonicalUri)
                            .collect(MoreCollectors.toImmutableSet())
            )).thenReturn(resolvedContentBuilder.build());
        }
    }

    private Item bgItemForBgid(int bgid, String bcid) {
        Alias bgAlias = new Alias(
                String.format(BG_BCID_NAMESPACE_FORMAT, bgid),
                bcid
        );
        Item bgItem = new Item();
        String bgUri = "bgUri" + bgid;
        bgItem.setCanonicalUri(bgUri);
        bgItem.setAliases(ImmutableSet.of(bgAlias));
        return bgItem;
    }

    private Item oobgItemForBgid(int bgid, String bcid) {
        Alias bgAlias = new Alias(
                String.format(OOBG_BCID_NAMESPACE_FORMAT, bgid),
                bcid
        );
        Item oobgItem = new Item();
        String bgUri = "oobgUri" + bgid;
        oobgItem.setCanonicalUri(bgUri);
        oobgItem.setAliases(ImmutableSet.of(bgAlias));
        return oobgItem;
    }

    private Item parentBcidItemForBgid(int bgid, String bcid) {
        Alias bgAlias = new Alias(
                String.format(BG_PARENT_VERSION_BCID_NAMESPACE_FORMAT, bgid),
                bcid
        );
        Item parentBcidItem = new Item();
        String bgUri = "parentBcidUri" + bgid;
        parentBcidItem.setCanonicalUri(bgUri);
        parentBcidItem.setAliases(ImmutableSet.of(bgAlias));
        return parentBcidItem;
    }

    private Item cmsItemForBgid(int bgid, String bcid) {
        Alias bgAlias = new Alias(
                CMS_BCID_NAMESPACE_MAP.get(bgid),
                bcid
        );
        Item cmsItem = new Item();
        String bgUri = "cmsUri" + bgid;
        cmsItem.setCanonicalUri(bgUri);
        cmsItem.setAliases(ImmutableSet.of(bgAlias));
        return cmsItem;
    }

    @Nullable
    private Alias getAlias(Content content) {
        return content.getAliases().stream()
                .findFirst()
                .orElse(null);
    }

    private LookupEntry lookupEntryForContent(Content content) {
        return new LookupEntry(
                content.getCanonicalUri(),
                0L,
                new LookupRef(
                        content.getCanonicalUri(),
                        0L,
                        content.getPublisher() != null ? content.getPublisher() : Publisher.BARB_TRANSMISSIONS,
                        ContentCategory.TOP_LEVEL_ITEM
                ),
                ImmutableSet.of(),
                content.getAliases(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                DateTime.now(),
                DateTime.now(),
                true
        );
    }

    @Test
    public void bgAliasMatchesAliases() {
        String bcid = "bcid";
        Item bgItem = bgItemForBgid(1, bcid);
        Item oobgItem = oobgItemForBgid(1, bcid);
        Item parentBcidItem = parentBcidItemForBgid(1, bcid);
        Item cmsItem = cmsItemForBgid(1, bcid);
        setUpContentResolving(ImmutableSet.of(bgItem, oobgItem, parentBcidItem, cmsItem));
        ScoredCandidates<Content> scoredCandidates = aliasGenerator.generate(
                bgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(!scoredCandidates.candidates().containsKey(bgItem));
        assertTrue(scoredCandidates.candidates().get(oobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(parentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(cmsItem) == SCORE_ON_MATCH);
    }

    @Test
    public void oobgAliasMatches() {
        String bcid = "bcid";
        Item bgItem = bgItemForBgid(1, bcid);
        Item oobgItem = oobgItemForBgid(1, bcid);
        Item parentBcidItem = parentBcidItemForBgid(1, bcid);
        Item cmsItem = cmsItemForBgid(1, bcid);
        setUpContentResolving(ImmutableSet.of(bgItem, oobgItem, parentBcidItem, cmsItem));
        ScoredCandidates<Content> scoredCandidates = aliasGenerator.generate(
                oobgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(bgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(oobgItem));
        assertTrue(scoredCandidates.candidates().get(parentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(cmsItem) == SCORE_ON_MATCH);
    }

    @Test
    public void parentVersionBcidAliasMatches() {
        String bcid = "bcid";
        Item bgItem = bgItemForBgid(1, bcid);
        Item oobgItem = oobgItemForBgid(1, bcid);
        Item parentBcidItem = parentBcidItemForBgid(1, bcid);
        Item cmsItem = cmsItemForBgid(1, bcid);
        setUpContentResolving(ImmutableSet.of(bgItem, oobgItem, parentBcidItem, cmsItem));
        ScoredCandidates<Content> scoredCandidates = aliasGenerator.generate(
                parentBcidItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(bgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(oobgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(parentBcidItem));
        assertTrue(scoredCandidates.candidates().get(cmsItem) == SCORE_ON_MATCH);
    }

    @Test
    public void cmsAliasMatches() {
        String bcid = "bcid";
        Item bgItem = bgItemForBgid(1, bcid);
        Item oobgItem = oobgItemForBgid(1, bcid);
        Item parentBcidItem = parentBcidItemForBgid(1, bcid);
        Item cmsItem = cmsItemForBgid(1, bcid);
        setUpContentResolving(ImmutableSet.of(bgItem, oobgItem, parentBcidItem, cmsItem));
        ScoredCandidates<Content> scoredCandidates = aliasGenerator.generate(
                cmsItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(bgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(oobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(parentBcidItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(cmsItem));
    }

    @Test
    public void stvAliasesMatchItv() {
        String bcid = "bcid";
        Item stvBgItem = bgItemForBgid(111, bcid);
        Item stvOobgItem = oobgItemForBgid(111, bcid);
        Item stvParentBcidItem = parentBcidItemForBgid(111, bcid);
        Item itvBgItem = bgItemForBgid(2, bcid);
        Item itvOobgItem = oobgItemForBgid(2, bcid);
        Item itvParentBcidItem = parentBcidItemForBgid(2, bcid);
        Item itvCmsItem = cmsItemForBgid(2, bcid);
        ScoredCandidates<Content> scoredCandidates;
        setUpContentResolving(ImmutableSet.of(
                stvBgItem,
                stvOobgItem,
                stvParentBcidItem,
                itvBgItem,
                itvOobgItem,
                itvParentBcidItem,
                itvCmsItem
                )
        );
        scoredCandidates = aliasGenerator.generate(
                stvOobgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(stvBgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(stvOobgItem));
        assertTrue(scoredCandidates.candidates().get(stvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvCmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                stvBgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(!scoredCandidates.candidates().containsKey(stvBgItem));
        assertTrue(scoredCandidates.candidates().get(stvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvCmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                stvParentBcidItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(stvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvOobgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(stvParentBcidItem));
        assertTrue(scoredCandidates.candidates().get(itvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvCmsItem) == SCORE_ON_MATCH);
    }

    @Test
    public void itvAliasesMatchStv() {
        String bcid = "bcid";
        Item stvBgItem = bgItemForBgid(111, bcid);
        Item stvOobgItem = oobgItemForBgid(111, bcid);
        Item stvParentBcidItem = parentBcidItemForBgid(111, bcid);
        Item itvBgItem = bgItemForBgid(2, bcid);
        Item itvOobgItem = oobgItemForBgid(2, bcid);
        Item itvParentBcidItem = parentBcidItemForBgid(2, bcid);
        Item itvCmsItem = cmsItemForBgid(2, bcid);
        ScoredCandidates<Content> scoredCandidates;
        setUpContentResolving(ImmutableSet.of(
                stvBgItem,
                stvOobgItem,
                stvParentBcidItem,
                itvBgItem,
                itvOobgItem,
                itvParentBcidItem,
                itvCmsItem
                )
        );
        scoredCandidates = aliasGenerator.generate(
                itvBgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(stvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(itvBgItem));
        assertTrue(scoredCandidates.candidates().get(itvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvCmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                itvOobgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(stvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvBgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(itvOobgItem));
        assertTrue(scoredCandidates.candidates().get(itvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvCmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                itvParentBcidItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(stvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvOobgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(itvParentBcidItem));
        assertTrue(scoredCandidates.candidates().get(itvCmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                itvCmsItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(stvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(stvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvBgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvOobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(itvParentBcidItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(itvCmsItem));
    }

    @Test
    public void c4AliasesMatchCmsWithoutBcidPrefix() {
        String bcid = "bcid";
        String c4Bcid = "C4:" + bcid;
        Item bgItem = bgItemForBgid(3, c4Bcid);
        Item oobgItem = oobgItemForBgid(3, c4Bcid);
        Item parentBcidItem = parentBcidItemForBgid(3, c4Bcid);
        Item cmsItem = cmsItemForBgid(3, bcid);
        setUpContentResolving(ImmutableSet.of(bgItem, oobgItem, parentBcidItem, cmsItem));
        ScoredCandidates<Content> scoredCandidates;
        scoredCandidates = aliasGenerator.generate(
                bgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(!scoredCandidates.candidates().containsKey(bgItem));
        assertTrue(scoredCandidates.candidates().get(oobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(parentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(cmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                oobgItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(bgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(oobgItem));
        assertTrue(scoredCandidates.candidates().get(parentBcidItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(cmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                parentBcidItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(bgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(oobgItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(parentBcidItem));
        assertTrue(scoredCandidates.candidates().get(cmsItem) == SCORE_ON_MATCH);

        scoredCandidates = aliasGenerator.generate(
                cmsItem,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().get(bgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(oobgItem) == SCORE_ON_MATCH);
        assertTrue(scoredCandidates.candidates().get(parentBcidItem) == SCORE_ON_MATCH);
        assertTrue(!scoredCandidates.candidates().containsKey(cmsItem));
    }

    @Test
    public void excludesPublishersWithSameAlias() {
        Alias alias = new Alias("bcid", "1");
        Set<Alias> aliases = ImmutableSet.of(alias);
        Item subject = new Item();
        subject.setCanonicalUri("uri1");
        subject.setAliases(aliases);

        Item item = new Item();
        item.setCanonicalUri("uri2");
        item.setAliases(aliases);
        item.setPublisher(Publisher.BARB_TRANSMISSIONS);

        Item differentItem = new Item();
        differentItem.setCanonicalUri("uri3");
        differentItem.setAliases(aliases);
        differentItem.setPublisher(Publisher.BBC);

        when(aliasLookupEntryStore.entriesForAliases(
                Optional.of(alias.getNamespace()),
                ImmutableSet.of(alias.getValue()),
                INCLUDE_UNPUBLISHED_CONTENT
        )).thenReturn(ImmutableList.of(lookupEntryForContent(item), lookupEntryForContent(differentItem)));

        ResolvedContent.ResolvedContentBuilder resolvedContentBuilder = ResolvedContent.builder();
        resolvedContentBuilder.put("q", item);

        when(aliasResolver.findByCanonicalUris(ImmutableSet.of(item.getCanonicalUri())))
                .thenReturn(resolvedContentBuilder.build());

        ScoredCandidates<Content> scoredCandidates;
        scoredCandidates = aliasGenerator.generate(
                subject,
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(!scoredCandidates.candidates().containsKey(differentItem));
        assertThat(scoredCandidates.candidates().get(item), is(SCORE_ON_MATCH));
    }

}
