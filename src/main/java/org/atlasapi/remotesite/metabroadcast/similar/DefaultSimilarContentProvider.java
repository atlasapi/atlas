package org.atlasapi.remotesite.metabroadcast.similar;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.SimilarContentRef;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;

import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;


public class DefaultSimilarContentProvider implements SimilarContentProvider {

    private static final int AVAILABLE_UPCOMING_BOOST_FACTOR = 3;
    private final ContentLister contentLister;
    private final Publisher publisher;
    private final int similarItemLimit;
    private Map<SimilarContentRef, Set<Integer>> similarHashes;
    private final TraitHashCalculator traitHashCalculator;
    private final AvailableItemsResolver availableItemsResolver;
    private final UpcomingItemsResolver upcomingItemsResolver;
    private final ApplicationConfiguration appConfig = 
            ApplicationConfiguration.defaultConfiguration()
                                    .withSources(Maps.asMap(Publisher.all(), new Function<Publisher, SourceStatus>() {

                                            @Override
                                            public SourceStatus apply(Publisher publisher) {
                                                return SourceStatus.AVAILABLE_ENABLED;
                                            }
        
                                     }));
    
    public DefaultSimilarContentProvider (ContentLister contentLister, Publisher publisher, 
            int similarItemLimit, TraitHashCalculator traitHashCalculator, 
            AvailableItemsResolver availableItemsResolver, UpcomingItemsResolver upcomingItemsResolver) {
        this.contentLister = checkNotNull(contentLister);
        this.publisher = checkNotNull(publisher);
        this.traitHashCalculator = checkNotNull(traitHashCalculator);
        this.availableItemsResolver = checkNotNull(availableItemsResolver);
        this.upcomingItemsResolver = checkNotNull(upcomingItemsResolver);
        this.similarItemLimit = similarItemLimit;
    }
    
    public void initialise() {
        ContentListingCriteria criteria = new ContentListingCriteria.Builder()
                                                    .forPublisher(publisher)
                                                    .forContent(ContentCategory.TOP_LEVEL_CONTENT)
                                                    .build();
        
        Iterator<Content> content = contentLister.listContent(criteria);
        ImmutableMap.Builder<SimilarContentRef, Set<Integer>> similarHashes = ImmutableMap.builder();
        
        while (content.hasNext()) {
            Content c = content.next();
            similarHashes.put(similarContentRefFrom(c), traitHashCalculator.traitHashesFor(c));
        }
        this.similarHashes = similarHashes.build();
    }

    private SimilarContentRef similarContentRefFrom(Content content) {
        Set<Publisher> availableFromPublishers;
        Set<Publisher> upcomingPublishers;
        
        if (content instanceof Container) {
            Container container = (Container) content;
            availableFromPublishers = availableItemsResolver.availableItemsByPublisherFor(container, appConfig).keySet();
            upcomingPublishers = upcomingItemsResolver.upcomingItemsByPublisherFor(container).keySet();
        } else if (content instanceof Item) {
            Item item = (Item) content;
            availableFromPublishers = availableItemsResolver.availableItemsByPublisherFor(item, appConfig).keySet();
            upcomingPublishers = upcomingItemsResolver.upcomingItemsByPublisherFor(item, appConfig).keySet();
        } else {
            throw new IllegalArgumentException("Can't deal with Content of type " + 
                            content.getClass().getSimpleName());
        }
        
        return SimilarContentRef.builder()
                    .withEntityType(EntityType.from(content))
                    .withId(content.getId())
                    .withUri(content.getCanonicalUri())
                    .withScore(0)
                    .withPublishersWithAvailableContent(availableFromPublishers)
                    .withPublishersWithUpcomingContent(upcomingPublishers)
                    .build();
    }

    @Override
    public List<SimilarContentRef> similarTo(Described described) {
        checkState(similarHashes != null, "Must call initialise() first");
        MinMaxPriorityQueue<ScoredContent> similarContent = MinMaxPriorityQueue
                .maximumSize(similarItemLimit)
                .<ScoredContent>create();
        
        Set<Integer> candidateHashes = traitHashCalculator.traitHashesFor(described);
        
        for (Entry<SimilarContentRef, Set<Integer>> entry : similarHashes.entrySet()) {
            if (entry.getKey().getId() != described.getId()) {
                int score = Sets.intersection(candidateHashes, entry.getValue()).size();
                if (!entry.getKey().getPublishersWithAvailableContent().isEmpty()
                        || !entry.getKey().getPublishersWithUpcomingContent().isEmpty()) {
                    score += AVAILABLE_UPCOMING_BOOST_FACTOR;
                }
                similarContent.add(new ScoredContent(entry.getKey(), score));
            }
        }
        
        return FluentIterable.from(similarContent)
                             .transform(TO_SIMILARREF)
                             .toList();
    }

    private static class ScoredContent implements Comparable<ScoredContent> {
        
        private final SimilarContentRef ref;
        private final int score;
        
        public ScoredContent(SimilarContentRef ref, int score) {
            this.ref = ref;
            this.score = score;
        }

        @Override
        public int compareTo(ScoredContent o) {
            return ComparisonChain.start()
                    .compare(o.score, this.score)
                    .compare(this.ref.getId(), o.ref.getId())
                    .result();
        }
    }
    
    private static final Function<ScoredContent, SimilarContentRef> TO_SIMILARREF = new Function<ScoredContent, SimilarContentRef>() {

        @Override
        public SimilarContentRef apply(ScoredContent sc) {
            return SimilarContentRef.Builder.from(sc.ref)
                        .withScore(sc.score)
                        .build();
        }
    };
    
}
