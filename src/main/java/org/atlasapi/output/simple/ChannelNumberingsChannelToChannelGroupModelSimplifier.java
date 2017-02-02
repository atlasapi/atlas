package org.atlasapi.output.simple;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.entity.simple.HistoricalChannelNumberingEntry;
import org.atlasapi.output.Annotation;
import org.joda.time.LocalDate;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class ChannelNumberingsChannelToChannelGroupModelSimplifier implements ModelSimplifier<Iterable<ChannelNumbering>, Iterable<org.atlasapi.media.entity.simple.ChannelNumbering>> {

    private static final Predicate<ChannelNumbering> CURRENT_OR_FUTURE = new Predicate<ChannelNumbering>() {
        @Override
        public boolean apply(ChannelNumbering input) {
            return input.getEndDate() == null || input.getEndDate().isAfter(new LocalDate());
        }
    };
    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelNumberingChannelGroupModelSimplifier channelGroupSimplifier;
    
    public ChannelNumberingsChannelToChannelGroupModelSimplifier(ChannelGroupResolver channelGroupResolver, ChannelNumberingChannelGroupModelSimplifier channelGroupSimplifier) {
        this.channelGroupResolver = channelGroupResolver;
        this.channelGroupSimplifier = channelGroupSimplifier;
    }
    
    @Override
    public Iterable<org.atlasapi.media.entity.simple.ChannelNumbering> simplify(Iterable<ChannelNumbering> channelNumberings,
            final Set<Annotation> annotations, final ApplicationConfiguration config) {
        if (annotations.contains(Annotation.HISTORY)) {
            final Multimap<Long, ChannelNumbering> channelMapping = ArrayListMultimap.create();
            for (ChannelNumbering numbering : channelNumberings) {
                channelMapping.put(numbering.getChannelGroup(), numbering);
            }

            return Iterables.concat(Iterables.transform(
                channelMapping.keySet(), 
                new Function<Long, Iterable<org.atlasapi.media.entity.simple.ChannelNumbering>>() {
                    @Override
                    public Iterable<org.atlasapi.media.entity.simple.ChannelNumbering> apply(Long input) {
                        Iterable<ChannelNumbering> numberings = channelMapping.get(input);
                        final Iterable<HistoricalChannelNumberingEntry> history = generateHistory(numberings);
                        return simplifyChannelNumberingsWithHistory(numberings, history, config);
                    }
                }));
        } else {
            return Iterables.transform(
                    Iterables.filter(channelNumberings, CURRENT_OR_FUTURE), 
                    new Function<ChannelNumbering, org.atlasapi.media.entity.simple.ChannelNumbering>() {
                        @Override
                        public org.atlasapi.media.entity.simple.ChannelNumbering apply(ChannelNumbering input) {
                            org.atlasapi.media.entity.simple.ChannelNumbering simple = simplifyNumbering(input, annotations.contains(Annotation.HISTORY), null, config);
                            simple.setChannelNumber(input.getChannelNumber());
                            return simple;
                        }
                    }
                    );
        }
    }
    
    private Iterable<HistoricalChannelNumberingEntry> generateHistory(Iterable<ChannelNumbering> numberings) {
        return Iterables.transform(numberings, new Function<ChannelNumbering, HistoricalChannelNumberingEntry>() {
            @Override
            public HistoricalChannelNumberingEntry apply(ChannelNumbering input) {
                HistoricalChannelNumberingEntry entry = new HistoricalChannelNumberingEntry();
                entry.setStartDate(input.getStartDate());
                entry.setChannelNumber(input.getChannelNumber());
                return entry;
            }
        });
    }
    
    private org.atlasapi.media.entity.simple.ChannelNumbering simplifyNumbering(ChannelNumbering input, boolean showHistory, 
        Iterable<HistoricalChannelNumberingEntry> history, ApplicationConfiguration config) {
        
        org.atlasapi.media.entity.simple.ChannelNumbering simple = new org.atlasapi.media.entity.simple.ChannelNumbering();
        Optional<ChannelGroup> channelGroup = channelGroupResolver.channelGroupFor(input.getChannelGroup());
        Preconditions.checkArgument(channelGroup.isPresent(), "Could not resolve channelGroup with id " +  input.getChannelGroup());
        if (showHistory) {
            simple.setChannelGroup(channelGroupSimplifier.simplify(channelGroup.get(), ImmutableSet.of(Annotation.HISTORY), config));
        } else {
            simple.setChannelGroup(channelGroupSimplifier.simplify(channelGroup.get(), ImmutableSet.of(Annotation.HISTORY), config));
        }
        if (input.getStartDate() != null) {
            simple.setStartDate(input.getStartDate().toDate());
        }
        if (input.getEndDate() != null) {
            simple.setEndDate(input.getEndDate().toDate());
        }
        if (history != null) {
            simple.setHistory(history);
        }
        return simple;
    }
    
    private Iterable<org.atlasapi.media.entity.simple.ChannelNumbering> simplifyChannelNumberingsWithHistory(
            Iterable<ChannelNumbering> numberings, Iterable<HistoricalChannelNumberingEntry> history, 
            ApplicationConfiguration config) {
        
        Iterable<ChannelNumbering> currentNumberings = ChannelNumbering.CURRENT_NUMBERINGS(numberings);
        
        if (Iterables.isEmpty(currentNumberings)) {
            if (Iterables.isEmpty(numberings)) {
                return ImmutableList.of();
            }
            return ImmutableList.of(simplifyNumbering(Iterables.get(numberings, 0), true, history, config));
        } else {
            List<org.atlasapi.media.entity.simple.ChannelNumbering> simpleNumberings = Lists.newArrayList();
            for (ChannelNumbering currentNumbering : currentNumberings) {
                org.atlasapi.media.entity.simple.ChannelNumbering simple = simplifyNumbering(currentNumbering, true, history, config);
                simple.setChannelNumber(currentNumbering.getChannelNumber());
                simpleNumberings.add(simple);
            }
            return simpleNumberings;
        }
    }
}
