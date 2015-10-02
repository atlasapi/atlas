package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.List;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.bbc.ion.BbcIonServices;
import org.joda.time.DateTime;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.model.Id;
import com.metabroadcast.atlas.glycerin.model.Ids;

/**
 * Extracts a {@link com.metabroadcast.atlas.glycerin.model.Broadcast Atlas
 * Broadcast} from a {@link Broadcast Nitro Broadcast}.
 */
public class NitroBroadcastExtractor
    implements ContentExtractor<com.metabroadcast.atlas.glycerin.model.Broadcast, Optional<Broadcast>> {

    private static final String TERRESTRIAL_EVENT_LOCATOR_TYPE = "terrestrial_event_locator";
    private static final String TERRESTRIAL_PROGRAM_CRID_TYPE = "terrestrial_programme_crid";
    private static final String BID_TYPE = "bid";
    private static final String PIPS_AUTHORITY = "pips";
    private static final String TELEVIEW_AUTHORITY = "teleview";
    
    private static final Predicate<Alias> PCRID_ALIAS = new Predicate<Alias>() {

        @Override
        public boolean apply(Alias input) {
            return input.getNamespace().contains(TERRESTRIAL_PROGRAM_CRID_TYPE);
        }
        
    };

    @Override
    public Optional<Broadcast> extract(com.metabroadcast.atlas.glycerin.model.Broadcast source) {
        String channel = BbcIonServices.get(source.getService().getSid());
        if (channel == null) {
            return Optional.absent();
        }
        DateTime start = NitroUtil.toDateTime(source.getPublishedTime().getStart());
        DateTime end = NitroUtil.toDateTime(source.getPublishedTime().getEnd());
        Broadcast broadcast = new Broadcast(channel, start, end)
            .withId("bbc:"+source.getPid());
        broadcast.setRepeat(source.isIsRepeat());
        broadcast.setAudioDescribed(source.isIsAudioDescribed());
        broadcast.setAliases(extractAliasesFrom(source));
        
        // Adding an alias uri for equivalence between Nitro and YV broadcasts
        Optional<Alias> pcridAlias = Iterables.tryFind(broadcast.getAliases(), PCRID_ALIAS);
        if (pcridAlias.isPresent()) {
            broadcast.setAliasUrls(ImmutableSet.of("pcrid:" + pcridAlias.get().getValue()));            
        }
        broadcast.setAudioDescribed(source.isIsAudioDescribed());
        return Optional.of(broadcast);
    }

    private Iterable<Alias> extractAliasesFrom(
            com.metabroadcast.atlas.glycerin.model.Broadcast source) {
        Ids ids = source.getIds();

        if (ids == null) {
            return ImmutableList.of();
        }

        return aliasesForTypesAndAuthorities(ids.getId(),
                new TypeAndAuthority(TERRESTRIAL_EVENT_LOCATOR_TYPE, TELEVIEW_AUTHORITY),
                new TypeAndAuthority(TERRESTRIAL_PROGRAM_CRID_TYPE, TELEVIEW_AUTHORITY),
                new TypeAndAuthority(BID_TYPE, PIPS_AUTHORITY));
    }

    private Predicate<Id> idOfTypeAndAuthority(final TypeAndAuthority typeAndAuthority) {
        return new Predicate<Id>() {
            @Override
            public boolean apply(Id input) {
                return typeAndAuthority.getType().equals(input.getType())
                        && typeAndAuthority.getAuthority().equals(input.getAuthority());
            }
        };
    }

    private Iterable<Alias> aliasesForTypesAndAuthorities(List<Id> ids,
            TypeAndAuthority... typeAndAuthorityPairs) {
        ImmutableList.Builder<Alias> aliases = ImmutableList.builder();

        for (TypeAndAuthority typeAndAuthority : typeAndAuthorityPairs) {
            Optional<Id> id = Iterables.tryFind(ids, idOfTypeAndAuthority(typeAndAuthority));

            if (id.isPresent()) {
                aliases.add(createAlias(id.get()));
            }
        }

        return aliases.build();
    }

    private Alias createAlias(Id id) {
        return new Alias(namespaceFor(id), id.getValue());
    }

    private String namespaceFor(Id id) {
        Joiner joiner = Joiner.on(':');
        return joiner.join("bbc", id.getType(), id.getAuthority());
    }

    private static class TypeAndAuthority {
        private final String type;
        private final String authority;

        private TypeAndAuthority(String type, String authority) {
            this.type = type;
            this.authority = authority;
        }

        public String getType() {
            return type;
        }

        public String getAuthority() {
            return authority;
        }
    }

}
