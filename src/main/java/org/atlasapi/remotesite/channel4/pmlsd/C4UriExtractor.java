package org.atlasapi.remotesite.channel4.pmlsd;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public interface C4UriExtractor<B, S, E> {
    
    Optional<C4UriAndAliases> uriForBrand(Publisher publisher, B remote);
    Optional<C4UriAndAliases> uriForSeries(Publisher publisher, S remote);
    Optional<C4UriAndAliases> uriForItem(Publisher publisher, E remote);
    Optional<C4UriAndAliases> uriForClip(Publisher publisher, E remote);

    class C4UriAndAliases {
        private final String uri;
        private final Set<String> aliasUrls;
        private final Set<Alias> aliases;

        private C4UriAndAliases(String uri, Iterable<String> aliasUrls, Iterable<Alias> aliases) {
            this.uri = checkNotNull(uri);
            this.aliasUrls = ImmutableSet.copyOf(aliasUrls);
            this.aliases = ImmutableSet.copyOf(aliases);
        }

        public static C4UriAndAliases create(String uri, Iterable<String> aliasUrls, Iterable<Alias> aliases) {
            return new C4UriAndAliases(uri, aliasUrls, aliases);
        }
        public static C4UriAndAliases create(String uri) {
            return create(uri, ImmutableSet.of(), ImmutableSet.of());
        }
        public static C4UriAndAliases create(String uri, String... aliasUrls) {
            return create(uri, ImmutableSet.copyOf(aliasUrls), ImmutableSet.of());
        }
        public static C4UriAndAliases create(String uri, Alias... aliases) {
            return create(uri, ImmutableSet.of(), ImmutableSet.copyOf(aliases));
        }

        public final String getUri() {
            return uri;
        }

        public final Set<String> getAliasUrls() {
            return aliasUrls;
        }

        public final Set<Alias> getAliases() {
            return aliases;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof C4UriExtractor.C4UriAndAliases)) return false;
            C4UriAndAliases c4Ids = (C4UriAndAliases) o;
            return Objects.equals(uri, c4Ids.uri) &&
                    Objects.equals(aliasUrls, c4Ids.aliasUrls) &&
                    Objects.equals(aliases, c4Ids.aliases);
        }

        @Override public int hashCode() {
            return Objects.hash(uri, aliasUrls, aliases);
        }

        @Override public String toString() {
            return getClass().getSimpleName() + '{' +
                    "uri='" + uri + '\'' +
                    ", aliasUrls=" + aliasUrls +
                    ", aliases=" + aliases +
                    '}';
        }
    }
}
