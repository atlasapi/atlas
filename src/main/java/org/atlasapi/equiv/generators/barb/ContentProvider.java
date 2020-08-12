package org.atlasapi.equiv.generators.barb;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.ContentProvider.Tier.T1;
import static org.atlasapi.equiv.generators.barb.ContentProvider.Tier.T2;

/**
 * Copied from cdmf-generator com.metabroadcast.barb.ContentProvider
 */
public enum ContentProvider {

    BBC(    T1, "1", "nitro.bbc.co.uk",              ImmutableSet.of("gb:bbc:nitro:prod:version:pid", "gb:bbc:pid"),           "BBC"),
    ITV(    T1, "2", "cps.itv.com",                  ImmutableSet.of("gb:itv:production:id"), "ITV"),
    C4(     T1, "3", "pmlsd.channel4.com",           ImmutableSet.of("gb:c4:episode:id"),     "ALL 4") {
        //e.g. 23444-234  The correct ids have a slash not a dash.
        private Pattern invalid = Pattern.compile("[0-9]{4,5}-[0-9]{3}");
        @Override public boolean isBcidValueValid(String value) {
            return super.isBcidValueValid(value) && !invalid.matcher(value).matches();
        }
        @Override public String toAtlasBcidValue(@Nonnull String value) {
            value = super.fromAtlasBcidValue(value);
            if (invalid.matcher(value).matches()) {
                throw new IllegalArgumentException("invalid BCID value: " + value);
            }
            if (value.isEmpty()) return value;

            return value.startsWith("C4:") ? value.substring(3) : value;
        }
        @Override public String fromAtlasBcidValue(@Nonnull String value) {
            String val = super.fromAtlasBcidValue(value);

            if (value.isEmpty()) return value;
            return val.startsWith("C4:") ? val : "C4:".concat(val);
        }
    },
    C5(     T1, "4", "datasubmission.channel5.com",  ImmutableSet.of("gb:c5:bcid"),           "C5"),
    UKTV(   T1, "63", "uktv.co.uk",                   ImmutableSet.of("gb:uktv:bcid"),         "UKTV"),
    //TODO S4C(    T2, "",                             ImmutableSet.of(),                     "S4C"),
    SKY(    T2, "5", "",                             ImmutableSet.of("gb:barb:broadcastGroup:5:bcid"),          "Sky"), //will match as an alias
    UNKNOWN(T2, "", "",                             ImmutableSet.of(),                     "");

    private static final Pattern BGID_FROM_ALIAS = Pattern.compile("^gb:barb:broadcastGroup:([0-9]+):.*");
    private static final Pattern OOBGID_FROM_ALIAS = Pattern.compile("^gb:barb:originatingOwner:broadcastGroup:([0-9]+):.*");

    private static final String TXLOG_BROADCASTER_GROUP = "txlog:broadcaster_group";
    private static final String NLE_BROADCASTER_GROUP = "nle:broadcaster_group";
    private static final String CDMF_BROADCASTER_GROUP = "cdmf:broadcaster_group";

    public enum Tier { T1, T2 }

    private final Tier tier;
    private final String broadcasterGroupId;
    private final String source;
    private final Set<String> bcidNamespaces;
    private final String censusName;
    ContentProvider(Tier tier, String broadcasterGroupId, String source, Iterable<String> bcidNamespaces, String censusName) {
        this.tier = tier;
        this.broadcasterGroupId = broadcasterGroupId;
        this.source = source;
        this.bcidNamespaces = ImmutableSet.copyOf(bcidNamespaces);
        this.censusName = censusName.toUpperCase(Locale.UK);
    }

    public Tier getTier() {
        return tier;
    }

    public String getBroadcasterGroupId() {
        return broadcasterGroupId;
    }

    public String getSource() {
        return source;
    }

    public Set<String> getBcidNamespaces() {
        return bcidNamespaces;
    }

    public boolean isBcidValueValid(@Nullable String value) {
        return value != null && !bcidNamespaces.isEmpty() && !value.trim().isEmpty();
    }

    public String toAtlasBcidValue(@Nonnull String value)
            throws NullPointerException, IllegalArgumentException {                             // NOSONAR
        checkNotNull(value, "null BCID value");
        checkArgument(!bcidNamespaces.isEmpty(), "empty BCID namespace");
        value = value.trim();
        checkArgument(!value.isEmpty(), "empty BCID value");
        return value;
    }
    public String fromAtlasBcidValue(@Nonnull String value)
            throws NullPointerException, IllegalArgumentException {                             // NOSONAR
        checkNotNull(value, "null BCID value");
        checkArgument(!bcidNamespaces.isEmpty(), "empty BCID namespace");
        value = value.trim();
        checkArgument(!value.isEmpty(), "empty BCID value");
        return value;
    }

    public String getCensusName() {
        return censusName;
    }

    private static final Map<String, ContentProvider> BGID_MAP;
    private static final Map<String, ContentProvider> SOURCE_MAP;
    private static final Map<String, ContentProvider> BCID_NS_MAP;
    private static final Map<String, ContentProvider> CENSUS_NAME_MAP;
    static {
        ImmutableMap.Builder<String, ContentProvider> bgidMap = ImmutableMap.builder();
        ImmutableMap.Builder<String, ContentProvider> sourceBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, ContentProvider> bcidNsMap = ImmutableMap.builder();
        ImmutableMap.Builder<String, ContentProvider> censusNameMap = ImmutableMap.builder();
        for (ContentProvider cp : ContentProvider.values()) {
            if (!cp.getBroadcasterGroupId().isEmpty()) bgidMap.put(cp.getBroadcasterGroupId(), cp);
            if (!cp.getSource().isEmpty()) sourceBuilder.put(cp.getSource(), cp);
            for(String bcidNamespace : cp.getBcidNamespaces()) {
                bcidNsMap.put(bcidNamespace, cp);
            }
            if (!cp.getCensusName().isEmpty()) censusNameMap.put(cp.getCensusName(), cp);
        }
        BGID_MAP = bgidMap.build();
        SOURCE_MAP = sourceBuilder.build();
        BCID_NS_MAP = bcidNsMap.build();
        CENSUS_NAME_MAP = censusNameMap.build();
    }

    public static ContentProvider fromSource(@Nullable String source) {
        if (source == null) return UNKNOWN;
        return SOURCE_MAP.getOrDefault(source.trim(), UNKNOWN);
    }
    public static ContentProvider fromBcidNs(@Nullable String bcidNs) {
        if (bcidNs == null) return UNKNOWN;
        return BCID_NS_MAP.getOrDefault(bcidNs.trim(), UNKNOWN);
    }

    public static ContentProvider fromBroadcasterGroupId(@Nullable String broadcasterGroupId) {
        if (broadcasterGroupId == null) return UNKNOWN;
        return BGID_MAP.getOrDefault(broadcasterGroupId.trim(), UNKNOWN);
    }

    public static ContentProvider fromCensusName(@Nullable String censusName) {
        if (censusName == null) return UNKNOWN;
        return CENSUS_NAME_MAP.getOrDefault(censusName.trim().toUpperCase(Locale.UK), UNKNOWN);
    }

    public static ContentProvider fromContent(@Nullable Content content) {
        if (content == null) return UNKNOWN;
        ContentProvider cp = fromSource(content.getPublisher().key());
        if (cp != UNKNOWN) return cp;
        for (String sameAs : content.getAliasUrls()) {
            cp = fromSource(sameAs);
            if (cp != UNKNOWN) return cp;
        }
        return UNKNOWN;
    }

    public static Set<ContentProvider> getAllfromContent(@Nullable Content content) {
        ImmutableSet.Builder<ContentProvider> set = ImmutableSet.builder();
        if (content == null) {
            return set.build();
        }
        ContentProvider cp = fromSource(content.getPublisher().key());
        if (cp != UNKNOWN) {
            set.add(cp);
        }
        for (String sameAs : content.getAliasUrls()) {
            cp = fromSource(sameAs);
            if (cp != UNKNOWN) {
                set.add(cp);
            }
        }
        for (Alias alias : content.getAliases()) {
            cp = fromBcidNs(alias.getNamespace());
            if (cp != UNKNOWN) {
                set.add(cp);
            }
        }

        content.getAliases().stream()
                .map(Alias::getNamespace)
                .map(BGID_FROM_ALIAS::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(1))
                .forEach(bgid -> addIfValid(set, bgid));

        content.getAliases().stream()
                .map(Alias::getNamespace)
                .map(OOBGID_FROM_ALIAS::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(1))
                .forEach(bgid -> addIfValid(set, bgid));

        String bgid = content.getCustomField(TXLOG_BROADCASTER_GROUP);
        addIfValid(set, bgid);
        bgid = content.getCustomField(NLE_BROADCASTER_GROUP);
        addIfValid(set, bgid);
        bgid = content.getCustomField(CDMF_BROADCASTER_GROUP);
        addIfValid(set, bgid);

        return set.build();
    }

    private static void addIfValid(ImmutableSet.Builder<ContentProvider> set, String bgid) {
        ContentProvider cp = fromBroadcasterGroupId(bgid);
        if (cp != UNKNOWN) {
            set.add(cp);
        }
    }

    public static boolean isTier1(Content content) {
        Set<ContentProvider> contentProviders = ContentProvider.getAllfromContent(content);
        return contentProviders.stream()
                .anyMatch(cp -> cp.getTier().equals(ContentProvider.Tier.T1));
    }

}
