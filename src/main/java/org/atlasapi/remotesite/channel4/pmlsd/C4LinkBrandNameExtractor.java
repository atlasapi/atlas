package org.atlasapi.remotesite.channel4.pmlsd;

import static org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi.PROGRAMMES_BASE;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Publisher;

class C4LinkBrandNameExtractor {

    private static final Pattern CHANNEL_4_LINK = Pattern.compile(
            "https?://.+\\.channel4\\.com/[^/]+/([^./]+).*"
    );

    public Optional<String> brandNameFrom(String url) {
        Matcher matcher = CHANNEL_4_LINK.matcher(url);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }

    public Optional<String> atlasBrandUriFrom(Publisher publisher, String url) {
        return brandNameFrom(url)
                .map(brandName -> String.format(
                        "http://%s/pmlsd/%s",
                        publisherHost(publisher), brandName
                ));
    }

    public Optional<String> c4CanonicalUriFrom(String url) {
        return brandNameFrom(url)
                .map(PROGRAMMES_BASE::concat);
    }

    private String publisherHost(Publisher publisher) {
        String host = C4PmlsdModule.PUBLISHER_TO_CANONICAL_URI_HOST_MAP.get(publisher);
        if (host == null) {
            throw new IllegalArgumentException("Could not map publisher " + publisher.key() + " to a canonical URI host");
        }
        return host;
    }
}
