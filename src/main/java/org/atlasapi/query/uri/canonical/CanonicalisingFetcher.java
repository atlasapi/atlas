package org.atlasapi.query.uri.canonical;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.system.Fetcher;
import org.atlasapi.remotesite.NoMatchingAdapterException;

import com.google.common.collect.Sets;

public class CanonicalisingFetcher implements Fetcher<Identified>, ContentResolver {

	private static final int MAX_CANONICALISATIONS = 5;
	private final Log log = LogFactory.getLog(getClass());
	
	private final List<Canonicaliser> chain;
	private final Fetcher<Identified> delegate;

	public CanonicalisingFetcher(Fetcher<Identified> delegate, List<Canonicaliser> chain) {
		this.delegate = delegate;
		this.chain = chain;
	}
	
	private static class Canonicalisation {
		
		private final InterestedInTheEventualContentCanonicaliser canonicaliser;
		private final String url;

		public Canonicalisation(InterestedInTheEventualContentCanonicaliser canonicaliser, String url) {
			this.canonicaliser = canonicaliser;
			this.url = url;
		}
	}

	@Override
	public Identified fetch(String uri) {
		Set<String> aliases = Sets.newHashSet();
		Set<Canonicalisation> resultAwareCanonicalisers = Sets.newHashSet();
		String currentUri = uri;
		for (int i = 0; i < MAX_CANONICALISATIONS; i++) {
			try {
				Identified bean = delegate.fetch(currentUri);
				if (bean == null) {
					return null;
				} else {
					return saveAliases(bean, resultAwareCanonicalisers);
				}
			} catch (NoMatchingAdapterException e) {
				String nextUri = nextUri(currentUri, aliases, resultAwareCanonicalisers);
				if (nextUri == null || nextUri.equals(currentUri)) {
					return null;
				}
				aliases.add(currentUri);
				currentUri = nextUri;
			}
		}
		return null;
	}

	private String nextUri(String currentUri, Set<String> aliases, Set<Canonicalisation> resultAwareCanonicalisers) {
		for (Canonicaliser canonicaliser : chain) {
			String next = canonicaliseOrNull(currentUri, canonicaliser);
			if (next != null) {
				if (canonicaliser instanceof InterestedInTheEventualContentCanonicaliser) {
					resultAwareCanonicalisers.add(new Canonicalisation((InterestedInTheEventualContentCanonicaliser) canonicaliser, currentUri));
				}
				return next;
			}
		}
		return null;
	}

	private String canonicaliseOrNull(String currentUri, Canonicaliser canonicaliser) {
		try {
			return canonicaliser.canonicalise(currentUri);
		} catch (Exception e) {
			log.warn(e);
			return null;
		}
	}

	private Identified saveAliases(Identified bean, Set<Canonicalisation> resultAwareCanonicalisers) {
		if (!resultAwareCanonicalisers.isEmpty()) {
			for (Canonicalisation canonicalisation : resultAwareCanonicalisers) {
				canonicalisation.canonicaliser.resolvedTo(canonicalisation.url, bean);
			}
		}
		return bean;
	}

	@Override
	public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
		ResolvedContentBuilder builder = ResolvedContent.builder();
		for (String uri : canonicalUris) {
			builder.put(uri, fetch(uri));
		}
		return builder.build();
	}

	@Override
	public ResolvedContent findByUris(Iterable<String> uris) {
	    throw new UnsupportedOperationException();
	}
}
