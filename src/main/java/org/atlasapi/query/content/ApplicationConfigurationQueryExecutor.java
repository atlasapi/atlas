package org.atlasapi.query.content;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.content.criteria.AtomicQuery;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.attribute.Attribute;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.content.criteria.operator.Operators;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class ApplicationConfigurationQueryExecutor implements KnownTypeQueryExecutor {
	
	private final KnownTypeQueryExecutor delegate;

	public ApplicationConfigurationQueryExecutor(KnownTypeQueryExecutor delegate) {
		this.delegate = delegate;
	}		
		
//	@Override
//	public List<Content> discover(ContentQuery query) {
//		return delegate.discover(queryForContent(query));
//	}

	@Override
	public Map<String,List<Identified>> executeUriQuery(Iterable<String> uris, ContentQuery query) {
		return delegate.executeUriQuery(uris, queryForContent(query));
	}

	@Override
	public Map<String,List<Identified>> executeIdQuery(Iterable<Long> ids, ContentQuery query) {
	    return delegate.executeIdQuery(ids, queryForContent(query));
	}

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace, Iterable<String> values,
            ContentQuery query) {
        return delegate.executeAliasQuery(namespace, values, queryForContent(query));
    }
    
    @Override
    public Map<String, List<Identified>> executePublisherQuery(Iterable<Publisher> publishers,
            ContentQuery query) {
        return delegate.executePublisherQuery(publishers, queryForContent(query));
    }

	private ContentQuery queryForContent(ContentQuery query) {
		Iterable<AtomicQuery> softs = ImmutableList.of(/*(AtomicQuery)
			mergeAttribute(Attributes.VERSION_PROVIDER, query)
		*/);
		
		query.setSoftConstraints(softs);

		Iterable<AtomicQuery> queryAtoms = ImmutableSet.of((AtomicQuery)
			mergeAttribute(Attributes.DESCRIPTION_PUBLISHER, query)
		);
		return ContentQuery.joinTo(query, new ContentQuery(queryAtoms));
	}

	private AtomicQuery mergeAttribute(Attribute<?> attr, ContentQuery query){
		Set<Publisher> configPublishers = query.getConfiguration().getEnabledSources();
		ImmutableSet<Publisher> requestedPublishers = query.includedPublishers();
		
		Set<?> values;
		if (requestedPublishers.isEmpty()) {
			values = configPublishers;
		} else {
			Set<Publisher> pubIntersection = Sets.intersection(configPublishers, requestedPublishers);
			values = pubIntersection.isEmpty() ? configPublishers : pubIntersection;
		}
		
		return attr.createQuery(Operators.EQUALS, values);
	}

}
