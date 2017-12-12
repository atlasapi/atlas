package org.atlasapi.output;

import com.google.common.collect.Iterables;
import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.simple.PeopleQueryResult;
import org.atlasapi.output.simple.ImageSimplifier;
import org.atlasapi.output.simple.PersonModelSimplifier;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;

import java.util.Set;

/**
 * {@link AtlasModelWriter} that translates the full URIplay object model
 * into a simplified form and renders that as XML.
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class SimplePersonModelWriter extends TransformingModelWriter<Iterable<Person>, PeopleQueryResult> {

    private final PersonModelSimplifier personSimplifier;

	public SimplePersonModelWriter(
	        AtlasModelWriter<PeopleQueryResult> outputter,
            ImageSimplifier imageSimplifier,
	        UpcomingItemsResolver upcomingResolver,
            AvailableItemsResolver availableResolver
    ) {
		super(outputter);
        this.personSimplifier = new PersonModelSimplifier(imageSimplifier, upcomingResolver, availableResolver);
	}
	
	@Override
	protected PeopleQueryResult transform(
	        Iterable<Person> people,
            final Set<Annotation> annotations,
            final Application application
    ) {
        PeopleQueryResult simplePeople = new PeopleQueryResult();
        simplePeople.setPeople(Iterables.transform(people,
                input -> personSimplifier.simplify(input, annotations, application))
        );
        return simplePeople;
    }
	
}
