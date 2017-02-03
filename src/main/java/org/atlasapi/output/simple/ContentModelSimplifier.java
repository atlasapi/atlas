package org.atlasapi.output.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.feeds.utils.DescriptionWatermarker;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.ContentGroupRef;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.KeyPhrase;
import org.atlasapi.media.entity.simple.Language;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class ContentModelSimplifier<F extends Content, T extends Description> extends DescribedModelSimplifier<F, T> {

    private final ContentGroupResolver contentGroupResolver;
    private final ModelSimplifier<ContentGroup, org.atlasapi.media.entity.simple.ContentGroup> contentGroupSimplifier;
    private final TopicQueryResolver topicResolver;
    private final ModelSimplifier<Topic, org.atlasapi.media.entity.simple.Topic> topicSimplifier;
    private final ProductResolver productResolver;
    private final ModelSimplifier<Product, org.atlasapi.media.entity.simple.Product> productSimplifier;
    protected final CrewMemberSimplifier crewSimplifier = new CrewMemberSimplifier();
    private boolean exposeIds = false;
    private final Map<String, Locale> localeMap;
    private final PeopleQueryResolver peopleQueryResolver;
    private final CrewMemberAndPersonSimplifier crewMemberAndPersonSimplifier;
    private final EventRefModelSimplifier eventRefSimplifier;

    public ContentModelSimplifier(
            String localHostName,
            ContentGroupResolver contentGroupResolver,
            TopicQueryResolver topicResolver,
            ProductResolver productResolver,
            ImageSimplifier imageSimplifier,
            PeopleQueryResolver peopleResolver,
            UpcomingItemsResolver upcomingResolver,
            AvailableItemsResolver availableResolver,
            @Nullable DescriptionWatermarker descriptionWatermarker,
            EventRefModelSimplifier eventSimplifier
    ) {
        super(
                imageSimplifier,
                SubstitutionTableNumberCodec.lowerCaseOnly(),
                descriptionWatermarker
        );
        this.contentGroupResolver = contentGroupResolver;
        this.topicResolver = topicResolver;
        this.productResolver = productResolver;
        this.contentGroupSimplifier = new ContentGroupModelSimplifier(imageSimplifier);
        this.topicSimplifier = new TopicModelSimplifier(localHostName);
        this.productSimplifier = new ProductModelSimplifier(localHostName);
        this.localeMap = initLocalMap();
        this.peopleQueryResolver = peopleResolver;
        this.crewMemberAndPersonSimplifier = new CrewMemberAndPersonSimplifier(imageSimplifier, upcomingResolver, availableResolver);
        this.eventRefSimplifier = eventSimplifier;
    }

    private Map<String, Locale> initLocalMap() {
        ImmutableMap.Builder<String, Locale> builder = ImmutableMap.builder();
        for (String code : Locale.getISOLanguages()) {
            builder.put(code, new Locale(code));
        }
        return builder.build();
    }


    protected void copyBasicContentAttributes(
            F content,
            T simpleDescription,
            final Set<Annotation> annotations,
            final Application application
    ) {
        copyBasicDescribedAttributes(content, simpleDescription, annotations);

        if(!exposeIds) {
            simpleDescription.setId(null);
        }
        
        if (annotations.contains(Annotation.DESCRIPTION) || annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            simpleDescription.setYear(content.getYear());
            if (annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
                simpleDescription.setOriginalLanguages(languagesFrom(content.getLanguages()));
                simpleDescription.setCertificates(simpleCertificates(content.getCertificates()));
            }
            simpleDescription.setGenericDescription(content.getGenericDescription());
        }
        
        if (annotations.contains(Annotation.CLIPS)) {
            simpleDescription.setClips(clipToSimple(content.getClips(), annotations, application));
        }
        if (annotations.contains(Annotation.TOPICS)) {
            simpleDescription.setTopics(topicRefToSimple(content, content.getTopicRefs(), annotations, application));
        }
        if (annotations.contains(Annotation.CONTENT_GROUPS)) {
            simpleDescription.setContentGroups(contentGroupRefToSimple(content.getContentGroupRefs(), annotations, application));
        }
        if (annotations.contains(Annotation.KEY_PHRASES)) {
            simpleDescription.setKeyPhrases(simplifyPhrases(content));
        }
        if (annotations.contains(Annotation.PRODUCTS)) {
            simpleDescription.setProducts(resolveAndSimplifyProductsFor(content, annotations, application));
        }
        
        if (annotations.contains(Annotation.PEOPLE_DETAIL)) {
            simpleDescription.setPeople(
                    Lists.transform(
                            resolve(content.people(), application),
                            input -> crewMemberAndPersonSimplifier.simplify(input, annotations, application)
                    ).stream()
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList())
            );
        } else if (annotations.contains(Annotation.PEOPLE)) {
            simpleDescription.setPeople(
                    content.people().stream()
                    .map(input -> crewSimplifier.simplify(input, annotations, application))
                    .collect(Collectors.toList())
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList())
            );
        }
        
        if (annotations.contains(Annotation.SIMILAR)) {
            
                // TODO temporary creation of ChildRef - we'll be changing output when we filter based on API key shortly
            simpleDescription.setSimilarContent(content.getSimilarContent().stream()
                    .map(s -> ContentIdentifier.identifierFor(
                            new ChildRef(
                                    s.getId(),
                                    s.getUri(),
                            "0",
                                     new DateTime(),
                                     s.getEntityType()
                            ),
                            idCodec))
                    .collect(Collectors.toList())
            );
        }
        
        simpleDescription.setEvents(content.events().stream()
                .map(input -> eventRefSimplifier.simplify(input, annotations, application))
                .collect(Collectors.toList())
        );
    }
    
    private List<CrewMemberAndPerson> resolve(List<CrewMember> crewMembers, Application application) {
        Iterable<Person> people = peopleQueryResolver.people(
                ImmutableSet.copyOf(
                        Lists.transform(crewMembers, Identified.TO_URI)
                                .stream()
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList())
                ),
                application
        );

        final ImmutableMap<String, Person> peopleIndex = Maps.uniqueIndex(people, Identified.TO_URI);

        return Lists.transform(crewMembers, crewMember -> {
            Person person = null;
            if (crewMember.getCanonicalUri() != null) {
                person = peopleIndex.get(crewMember.getCanonicalUri());
            }
            return new CrewMemberAndPerson(crewMember, person);
        });
    }

    private Iterable<org.atlasapi.media.entity.simple.Certificate> simpleCertificates(Set<Certificate> certificates) {
        return certificates.stream()
                .map(certificate -> new org.atlasapi.media.entity.simple.Certificate(
                        certificate.classification(),
                        certificate.country().code()))
                .collect(Collectors.toList());
    }
    
    protected Language languageForCode(String input) {
        Locale locale = localeMap.get(input);
        if (locale == null) {
            return null;
        }
        return new Language(locale.getLanguage(), locale.getDisplayLanguage());
    }
    
    private Iterable<Language> languagesFrom(Set<String> languages) {
        return languages.stream()
                .map(this::languageForCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Iterable<org.atlasapi.media.entity.simple.Product> resolveAndSimplifyProductsFor(
            Content content,
            final Set<Annotation> annotations,
            final Application application
    ) {
        return StreamSupport.stream(
                filter(productResolver.productsForContent(
                        content.getCanonicalUri()),
                        application
                ).spliterator(),
                false)
                .map(product -> productSimplifier.simplify(product, annotations, application))
                .collect(Collectors.toList());
    }

    private Iterable<Product> filter(
            Iterable<Product> productsForContent,
            final Application application
    ) {
        return StreamSupport.stream(productsForContent.spliterator(), false)
                .filter(input -> application.getConfiguration().isReadEnabled(input.getPublisher()))
                .collect(Collectors.toList());
    }
    
    public void exposeIds(boolean expose) {
        this.exposeIds = expose;
    }

    private Iterable<Topic> res(Iterable<Long> topics, Set<Annotation> annotations) {
        if (Iterables.isEmpty(topics)) { // don't even ask (the resolver)
            return ImmutableList.of();
        }
        return topicResolver.topicsForIds(topics);
    }

    private Iterable<ContentGroup> resolveContentGroups(Iterable<Long> contentGroups, Set<Annotation> annotations) {
        if (Iterables.isEmpty(contentGroups)) { // don't even ask (the resolver)
            return ImmutableList.of();
        }
        return contentGroupResolver.findByIds(contentGroups)
                .asResolvedMap()
                .values()
                .stream()
                .map(identified -> (ContentGroup) identified)
                .collect(Collectors.toList());
    }

    public Iterable<KeyPhrase> simplifyPhrases(F content) {
        return content.getKeyPhrases().stream()
                .map(keyPhrase -> new KeyPhrase(
                        keyPhrase.getPhrase(),
                        toPublisherDetails(keyPhrase.getPublisher()),
                        keyPhrase.getWeighting()
                ))
                .collect(Collectors.toList());
    }

    private List<org.atlasapi.media.entity.simple.Item> clipToSimple(
            List<Clip> clips,
            final Set<Annotation> annotations,
            final Application application
    ) {
        return Lists.transform(clips, clip -> simplify(clip, annotations, application));
    }

    private List<org.atlasapi.media.entity.simple.TopicRef> topicRefToSimple(
            final Content content,
            List<TopicRef> contentTopics,
            final Set<Annotation> annotations,
            final Application application
    ) {

        final Map<Long, Topic> topics = Maps.uniqueIndex(res(contentTopics.stream()
                .map(TopicRef::getTopic)
                .collect(Collectors.toList()), annotations), Identified::getId);

        return Lists.transform(contentTopics, topicRef -> {
            org.atlasapi.media.entity.simple.TopicRef simpleTopicRef = new org.atlasapi.media.entity.simple.TopicRef();
            simpleTopicRef.setSupervised(topicRef.isSupervised());
            simpleTopicRef.setWeighting(topicRef.getWeighting());
            simpleTopicRef.setRelationship(topicRef.getRelationship().toString());
            simpleTopicRef.setOffset(topicRef.getOffset());
            simpleTopicRef.setTopic(topicSimplifier.simplify(topics.get(topicRef.getTopic()), annotations, application));
            if (annotations.contains(Annotation.PUBLISHER)) {
                simpleTopicRef.setPublisher(toPublisherDetails(topicRef.getPublisher()));
            }
            return simpleTopicRef;
        });
    }

    private Iterable<org.atlasapi.media.entity.simple.ContentGroup> contentGroupRefToSimple(
            List<ContentGroupRef> refs,
            final Set<Annotation> annotations,
            final Application application
    ) {

        Iterable<ContentGroup> groups = resolveContentGroups(
                Iterables.transform(refs, ContentGroupRef::getId),
                annotations
        );

        return Iterables.transform(
                groups,
                group -> contentGroupSimplifier.simplify(group, annotations, application)
        );
    }

    protected abstract org.atlasapi.media.entity.simple.Item simplify(Item item, Set<Annotation> annotations, Application application);
}
