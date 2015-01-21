package org.atlasapi.remotesite.btfeatured;

import javax.annotation.Nonnull;

import com.metabroadcast.common.scheduling.UpdateProgress;

import nu.xom.Document;
import nu.xom.Element;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.scheduling.ScheduledTask;


public class BtFeaturedContentUpdater extends ScheduledTask {

    static final String CONTENT_GROUP_URI = "http://featured.bt.com";

    private static final String XML_SUFFIX = ".xml";

    private final BtFeaturedClient client;
    private final SimpleElementHandler handler;
    private final ContentGroupResolver groupResolver;
    private final ContentGroupWriter groupWriter;

    private ContentResolver contentResolver;

    private ContentWriter contentWriter;
    
    private static final Logger log = LoggerFactory.getLogger(BtFeaturedContentUpdater.class);

    private final String productBaseUri;

    private final String rootDocumentUri;

    private final ContentMerger contentMerger;

    public BtFeaturedContentUpdater(BtFeaturedClient client, 
            SimpleElementHandler handler, 
            ContentGroupResolver groupResolver, 
            ContentGroupWriter groupWriter, 
            ContentResolver contentResolver, 
            ContentWriter contentWriter, 
            String productBaseUri, 
            String rootDocumentUri) {
        
        this.client = client;
        this.handler = handler;
        this.groupResolver = groupResolver;
        this.groupWriter = groupWriter;
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.productBaseUri = productBaseUri;
        this.rootDocumentUri = rootDocumentUri;
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        
    }
    
    @Override
    protected void runTask() {
        try { 
            ContentGroup contentGroup = null;
            if (groupResolver != null && groupWriter != null) {
                
                ResolvedContent resolvedContent = groupResolver.findByCanonicalUris(ImmutableList.of(CONTENT_GROUP_URI));
                if (resolvedContent.get(CONTENT_GROUP_URI).hasValue()) {
                    contentGroup = (ContentGroup) resolvedContent.get(CONTENT_GROUP_URI).requireValue();
                    contentGroup.setContents(ImmutableList.<ChildRef>of());
                } else {
                    contentGroup = new ContentGroup(CONTENT_GROUP_URI, Publisher.BT_FEATURED_CONTENT);
                }
            }
            
            Document rootDocument = client.get(rootDocumentUri);
            // Returns a Page element, containing nested Products
            
            // Add each product to the BT Featured Content content group
            // If a product is a collection (contains a collection element), treat it as a container and ingest each of the products it contains
            BtFeaturedContentProcessor processor = processor(contentGroup);
            
            Element rootElement = rootDocument.getRootElement();
            evaluateProducts(processor, rootElement, Optional.<Container>absent());
                        
            reportStatus(processor.getResult().toString());

            if (contentGroup != null) {
                groupWriter.createOrUpdate(contentGroup);
            }
        } catch (Exception e) {
            reportStatus(e.getMessage());
            Throwables.propagate(e);
        }
    }

    protected void evaluateProducts(BtFeaturedContentProcessor processor, Element rootElement, Optional<Container> parent)
            throws Exception {
        for (int i = 0; i < rootElement.getChildElements().size(); i++) {
            Element product = rootElement.getChildElements().get(i);

            if (!this.shouldContinue()) return;

            log.info("Reading "+product.getAttributeValue("id"));
            Element hydratedProduct = client.get(productBaseUri+product.getAttributeValue("id")+XML_SUFFIX).getRootElement();
            Optional<Content> item = processor.process(hydratedProduct, parent);
            
            if (item.isPresent() && !(item.get() instanceof Item)) {
                log.info("Product has child "+item.get());
                Document childDoc = client.get(urlForProductWithCurie(item.get().getCurie()));
                
                if (childDoc.getRootElement() instanceof BtFeaturedProductElement) {
                    Element container = ((BtFeaturedProductElement)childDoc.getRootElement()).getContainer();
                    evaluateProducts(processor, container, Optional.of((Container)item.get()));
                }
            }
            
        }
    }

    protected String urlForProductWithCurie(String curie) {
        if (!curie.startsWith(BtFeaturedElementHandler.CURIE_PREFIX)) {
            throw new RuntimeException("Not a recognised curie for BT Featured Content: "+curie);
        }
        return productBaseUri+curie.substring(BtFeaturedElementHandler.CURIE_PREFIX.length())+XML_SUFFIX;
    }

    private BtFeaturedContentProcessor processor(final ContentGroup contentGroup) {
        return new BtFeaturedContentProcessor() {
            UpdateProgress progress = UpdateProgress.START;

            @Override
            public Optional<Content> process(Element element, @Nonnull Optional<Container> parent) {
                Optional<Content> content = Optional.absent();
                
                try {
                    content = handler.handle(element, parent);
                    
                   
                    if (content.isPresent()) {
                        Content content2 = content.get();
                        ResolvedContent resolved = contentResolver.findByCanonicalUris(ImmutableList.of(content2.getCanonicalUri()));
                        
                        if (!resolved.resolved(content2.getCanonicalUri())) {
                            if (content2 instanceof Item) {
                                contentWriter.createOrUpdate((Item)content2);
                            }
                            else {
                                contentWriter.createOrUpdate((Container)content2);
                            }
                        }
                        else {
                            Content resolvedContent = (Content)resolved.get(content2.getCanonicalUri()).requireValue();
                            if (content2 instanceof Item) {
                                contentMerger.merge((Item)resolvedContent, (Item)content2);
                                contentWriter.createOrUpdate((Item)resolvedContent);
                            }
                            else {
                                contentMerger.merge((Container)resolvedContent, (Container)content2);
                                contentWriter.createOrUpdate((Container)resolvedContent);
                            }
                        }
                        
                        if (!parent.isPresent()) {
                            resolved = contentResolver.findByCanonicalUris(ImmutableList.of(content2.getCanonicalUri()));
                            if (content2 instanceof Item) {
                                Item item = (Item)(Content)resolved.get(content.get().getCanonicalUri()).requireValue();
                                contentGroup.addContent(item.childRef());    
                            } else {
                                Container container = (Container)(Content)resolved.get(content.get().getCanonicalUri()).requireValue();
                                contentGroup.addContent(container.childRef());                                    
                            }
                        }

                    }
                    
                    progress = progress.reduce(UpdateProgress.SUCCESS);
                } catch (Exception e) {
                    log.warn(element.getLocalName() , e);
                    progress = progress.reduce(UpdateProgress.FAILURE);
                }
                reportStatus(progress.toString());
                return content;
            }

            @Override
            public UpdateProgress getResult() {
                return progress;
            }
        };
    }
}
