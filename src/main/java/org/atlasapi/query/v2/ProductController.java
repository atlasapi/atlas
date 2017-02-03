package org.atlasapi.query.v2;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.QueryResult;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class ProductController extends BaseController<Iterable<Product>> {
    
    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException()).withErrorCode("PRODUCT_NOT_FOUND").withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException()).withErrorCode("PRODUCT_UNAVAILABLE").withStatusCode(HttpStatusCode.FORBIDDEN);

    private final ProductResolver productResolver;
    private final KnownTypeQueryExecutor queryExecutor;
    private final QueryController queryController;

    private final NumberToShortStringCodec idCodec = new SubstitutionTableNumberCodec();

    public ProductController(
            ProductResolver productResolver,
            KnownTypeQueryExecutor queryExecutor,
            ApplicationFetcher configFetcher,
            AdapterLog log,
            AtlasModelWriter<? super Iterable<Product>> outputter,
            QueryController queryController
    ) {
        super(configFetcher, log, outputter, DefaultApplication.createDefault());
        this.productResolver = productResolver;
        this.queryExecutor = queryExecutor;
        this.queryController = queryController;
    }

    @RequestMapping(value={"3.0/products.*","/products.*"})
    public void products(HttpServletRequest req, HttpServletResponse resp) throws IOException  {
        try {
            final ContentQuery query = buildQuery(req);
            modelAndViewFor(
                    req,
                    resp,
                    StreamSupport.stream(productResolver.products().spliterator(), false)
                            .filter(input -> query.allowsSource(input.getPublisher()))
                            .collect(Collectors.toList()),
                    query.getApplication()
            );
        } catch (Exception e) {
            errorViewFor(req, resp, AtlasErrorSummary.forException(e));
        }
    }
    
    @RequestMapping(value={"3.0/products/{id}.*","/products/{id}.*"})
    public void topic(
            HttpServletRequest req,
            HttpServletResponse resp,
            @PathVariable("id") String id
    ) throws IOException {
        
        ContentQuery query;
        try {
            query = buildQuery(req);
        } catch (InvalidApiKeyException e) {
            outputter.writeError(req, resp, AtlasErrorSummary.forException(e));
            return;
        }
        
        Optional<Product> productForId = productResolver.productForId(
                idCodec.decode(id).longValue()
        );
        
        if(!productForId.isPresent()) {
            outputter.writeError(req, resp, NOT_FOUND.withMessage("Product " + id + " not found"));
            return;
        }
        
        Product product = productForId.get();
        
        if(!query.allowsSource(product.getPublisher())) {
            outputter.writeError(req, resp, FORBIDDEN.withMessage("Product " + id + " unavailable"));
            return;
        }
        
        modelAndViewFor(req, resp, ImmutableSet.of(product), query.getApplication());
    }
    
    @RequestMapping(value={"3.0/products/{id}/content.*", "/products/{id}/content"})
    public void topicContents(
            HttpServletRequest req,
            HttpServletResponse resp,
            @PathVariable("id") String id
    ) throws IOException {
        ContentQuery query;
        try {
            query = buildQuery(req);
        } catch (InvalidApiKeyException e) {
            outputter.writeError(req, resp, AtlasErrorSummary.forException(e));
            return;
        }

        long decodedId = idCodec.decode(id).longValue();
        Optional<Product> productForId = productResolver.productForId(decodedId);
        
        if(!productForId.isPresent()) {
            outputter.writeError(req, resp, NOT_FOUND.withMessage("Product " + id + " not found"));
            return;
        }
        
        Product product = productForId.get();
        
        if(!query.allowsSource(product.getPublisher())) {
            outputter.writeError(req, resp, FORBIDDEN.withMessage("Product " + id + " unavailable"));
            return;
        }
        
        try {
            Selection selection = query.getSelection();
            QueryResult<Identified, Product> result = QueryResult.of(
                    Iterables.filter(
                            queryExecutor.executeUriQuery(
                                    query.getSelection().apply(product.getContent()), query
                            ).values(),
                            Identified.class
                    ),
                    product);

            queryController.modelAndViewFor(
                    req,
                    resp,
                    result.withSelection(selection),
                    query.getApplication()
            );
        } catch (Exception e) {
            errorViewFor(req, resp, AtlasErrorSummary.forException(e));
        }
    }
}
