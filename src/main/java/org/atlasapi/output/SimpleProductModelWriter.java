package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.ProductQueryResult;
import org.atlasapi.media.product.Product;
import org.atlasapi.output.simple.ProductModelSimplifier;
import org.atlasapi.persistence.content.ContentResolver;

public class SimpleProductModelWriter extends TransformingModelWriter<Iterable<Product>, ProductQueryResult> {

    private final ProductModelSimplifier modelSimplifier;

    public SimpleProductModelWriter(AtlasModelWriter<ProductQueryResult> delegate, ContentResolver contentResolver, ProductModelSimplifier modelSimplifier) {
        super(delegate);
        this.modelSimplifier = modelSimplifier;
    }

    @Override
    protected ProductQueryResult transform(Iterable<Product> model, Set<Annotation> annotations, final ApplicationConfiguration config) {
        ProductQueryResult result = new ProductQueryResult();
        for (Product product : model) {
            result.add(modelSimplifier.simplify(product, annotations, config));
        }
        return result;
    }

}
