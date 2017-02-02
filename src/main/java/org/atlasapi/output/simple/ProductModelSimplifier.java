package org.atlasapi.output.simple;

import java.math.BigInteger;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductLocation;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class ProductModelSimplifier extends IdentifiedModelSimplifier<Product, org.atlasapi.media.entity.simple.Product> {

    private SubstitutionTableNumberCodec idCodec;
    private final String productUriFormat;

    public ProductModelSimplifier(String localHostName) {
        this.productUriFormat = String.format("http://%s/products/", localHostName);
        this.idCodec = new SubstitutionTableNumberCodec();
    }
    
    @Override
    public org.atlasapi.media.entity.simple.Product simplify(Product model, Set<Annotation> annotations, ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Product simpleProduct = new org.atlasapi.media.entity.simple.Product();
        
        String id = idCodec.encode(BigInteger.valueOf(model.getId()));
        simpleProduct.setId(id);
        simpleProduct.setUri(productUriFormat + id);
        simpleProduct.setType(model.getType() != null ? model.getType().toString() : null);
        simpleProduct.setGtin(model.getGtin());
        simpleProduct.setTitle(model.getTitle());
        simpleProduct.setDescription(model.getDescription());
        simpleProduct.setImage(model.getImage());
        simpleProduct.setThumbnail(model.getThumbnail());
        simpleProduct.setYear(model.getYear());
        simpleProduct.setLocations(simplify(model.getLocations()));
        simpleProduct.setPublisher(toPublisherDetails(model.getPublisher()));
        
        return simpleProduct;
    }

    private Set<org.atlasapi.media.entity.simple.ProductLocation> simplify(Set<ProductLocation> locations) {
        return ImmutableSet.copyOf(Iterables.transform(locations, new Function<ProductLocation, org.atlasapi.media.entity.simple.ProductLocation>() {
            @Override
            public org.atlasapi.media.entity.simple.ProductLocation apply(ProductLocation input) {
                org.atlasapi.media.entity.simple.ProductLocation productLocation = new org.atlasapi.media.entity.simple.ProductLocation();
                productLocation.setUri(input.getUri());
                productLocation.setAvailability(input.getAvailability());
                productLocation.setPrice(input.getPrice() != null ? input.getPrice().toString() : null);
                productLocation.setShippingPrice(input.getShippingPrice() != null ? input.getShippingPrice().toString() : null);
                return productLocation;
            }
        }));
    }

}
