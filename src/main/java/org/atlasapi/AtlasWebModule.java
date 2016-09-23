package org.atlasapi;

import java.io.IOException;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;
import com.github.jknack.handlebars.io.ClassPathTemplateLoader;
import com.github.jknack.handlebars.io.TemplateLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.accept.ContentNegotiationManager;
import org.springframework.web.accept.ContentNegotiationStrategy;
import org.springframework.web.accept.MediaTypeFileExtensionResolver;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.ContentNegotiatingViewResolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.webapp.json.JsonView;
import com.metabroadcast.common.webapp.soy.SoyTemplateRenderer;
import com.metabroadcast.common.webapp.soy.SoyTemplateViewResolver;

@Configuration
@EnableWebMvc
public class AtlasWebModule extends WebMvcConfigurerAdapter {

    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer.ignoreAcceptHeader(true).defaultContentType(MediaType.TEXT_HTML);
    }

    public @Bean ViewResolver viewResolver(ContentNegotiationManager manager) {
        ContentNegotiatingViewResolver resolver = new ContentNegotiatingViewResolver();

        resolver.setContentNegotiationManager(manager);

        resolver.setMediaTypes(ImmutableMap.of("json", MimeType.APPLICATION_JSON.toString()));

        resolver.setFavorPathExtension(true);
        resolver.setIgnoreAcceptHeader(true);
        resolver.setDefaultContentType(MediaType.TEXT_HTML);
        resolver.setDefaultViews(ImmutableList.<View> of(new JsonView()));

        SoyTemplateViewResolver soyResolver = new SoyTemplateViewResolver(soyRenderer());

        soyResolver.setNamespace("atlas.templates");
        resolver.setViewResolvers(ImmutableList.<ViewResolver> of(soyResolver));
        return resolver;
    }

    public @Bean SoyTemplateRenderer soyRenderer() {
        SoyTemplateRenderer renderer = new SoyTemplateRenderer();
        renderer.setPrefix("/src/main/webapp/WEB-INF/templates/");
        renderer.setSuffix(".soy");
        renderer.setResourceBase(System.getProperty("user.dir"));
        return renderer;
    }

//    private final Logger log = LoggerFactory.getLogger(AtlasWebModule.class);
//
//	public @Bean ViewResolver viewResolver() {
//	    TemplateLoader loader = new ClassPathTemplateLoader("/WEB-INF/templates/", ".hbs");
//        Handlebars handlebars = new Handlebars(loader);
//
//        String template = "equiv";
//        try {
//            handlebars.compile(template);
//        } catch (IOException e) {
//            log.error("Couldn't get template for {}", template);
//        }
//
//        ContentNegotiatingViewResolver resolver = new ContentNegotiatingViewResolver();
//
//        resolver.setMediaTypes(ImmutableMap.of("json", MimeType.APPLICATION_JSON.toString()));
//
//        resolver.setFavorPathExtension(true);
//        resolver.setIgnoreAcceptHeader(true);
//        resolver.setDefaultContentType(MediaType.TEXT_HTML);
//        resolver.setDefaultViews(ImmutableList.<View> of(new JsonView()));
//
////        SoyTemplateViewResolver soyResolver = new SoyTemplateViewResolver(soyRenderer());
////
////        soyResolver.setNamespace("atlas.templates");
//
//        resolver.setViewResolvers(ImmutableList.<ViewResolver> of(soyResolver));
//        return resolver;
//    }
//
////    public @Bean SoyTemplateRenderer soyRenderer() {
//////        SoyTemplateRenderer renderer = new SoyTemplateRenderer();
//////        renderer.setPrefix("/WEB-INF/templates/");
//////        renderer.setSuffix(".soy");
////        return renderer;
////    }
}
