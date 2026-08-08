package com.antontech.itemkafka_poc.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Enables Cross-Origin Resource Sharing (CORS) so that a browser-based front
 * end served from a different origin (e.g. the React UI running on
 * {@code http://localhost:5173} during local development) is allowed to call
 * this API directly with {@code fetch}/{@code axios}, instead of every
 * request being blocked by the browser's same-origin policy.
 * <p>
 * Without this, the browser would send a CORS pre-flight {@code OPTIONS}
 * request ahead of every non-trivial call and, seeing no
 * {@code Access-Control-Allow-Origin} response header, refuse to hand the
 * actual response back to the calling JavaScript - even though the server
 * technically processed the request fine. See {@code ARCHITECTURE.md},
 * section "CORS: How the React Front End Is Allowed to Call This API" for a
 * full explanation of the browser-side mechanics.
 * <p>
 * Allowed origins are externalised via {@code cors.allowed-origins}
 * (comma-separated) / the {@code ITEM_CORS_ALLOWED_ORIGINS} environment
 * variable so that production deployments can restrict this to the real
 * front-end domain instead of leaving local dev ports open.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${cors.allowed-origins:http://localhost:5173,http://localhost:3000}")
    private String[] allowedOrigins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins(allowedOrigins)
                .allowedMethods("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS")
                .allowedHeaders("*")
                .exposedHeaders("Authorization")
                .allowCredentials(false)
                .maxAge(3600);
    }

    /**
     * Some Spring Security / filter-chain based setups ignore
     * {@link WebMvcConfigurer#addCorsMappings} entirely, so a
     * {@code CorsFilter}-compatible {@link org.springframework.web.cors.CorsConfigurationSource}
     * bean is also provided for defence in depth. Harmless (and simply
     * unused) if no security filter chain is present.
     */
    @Bean
    public org.springframework.web.cors.CorsConfigurationSource corsConfigurationSource() {
        org.springframework.web.cors.CorsConfiguration configuration = new org.springframework.web.cors.CorsConfiguration();
        configuration.setAllowedOrigins(java.util.Arrays.asList(allowedOrigins));
        configuration.setAllowedMethods(java.util.Arrays.asList("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"));
        configuration.setAllowedHeaders(java.util.List.of("*"));
        configuration.setExposedHeaders(java.util.List.of("Authorization"));
        configuration.setMaxAge(3600L);
        org.springframework.web.cors.UrlBasedCorsConfigurationSource source = new org.springframework.web.cors.UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
}

