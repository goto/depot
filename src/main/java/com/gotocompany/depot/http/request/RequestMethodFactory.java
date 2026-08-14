package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.request.method.HttpDeleteWithBody;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.net.URI;

/**
 * Factory that maps a {@link HttpRequestMethodType} onto a concrete, body-capable Apache
 * HttpComponents request object.
 *
 * <p>All supported verbs are realized as
 * {@link org.apache.http.client.methods.HttpEntityEnclosingRequestBase} subclasses so that a request
 * body may be attached regardless of method. {@code DELETE} is served by the custom
 * {@link HttpDeleteWithBody} because the standard {@link org.apache.http.client.methods.HttpDelete}
 * cannot carry an entity.</p>
 *
 * @see HttpRequestMethodType
 * @see HttpDeleteWithBody
 */
public class RequestMethodFactory {

    /**
     * Creates an HTTP request object for the given URI and method.
     *
     * <p>{@link HttpRequestMethodType#POST}, {@link HttpRequestMethodType#PATCH} and
     * {@link HttpRequestMethodType#DELETE} map to {@link org.apache.http.client.methods.HttpPost},
     * {@link org.apache.http.client.methods.HttpPatch} and {@link HttpDeleteWithBody} respectively;
     * any other value (notably {@link HttpRequestMethodType#PUT}) maps to
     * {@link org.apache.http.client.methods.HttpPut}.</p>
     *
     * @param uri the target URI for the request
     * @param method the HTTP method to use
     * @return a new entity-enclosing request configured for {@code uri}
     */
    public static HttpEntityEnclosingRequestBase create(URI uri, HttpRequestMethodType method) {
        switch (method) {
            case POST:
                return new HttpPost(uri);
            case PATCH:
                return new HttpPatch(uri);
            case DELETE:
                return new HttpDeleteWithBody(uri);
            default:
                return new HttpPut(uri);
        }
    }
}
