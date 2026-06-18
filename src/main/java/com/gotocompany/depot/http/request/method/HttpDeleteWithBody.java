package com.gotocompany.depot.http.request.method;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;

/**
 * A {@code DELETE} HTTP request that is allowed to carry a request body.
 *
 * <p>The standard Apache HttpComponents {@link org.apache.http.client.methods.HttpDelete} extends a
 * base that cannot enclose an entity, so it cannot send a body. This class instead extends
 * {@link HttpEntityEnclosingRequestBase} and reports the {@code DELETE} method name, enabling the HTTP
 * sink to issue delete requests with a payload when {@code SINK_HTTPV2_DELETE_BODY_ENABLE} is set. It
 * is instantiated by {@link com.gotocompany.depot.http.request.RequestMethodFactory} for the
 * {@link com.gotocompany.depot.http.enums.HttpRequestMethodType#DELETE} method.</p>
 *
 * @see com.gotocompany.depot.http.request.RequestMethodFactory
 * @see com.gotocompany.depot.http.enums.HttpRequestMethodType#DELETE
 */
public class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
    /**
     * The HTTP method name reported by this request, namely {@code "DELETE"}.
     */
    public static final String METHOD_NAME = "DELETE";

    /**
     * Returns the HTTP method name of this request.
     *
     * @return the constant {@link #METHOD_NAME}, {@code "DELETE"}
     */
    @Override
    public String getMethod() {
        return METHOD_NAME;
    }

    /**
     * Creates a body-capable {@code DELETE} request targeting the given URI.
     *
     * @param uri the target URI of the delete request
     */
    public HttpDeleteWithBody(final URI uri) {
        super();
        setURI(uri);
    }
}
