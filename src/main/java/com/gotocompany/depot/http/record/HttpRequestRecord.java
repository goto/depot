package com.gotocompany.depot.http.record;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class HttpRequestRecord implements Iterable<Integer> {

    private final Set<Integer> recordIndexes = new HashSet<>();
    private final ErrorInfo errorInfo;
    private final boolean valid;
    private final HttpEntityEnclosingRequestBase httpRequest;


    public HttpRequestRecord(ErrorInfo errorInfo, boolean valid, HttpEntityEnclosingRequestBase httpRequest) {
        this.errorInfo = errorInfo;
        this.valid = valid;
        this.httpRequest = httpRequest;
    }

    public HttpRequestRecord(HttpEntityEnclosingRequestBase httpRequest) {
        this(null, true, httpRequest);
    }

    public HttpRequestRecord(ErrorInfo errorInfo) {
        this(errorInfo, false, null);
    }


    public HttpSinkResponse send(HttpClient httpClient, Instrumentation instrumentation) throws IOException {
        HttpResponse response = httpClient.execute(httpRequest);
        return new HttpSinkResponse(response, instrumentation);
    }

    public String getRequestBody() throws IOException {
        return EntityUtils.toString(httpRequest.getEntity());
    }

    public void addIndex(Integer index) {
        recordIndexes.add(index);
    }

    public void addAllIndexes(Set<Integer> indexes) {
        recordIndexes.addAll(indexes);
    }

    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    public boolean isValid() {
        return valid;
    }

    public String getRequestString() throws IOException {
        StringBuilder requestString = new StringBuilder()
                .append("\nRequest Method: ").append(httpRequest.getMethod())
                .append("\nRequest Url: ").append(httpRequest.getURI())
                .append("\nRequest Headers: ").append(Arrays.asList(httpRequest.getAllHeaders()));
        if (httpRequest.getEntity() != null) {
            requestString.append("\nRequest Body: ").append(getRequestBody());
        }
        return requestString.toString();
    }

    @NotNull
    @Override
    public Iterator<Integer> iterator() {
        return recordIndexes.iterator();
    }
}
