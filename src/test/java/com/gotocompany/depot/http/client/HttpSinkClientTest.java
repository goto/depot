package com.gotocompany.depot.http.client;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.HttpSinkMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.HttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link HttpSinkClient}, the component that dispatches a list of
 * {@link HttpRequestRecord} instances over an Apache {@link HttpClient} and records per-status-code
 * metrics for the responses.
 *
 * <p>The Apache {@link HttpClient} and the {@link Instrumentation} facade are mocked with Mockito,
 * while a real {@link HttpSinkMetrics} is built from an {@link HttpSinkConfig} in {@link #setUp()} so
 * that the actual metric names (including the configured application prefix) are exercised. Each
 * {@link HttpRequestRecord} is itself a mock whose {@code send} method is stubbed to return a
 * corresponding mocked {@link HttpSinkResponse}, isolating the client's fan-out and metric-capture
 * behaviour from real HTTP transport.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpSinkClientTest {

    /**
     * Mocked Apache HttpClient handed to the sink client and forwarded to each record's send call.
     */
    @Mock
    private HttpClient client;

    /**
     * Real metrics holder built from configuration in {@link #setUp()}; supplies the metric names
     * the client emits.
     */
    private HttpSinkMetrics httpSinkMetrics;

    /**
     * Mocked logging and metric facade used to verify the status-code counts captured by the client.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Initialises the metrics fixture before each test.
     *
     * <p>Sets the {@code SINK_METRICS_APPLICATION_PREFIX} system property to {@code xyz_}, builds an
     * {@link HttpSinkConfig} from the current system properties via {@code ConfigFactory}, and
     * constructs the {@link HttpSinkMetrics} used to assert on the emitted metric names.</p>
     *
     * @throws IOException never in practice; declared to mirror the configuration-loading contract
     */
    @Before
    public void setUp() throws IOException {
        System.setProperty("SINK_METRICS_APPLICATION_PREFIX", "xyz_");
        HttpSinkConfig sinkConfig = ConfigFactory.create(HttpSinkConfig.class, System.getProperties());
        httpSinkMetrics = new HttpSinkMetrics(sinkConfig);

    }

    /**
     * Verifies that the client sends every record and returns the corresponding responses in order.
     *
     * <p>Given five mocked records, each stubbed so that {@code send} returns a distinct mocked
     * {@link HttpSinkResponse} with response code {@code 200}, when {@link HttpSinkClient#send(List)}
     * is invoked, then the returned list contains exactly those responses at the same positions.</p>
     *
     * @throws IOException never in practice; declared because the record's send method is checked
     */
    @Test
    public void shouldSendRecords() throws IOException {
        HttpSinkClient sinkClient = new HttpSinkClient(client, httpSinkMetrics, instrumentation);

        List<HttpRequestRecord> requestRecords = new ArrayList<HttpRequestRecord>() {{
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
        }};

        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
        }};

        IntStream.range(0, requestRecords.size()).forEach(
                index -> {
                    try {
                        when(requestRecords.get(index).send(client, instrumentation)).thenReturn(responses.get(index));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        IntStream.range(0, responses.size()).forEach(
                index -> when(responses.get(index).getResponseCode()).thenReturn(200)
        );
        List<HttpSinkResponse> actualResponses = sinkClient.send(requestRecords);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }

    /**
     * Verifies that the client records a per-status-code metric for every response it receives.
     *
     * <p>Given five mocked records whose responses all report status code {@code 200}, when
     * {@link HttpSinkClient#send(List)} is invoked, then {@link Instrumentation#captureCount} is
     * called five times with the prefixed metric {@code xyz_sink_http_response_code_total}, a count of
     * {@code 1} and the tag {@code status_code=200}.</p>
     *
     * @throws IOException never in practice; declared because the record's send method is checked
     */
    @Test
    public void shouldCaptureStatusCodeCount() throws IOException {
        HttpSinkClient sinkClient = new HttpSinkClient(client, httpSinkMetrics, instrumentation);

        List<HttpRequestRecord> requestRecords = new ArrayList<HttpRequestRecord>() {{
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
        }};

        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
        }};

        IntStream.range(0, requestRecords.size()).forEach(
                index -> {
                    try {
                        when(requestRecords.get(index).send(client, instrumentation)).thenReturn(responses.get(index));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        IntStream.range(0, responses.size()).forEach(
                index -> when(responses.get(index).getResponseCode()).thenReturn(200)
        );
        sinkClient.send(requestRecords);
        verify(instrumentation, times(5)).captureCount("xyz_sink_http_response_code_total", 1L, "status_code=200");

    }
}
