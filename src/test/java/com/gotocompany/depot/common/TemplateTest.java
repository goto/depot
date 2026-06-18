package com.gotocompany.depot.common;

import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import com.gotocompany.stencil.Parser;
import com.gotocompany.stencil.StencilClientFactory;
import com.jayway.jsonpath.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link Template}, Depot's comma-separated message-interpolation template.
 *
 * <p>The suite exercises three concerns: compiling constant and placeholder-bearing templates,
 * rendering them against parsed Protobuf messages through {@link Template#parse(ParsedMessage)}, and
 * rejecting malformed templates with an {@link InvalidTemplateException}. It also covers escaping of
 * literal commas written as {@code /,/}, which must not be treated as the separator between the
 * format pattern and the field names that follow it.
 *
 * <p>The class runs with the JUnit {@link Parameterized} runner. Only
 * {@link #testCommaParsingInTemplate()} consumes the constructor-injected {@code templateString} and
 * {@code expectedOutput} values supplied by {@link #data()}; every other {@code @Test} method is
 * parameter-independent and is therefore re-executed, with identical behavior, once per parameter
 * row.
 *
 * <p>{@link #setUp()} prepares two {@link ProtoParsedMessage} fixtures — one wrapping a
 * {@link TestMessage} and one wrapping a {@link TestBookingLogMessage} — using Stencil
 * {@link Parser} instances and a JSON-path {@link Configuration} backed by a
 * {@link ProtoJsonProvider}.
 */
@RunWith(Parameterized.class)
public class TemplateTest {
    /**
     * Parsed Protobuf fixture wrapping a {@link TestMessage}, exercised by the string and
     * multi-variable rendering tests.
     */
    private ParsedMessage parsedTestMessage;
    /**
     * Parsed Protobuf fixture wrapping a {@link TestBookingLogMessage}, exercised by the numeric and
     * complex-object rendering tests.
     */
    private ParsedMessage parsedBookingMessage;
    /**
     * Mock sink configuration handed to the {@link ProtoJsonProvider}; its default-field-value flag is
     * stubbed to {@code false} in {@link #setUp()}.
     */
    @Mock
    private SinkConfig sinkConfig;

    /**
     * Raw template string for the parameterized comma-parsing row, injected by the constructor from
     * {@link #data()}.
     */
    private final String templateString;
    /**
     * Compiled pattern expected for {@code templateString} in the parameterized comma-parsing row.
     */
    private final String expectedOutput;

    /**
     * Creates a parameterized test instance for a single comma-parsing row.
     *
     * @param templateString the raw template whose compiled pattern is under test
     * @param expectedOutput the pattern expected from {@link Template#getTemplateString()} once the
     *                       template is compiled
     */
    public TemplateTest(String templateString, String expectedOutput) {
        this.templateString = templateString;
        this.expectedOutput = expectedOutput;
    }


    /**
     * Initializes the parsed-message fixtures before each test.
     *
     * <p>Stubs {@link SinkConfig#getSinkDefaultFieldValueEnable()} to return {@code false}, builds a
     * JSON-path {@link Configuration} backed by a {@link ProtoJsonProvider}, and parses a
     * {@link TestMessage} and a {@link TestBookingLogMessage} into {@link ProtoParsedMessage}
     * instances using Stencil parsers obtained from {@link StencilClientFactory}.
     *
     * @throws Exception if a Stencil parser cannot be created or a message payload cannot be parsed
     */
    @Before
    public void setUp() throws Exception {
        sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkDefaultFieldValueEnable()).thenReturn(false);
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setOrderNumber("booking-order-1")
                .setCustomerTotalFareWithoutSurge(2000L)
                .setAmountPaidByCash(12.3F)
                .setDriverPickupLocation(TestLocation.newBuilder().setLongitude(10.0).setLatitude(23.9).build())
                .build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        Message message = new Message(testKey.toByteArray(), testMessage.toByteArray());
        Message bookingMessage = new Message(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        Parser protoParserTest = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        parsedTestMessage = new ProtoParsedMessage(protoParserTest.parse((byte[]) message.getLogMessage()), jsonPathConfig);
        Parser protoParserBooking = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        parsedBookingMessage = new ProtoParsedMessage(protoParserBooking.parse((byte[]) bookingMessage.getLogMessage()), jsonPathConfig);
    }


    /**
     * Verifies that a single {@code %s} placeholder is replaced with a string field's value.
     *
     * <p>Compiles {@code "Test-%s,order_number"} and renders it against the {@link TestMessage}
     * fixture, asserting the {@code order_number} value {@code "test-order"} is substituted to produce
     * {@code "Test-test-order"}.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage));
    }

    /**
     * Verifies that surrounding whitespace in a field-name segment is trimmed during compilation.
     *
     * <p>Compiles {@code "Test-%s, order_number"} — note the space before {@code order_number} — and
     * asserts it renders identically to the un-spaced form, yielding {@code "Test-test-order"} for the
     * {@link TestMessage} fixture.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s, order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage));
    }

    /**
     * Verifies that a {@code float} field is rendered into a {@code %s} placeholder via its string
     * form.
     *
     * <p>Compiles {@code "Test-%s,amount_paid_by_cash"} and renders it against the
     * {@link TestBookingLogMessage} fixture, asserting the {@code 12.3} cash amount produces
     * {@code "Test-12.3"}.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,amount_paid_by_cash");
        assertEquals("Test-12.3", template.parse(parsedBookingMessage));
    }

    /**
     * Verifies that a {@code long} field is rendered into a {@code %s} placeholder via its string form.
     *
     * <p>Compiles {@code "Test-%s,customer_total_fare_without_surge"} and renders it against the
     * {@link TestBookingLogMessage} fixture, asserting the {@code 2000} fare value produces
     * {@code "Test-2000"}.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,customer_total_fare_without_surge");
        assertEquals("Test-2000", template.parse(parsedBookingMessage));
    }

    /**
     * Verifies that a {@code null} template is rejected at construction.
     *
     * <p>Asserts that {@code new Template(null)} throws an {@link InvalidTemplateException} whose
     * message is {@code "Template cannot be empty"}.
     */
    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() {
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class, () -> new Template(null));
        assertEquals("Template cannot be empty", e.getMessage());
    }

    /**
     * Verifies that an empty-string template is rejected at construction.
     *
     * <p>Asserts that {@code new Template("")} throws an {@link InvalidTemplateException} whose message
     * is {@code "Template cannot be empty"}.
     */
    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() {
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class, () -> new Template(""));
        assertEquals("Template cannot be empty", e.getMessage());
    }

    /**
     * Verifies that a constant template without placeholders renders to itself.
     *
     * <p>Compiles the constant template {@code "Test"} and asserts that rendering it against the
     * {@link TestBookingLogMessage} fixture returns the unchanged literal {@code "Test"}.
     *
     * @throws InvalidTemplateException if the constant template is unexpectedly rejected
     */
    @Test
    public void shouldAcceptStringForCollectionKey() throws InvalidTemplateException {
        Template template = new Template("Test");
        assertEquals("Test", template.parse(parsedBookingMessage));
    }

    /**
     * Verifies that mismatches between placeholders and field names are rejected with a descriptive
     * message.
     *
     * <p>Asserts two failures: {@code "Test-%s%d%b,t1,t2"} declares three placeholders but supplies
     * only two field names ({@code variables=3, validArgs=3, values=2}), and
     * {@code "Test-%s%s%y,order_number,order_details"} contains the invalid conversion {@code %y}
     * ({@code variables=3, validArgs=2, values=2}); each raises an {@link InvalidTemplateException}
     * carrying the corresponding {@code "Template is not valid, ..."} message.
     */
    @Test
    public void shouldNotAcceptStringWithPatternForCollectionKeyWithEmptyVariables() {
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class, () -> new Template("Test-%s%d%b,t1,t2"));
        Assert.assertEquals("Template is not valid, variables=3, validArgs=3, values=2", e.getMessage());

        e = assertThrows(InvalidTemplateException.class, () -> new Template("Test-%s%s%y,order_number,order_details"));
        Assert.assertEquals("Template is not valid, variables=3, validArgs=2, values=2", e.getMessage());
    }

    /**
     * Verifies that a pattern with multiple placeholders is rendered from multiple fields.
     *
     * <p>Compiles {@code "Test-%s::%s, order_number, order_details"} and renders it against the
     * {@link TestMessage} fixture, asserting the {@code order_number} and {@code order_details} values
     * are substituted in order to produce {@code "Test-test-order::ORDER-DETAILS"}.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithMultipleVariables() throws InvalidTemplateException {
        Template template = new Template("Test-%s::%s, order_number, order_details");
        assertEquals("Test-test-order::ORDER-DETAILS", template.parse(parsedTestMessage));
    }

    /**
     * Verifies that a message-typed field renders as its JSON representation.
     *
     * <p>Compiles {@code "%s,driver_pickup_location"} and renders it against the
     * {@link TestBookingLogMessage} fixture, using {@link JSONAssert} to assert under a strict
     * comparison that the nested location message serializes to a JSON object whose {@code latitude}
     * is {@code 23.9} and {@code longitude} is {@code 10.0}.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldParseComplexObject() throws InvalidTemplateException {
        Template template = new Template("%s,driver_pickup_location");
        String expectedLocation = "{\"latitude\":23.9,\"longitude\":10.0}";
        JSONAssert.assertEquals(expectedLocation, template.parse(parsedBookingMessage), true);
    }

    /**
     * Verifies that a constant template exposes its full text as the template string.
     *
     * <p>Compiles {@code "http://dummy.com"} and asserts {@link Template#getTemplateString()} returns
     * the same constant value, confirming that no placeholder extraction occurs.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldGetTemplateStringFromConstantString() throws InvalidTemplateException {
        Template template = new Template("http://dummy.com");
        assertEquals("http://dummy.com", template.getTemplateString());
    }

    /**
     * Verifies that a placeholder-bearing template exposes only its format pattern.
     *
     * <p>Compiles {@code "http://dummy.com/%s,order_number"} and asserts
     * {@link Template#getTemplateString()} returns just the pattern {@code "http://dummy.com/%s"},
     * with the trailing field-name segment stripped away.
     *
     * @throws InvalidTemplateException if the template is unexpectedly rejected during compilation
     */
    @Test
    public void shouldGetTemplatePatternFromParameterizedString() throws InvalidTemplateException {
        Template template = new Template("http://dummy.com/%s,order_number");
        assertEquals("http://dummy.com/%s", template.getTemplateString());
    }

    /**
     * Supplies the parameter rows for the comma-escaping cases driven by
     * {@link #testCommaParsingInTemplate()}.
     *
     * <p>Each row pairs a raw template string with the format pattern expected after compilation,
     * covering escaped separators ({@code /,/} unescaped to a literal comma), doubled slashes, and
     * templates that mix escaped commas with trailing {@code %s} placeholders.
     *
     * @return the parameter rows, each a {@code templateString} paired with its {@code expectedOutput}
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"eeeee/,/2222", "eeeee,2222"},
                {"eeeee/,/2222/,/eeeee/,/2222", "eeeee,2222,eeeee,2222"},
                {"/,/", ","},
                {"ff//,//ss", "ff/,/ss"},
                {"%s/,/%s,order_number,driver_pickup_location", "%s,%s"},
                {"eeeee/,/2222/,/eeeee/,/2222%s,order_number", "eeeee,2222,eeeee,2222%s"}
        });
    }

    /**
     * Verifies that escaped commas are correctly unescaped when compiling a template pattern.
     *
     * <p>Compiles the parameterized {@code templateString} and asserts
     * {@link Template#getTemplateString()} equals the expected {@code expectedOutput}, confirming that
     * {@code /,/} sequences become literal commas while genuine separators are honored.
     *
     * @throws InvalidTemplateException if the parameterized template is unexpectedly rejected
     */
    @Test
    public void testCommaParsingInTemplate() throws InvalidTemplateException {

        Template template = new Template(templateString);
        assertEquals(expectedOutput, template.getTemplateString());
    }


    /**
     * Verifies that a template with a malformed doubled-comma escape is rejected.
     *
     * <p>Asserts that {@code "eee/,,/222%s,order_number"} raises an {@link InvalidTemplateException}
     * reporting {@code variables=0, validArgs=0, values=2}: the doubled comma is split as a separator,
     * leaving a placeholder-free first segment while two field-name segments remain.
     */
    @Test
    public void shouldThrowExceptionIfMultipleCommasEnclosedInQuotes() {
        Exception exception = assertThrows(InvalidTemplateException.class, () -> new Template("eee/,,/222%s,order_number"));
        assertEquals("Template is not valid, variables=0, validArgs=0, values=2", exception.getMessage());
    }


    /**
     * Verifies that an unescaped comma inside the pattern segment is rejected when field names follow.
     *
     * <p>Asserts that {@code "ss,%s,order_number"} raises an {@link InvalidTemplateException}
     * reporting {@code variables=0, validArgs=0, values=2}: the first unescaped comma ends the pattern
     * at {@code "ss"}, which has no placeholders, while two field-name segments remain.
     */
    @Test
    public void shouldThrowExceptionIfCommaNotEnclosedWithArguments() {
        Exception exception = assertThrows(InvalidTemplateException.class, () -> new Template("ss,%s,order_number"));
        assertEquals("Template is not valid, variables=0, validArgs=0, values=2", exception.getMessage());
    }

    /**
     * Verifies that an unescaped comma is rejected even when no placeholders are present.
     *
     * <p>Asserts that {@code "ss,dd"} raises an {@link InvalidTemplateException} reporting
     * {@code variables=0, validArgs=0, values=1}: {@code "ss"} becomes a placeholder-free pattern and
     * {@code "dd"} is treated as a single, unmatched field-name segment.
     */
    @Test
    public void shouldThrowExceptionIfCommaNotEnclosedWithoutArguments() {
        Exception exception = assertThrows(InvalidTemplateException.class, () -> new Template("ss,dd"));
        assertEquals("Template is not valid, variables=0, validArgs=0, values=1", exception.getMessage());
    }

    /**
     * Verifies that escaping the only comma leaves a placeholder with no field name to satisfy it.
     *
     * <p>Asserts that {@code "%s/,/order_number"} raises an {@link InvalidTemplateException} reporting
     * {@code variables=1, validArgs=1, values=0}: the escaped {@code /,/} keeps the whole input as a
     * single pattern (unescaped to {@code "%s,order_number"}) that declares one placeholder but
     * supplies no field-name segment.
     */
    @Test
    public void shouldThrowExceptionIfCommaEnclosedOutsideTemplateString() {
        Exception exception = assertThrows(InvalidTemplateException.class, () -> new Template("%s/,/order_number"));
        assertEquals("Template is not valid, variables=1, validArgs=1, values=0", exception.getMessage());
    }
}
