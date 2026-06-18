package com.gotocompany.depot.redis.ttl;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;
/**
 * Unit tests for {@link RedisTTLFactory}, the factory that selects a {@link RedisTtl} implementation
 * based on the configured {@link RedisSinkTtlType}.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link RedisSinkConfig} whose TTL
 * type and value are stubbed per scenario, and rely on an {@link ExpectedException} rule to assert
 * the {@link ConfigurationException} raised for an invalid TTL value.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisTtlFactoryTest {

    /**
     * Mocked sink configuration whose TTL type and value are stubbed for each scenario.
     */
    @Mock
    private RedisSinkConfig redisSinkConfig;

    /**
     * JUnit rule used to assert the type and message of the {@link ConfigurationException} expected
     * for an invalid TTL configuration.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Establishes the default fixture by stubbing the TTL type to {@link RedisSinkTtlType#DISABLE},
     * so the baseline scenario produces a no-op TTL unless a test overrides it.
     */
    @Before
    public void setup() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DISABLE);
    }

    /**
     * Verifies that a disabled TTL configuration yields a {@link NoRedisTtl}.
     *
     * <p>Given the default fixture where the TTL type is {@link RedisSinkTtlType#DISABLE}, when
     * {@link RedisTTLFactory#getTTl} is called, then the returned {@link RedisTtl} is a
     * {@link NoRedisTtl}.</p>
     */
    @Test
    public void shouldReturnNoTTLIfNothingGiven() {
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), NoRedisTtl.class);
    }

    /**
     * Verifies that an exact-time TTL configuration yields an {@link ExactTimeTtl}.
     *
     * <p>Given the TTL type stubbed to {@link RedisSinkTtlType#EXACT_TIME} with a value of
     * {@code 100}, when {@link RedisTTLFactory#getTTl} is called, then the returned {@link RedisTtl}
     * is an {@link ExactTimeTtl}.</p>
     */
    @Test
    public void shouldReturnExactTimeTTL() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.EXACT_TIME);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(100L);
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), ExactTimeTtl.class);
    }

    /**
     * Verifies that a duration TTL configuration yields a {@link DurationTtl}.
     *
     * <p>Given the TTL type stubbed to {@link RedisSinkTtlType#DURATION} with a value of {@code 100},
     * when {@link RedisTTLFactory#getTTl} is called, then the returned {@link RedisTtl} is a
     * {@link DurationTtl}.</p>
     */
    @Test
    public void shouldReturnDurationTTL() {
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(100L);
        RedisTtl redisTTL = RedisTTLFactory.getTTl(redisSinkConfig);
        Assert.assertEquals(redisTTL.getClass(), DurationTtl.class);
    }

    /**
     * Verifies that a negative TTL value is rejected.
     *
     * <p>Given a {@link RedisSinkTtlType#DURATION} configuration with a value of {@code -1}, when
     * {@link RedisTTLFactory#getTTl} is called, then a {@link ConfigurationException} carrying the
     * message {@code "Provide a positive TTL value"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionInCaseOfInvalidConfiguration() {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Provide a positive TTL value");
        when(redisSinkConfig.getSinkRedisTtlValue()).thenReturn(-1L);
        when(redisSinkConfig.getSinkRedisTtlType()).thenReturn(RedisSinkTtlType.DURATION);
        RedisTTLFactory.getTTl(redisSinkConfig);
    }
}
