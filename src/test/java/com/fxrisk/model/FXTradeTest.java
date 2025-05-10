package com.fxrisk.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FXTradeTest {

    @Test
    @DisplayName("Constructor should create a valid FXTrade instance with correct properties")
    void constructorShouldCreateValidInstance() {
        // Arrange
        String id = "TR12345";
        String currencyPair = "EUR/USD";
        BigDecimal amount = new BigDecimal("1000000");
        String direction = "BUY";
        String counterparty = "BANK_A";
        boolean settlementRiskCovered = true;
        
        // Act
        FXTrade trade = new FXTrade(id, currencyPair, amount, direction, counterparty, settlementRiskCovered);
        
        // Assert
        assertEquals(id, trade.getId());
        assertEquals(currencyPair, trade.getCurrencyPair());
        assertEquals(amount, trade.getAmount());
        assertEquals(direction, trade.getDirection());
        assertEquals(counterparty, trade.getCounterparty());
        assertEquals(settlementRiskCovered, trade.isSettlementRiskCovered());
    }
    
    @Test
    @DisplayName("Constructor should throw NullPointerException when required fields are null")
    void constructorShouldThrowExceptionForNullFields() {
        // Valid values for test
        String id = "TR12345";
        String currencyPair = "EUR/USD";
        BigDecimal amount = new BigDecimal("1000000");
        String direction = "BUY";
        String counterparty = "BANK_A";
        boolean settlementRiskCovered = true;
        
        // Assert null checks
        assertThrows(NullPointerException.class, () -> 
            new FXTrade(null, currencyPair, amount, direction, counterparty, settlementRiskCovered));
            
        assertThrows(NullPointerException.class, () -> 
            new FXTrade(id, null, amount, direction, counterparty, settlementRiskCovered));
            
        assertThrows(NullPointerException.class, () -> 
            new FXTrade(id, currencyPair, null, direction, counterparty, settlementRiskCovered));
            
        assertThrows(NullPointerException.class, () -> 
            new FXTrade(id, currencyPair, amount, null, counterparty, settlementRiskCovered));
            
        assertThrows(NullPointerException.class, () -> 
            new FXTrade(id, currencyPair, amount, direction, null, settlementRiskCovered));
    }
    
    @Test
    @DisplayName("Constructor should throw IllegalArgumentException for non-positive amounts")
    void constructorShouldThrowExceptionForNonPositiveAmount() {
        // Arrange
        String id = "TR12345";
        String currencyPair = "EUR/USD";
        String direction = "BUY";
        String counterparty = "BANK_A";
        boolean settlementRiskCovered = true;
        
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> 
            new FXTrade(id, currencyPair, BigDecimal.ZERO, direction, counterparty, settlementRiskCovered));
            
        assertThrows(IllegalArgumentException.class, () -> 
            new FXTrade(id, currencyPair, new BigDecimal("-1"), direction, counterparty, settlementRiskCovered));
    }
    
    @ParameterizedTest
    @MethodSource("riskScenarioProvider")
    @DisplayName("calculateRiskScore should return correct risk level for different scenarios")
    void calculateRiskScoreShouldReturnCorrectRiskLevel(
            String currencyPair, 
            BigDecimal amount, 
            String counterparty, 
            boolean settlementRiskCovered,
            RiskLevel expectedRiskLevel) {
        
        // Arrange
        FXTrade trade = new FXTrade(
            "TEST-ID", 
            currencyPair, 
            amount, 
            "BUY", 
            counterparty, 
            settlementRiskCovered
        );
        
        // Act
        RiskLevel actualRiskLevel = trade.calculateRiskScore();
        
        // Assert
        assertEquals(expectedRiskLevel, actualRiskLevel, 
            String.format("Expected %s risk for %s %s with %s (settlement covered: %s)", 
                expectedRiskLevel, amount, currencyPair, counterparty, settlementRiskCovered));
    }
    
    static Stream<Arguments> riskScenarioProvider() {
        return Stream.of(
            // Low risk scenarios
            Arguments.of("EUR/USD", new BigDecimal("100000"), "BANK_A", true, RiskLevel.LOW),
            Arguments.of("USD/JPY", new BigDecimal("500000"), "BANK_A", true, RiskLevel.LOW),
            
            // Medium risk scenarios
            Arguments.of("EUR/USD", new BigDecimal("7000000"), "BANK_A", true, RiskLevel.MEDIUM), // Medium amount
            Arguments.of("EUR/USD", new BigDecimal("1000000"), "BANK_B", true, RiskLevel.MEDIUM), // Medium risk counterparty
            Arguments.of("EUR/USD", new BigDecimal("1000000"), "BANK_A", false, RiskLevel.MEDIUM), // No settlement risk
            
            // High risk scenarios
            Arguments.of("USD/RUB", new BigDecimal("1000000"), "BANK_A", true, RiskLevel.MEDIUM), // High risk currency
            Arguments.of("EUR/USD", new BigDecimal("11000000"), "BANK_A", true, RiskLevel.MEDIUM), // High amount
            Arguments.of("EUR/USD", new BigDecimal("1000000"), "BANK_C", true, RiskLevel.MEDIUM), // High risk counterparty
            
            // Combined high risk scenarios
            Arguments.of("USD/RUB", new BigDecimal("11000000"), "BANK_A", true, RiskLevel.HIGH), // High risk currency + high amount
            Arguments.of("USD/RUB", new BigDecimal("1000000"), "BANK_C", true, RiskLevel.HIGH), // High risk currency + high risk counterparty
            Arguments.of("EUR/TRY", new BigDecimal("7000000"), "HEDGE_FUND_B", false, RiskLevel.HIGH), // Many risk factors combined
            Arguments.of("USD/TRY", new BigDecimal("15000000"), "OFFSHORE_ENTITY_A", false, RiskLevel.HIGH) // Extreme case
        );
    }
    
    @Test
    @DisplayName("equals and hashCode should work correctly")
    void equalsAndHashCodeShouldWorkCorrectly() {
        // Arrange
        FXTrade trade1 = new FXTrade("id1", "EUR/USD", new BigDecimal("1000000"), "BUY", "BANK_A", true);
        FXTrade trade2 = new FXTrade("id1", "EUR/USD", new BigDecimal("1000000"), "BUY", "BANK_A", true);
        FXTrade trade3 = new FXTrade("id2", "EUR/USD", new BigDecimal("1000000"), "BUY", "BANK_A", true);
        
        // Assert
        assertEquals(trade1, trade2, "Same content trades should be equal");
        assertNotEquals(trade1, trade3, "Trades with different IDs should not be equal");
        assertEquals(trade1.hashCode(), trade2.hashCode(), "Same content trades should have same hashCode");
        
        // Test null and different object types
        assertNotEquals(null, trade1, "Trade should not equal null");
        assertNotEquals("not a trade", trade1, "Trade should not equal other object types");
    }
    
    @Test
    @DisplayName("toString should return a meaningful representation")
    void toStringShouldReturnMeaningfulRepresentation() {
        // Arrange
        String id = "TR12345";
        String currencyPair = "EUR/USD";
        BigDecimal amount = new BigDecimal("1000000");
        String direction = "BUY";
        String counterparty = "BANK_A";
        boolean settlementRiskCovered = true;
        
        FXTrade trade = new FXTrade(id, currencyPair, amount, direction, counterparty, settlementRiskCovered);
        
        // Act
        String result = trade.toString();
        
        // Assert
        assertTrue(result.contains(id), "toString should contain the ID");
        assertTrue(result.contains(currencyPair), "toString should contain the currency pair");
        assertTrue(result.contains(amount.toString()), "toString should contain the amount");
        assertTrue(result.contains(direction), "toString should contain the direction");
        assertTrue(result.contains(counterparty), "toString should contain the counterparty");
        assertTrue(result.contains(String.valueOf(settlementRiskCovered)), 
            "toString should contain the settlement risk status");
    }
}
