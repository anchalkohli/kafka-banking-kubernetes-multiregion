package com.example.ingestion.service;

import com.example.ingestion.model.MessageClassification;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class MessageClassifier {
    private static final Pattern SWIFT_BLOCK_2 = Pattern.compile("\\{2:([IO])(\\d{3})([^}]*)}");
    private static final Pattern MX_DOCUMENT = Pattern.compile(
            "<(?:\\w+:)?(pacs\\.\\d{3}\\.\\d{3}\\.\\d{2}|camt\\.\\d{3}\\.\\d{3}\\.\\d{2})",
            Pattern.CASE_INSENSITIVE);

    private static final Set<String> PAYMENT_MT = Set.of(
            "MT101", "MT102", "MT103", "MT104", "MT202", "MT205");
    private static final Set<String> REPORTING_MT = Set.of(
            "MT900", "MT910", "MT940", "MT942", "MT950");

    public MessageClassification classify(String payload) {
        if (payload == null || payload.isBlank()) {
            return unknown();
        }

        Matcher mt = SWIFT_BLOCK_2.matcher(payload);
        if (mt.find()) {
            String type = "MT" + mt.group(2);
            String networkPriority = extractSwiftNetworkPriority(mt.group(3));
            String route = routeMt(type, networkPriority);
            String businessPriority = "critical".equals(route) ? "HIGH"
                    : "reporting".equals(route) ? "LOW" : "STANDARD";
            return new MessageClassification("SWIFT_MT", type, networkPriority, businessPriority, route);
        }

        Matcher mx = MX_DOCUMENT.matcher(payload);
        if (mx.find()) {
            String type = mx.group(1).toLowerCase(Locale.ROOT);
            String route = type.startsWith("pacs.") ? "standard" : "reporting";
            return new MessageClassification("ISO_20022", type, "NA",
                    "reporting".equals(route) ? "LOW" : "STANDARD", route);
        }

        String trimmed = payload.stripLeading();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            return new MessageClassification("JSON", "UNKNOWN", "NA", "STANDARD", "standard");
        }

        return unknown();
    }

    private String routeMt(String type, String networkPriority) {
        if (REPORTING_MT.contains(type)) {
            return "reporting";
        }
        if (PAYMENT_MT.contains(type)) {
            // U = urgent FIN network priority. Message type alone does not make a payment critical.
            return "U".equals(networkPriority) ? "critical" : "standard";
        }
        return "quarantine";
    }

    private String extractSwiftNetworkPriority(String block2Remainder) {
        if (block2Remainder == null || block2Remainder.isBlank()) {
            return "N";
        }
        // Input headers normally end with delivery-monitoring/obsolescence after priority.
        // Search from the end to avoid interpreting BIC characters as priority.
        for (int i = block2Remainder.length() - 1; i >= 0; i--) {
            char c = Character.toUpperCase(block2Remainder.charAt(i));
            if (c == 'U' || c == 'N' || c == 'S') {
                return String.valueOf(c);
            }
        }
        return "N";
    }

    private MessageClassification unknown() {
        return new MessageClassification("UNKNOWN", "UNKNOWN", "NA", "UNKNOWN", "quarantine");
    }
}
