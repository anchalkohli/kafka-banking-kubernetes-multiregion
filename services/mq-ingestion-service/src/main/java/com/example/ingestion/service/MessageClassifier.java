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
    private static final Pattern MX_NAMESPACE = Pattern.compile(
            "urn:iso:std:iso:20022:tech:xsd:((?:pacs|camt)\\.\\d{3}\\.\\d{3}\\.\\d{2})",
            Pattern.CASE_INSENSITIVE);

    private static final Set<String> PAYMENT_MT = Set.of("MT101", "MT102", "MT103", "MT104", "MT202", "MT205");
    private static final Set<String> REPORTING_MT = Set.of("MT900", "MT910", "MT940", "MT942", "MT950");

    public MessageClassification classify(String payload) {
        if (payload == null || payload.isBlank()) return unknown();

        Matcher mt = SWIFT_BLOCK_2.matcher(payload);
        if (mt.find()) {
            String direction = mt.group(1);
            String type = "MT" + mt.group(2);
            String priority = swiftPriority(direction, mt.group(3));
            String route = routeMt(type, priority);
            return new MessageClassification("SWIFT_MT", type, priority,
                    route.equals("critical") ? "HIGH" : route.equals("reporting") ? "LOW" : "STANDARD", route);
        }

        Matcher mx = MX_NAMESPACE.matcher(payload);
        if (mx.find()) {
            String type = mx.group(1).toLowerCase(Locale.ROOT);
            String route = type.startsWith("pacs.") ? "standard" : "reporting";
            return new MessageClassification("ISO_20022", type, "NA",
                    route.equals("reporting") ? "LOW" : "STANDARD", route);
        }

        String trimmed = payload.stripLeading();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            return new MessageClassification("JSON", "UNKNOWN", "NA", "STANDARD", "standard");
        }
        return unknown();
    }

    private String routeMt(String type, String priority) {
        if (REPORTING_MT.contains(type)) return "reporting";
        if (PAYMENT_MT.contains(type)) return "U".equals(priority) ? "critical" : "standard";
        return "quarantine";
    }

    private String swiftPriority(String direction, String remainder) {
        if (remainder == null) return "N";
        // FIN input block 2: receiver LT address (12 chars) followed by priority.
        if ("I".equals(direction) && remainder.length() >= 13) {
            char p = Character.toUpperCase(remainder.charAt(12));
            return validPriority(p) ? String.valueOf(p) : "N";
        }
        // FIN output block 2 ends with network priority in the standard envelope.
        if ("O".equals(direction) && !remainder.isBlank()) {
            char p = Character.toUpperCase(remainder.charAt(remainder.length() - 1));
            return validPriority(p) ? String.valueOf(p) : "N";
        }
        return "N";
    }

    private boolean validPriority(char c) {
        return c == 'U' || c == 'N' || c == 'S';
    }

    private MessageClassification unknown() {
        return new MessageClassification("UNKNOWN", "UNKNOWN", "NA", "UNKNOWN", "quarantine");
    }
}
