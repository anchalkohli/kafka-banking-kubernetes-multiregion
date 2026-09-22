package com.example.ingestion.service;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MessageClassifierTest {
    private final MessageClassifier classifier = new MessageClassifier();

    @Test void identifiesUrgentMt103AsCritical() {
        var r = classifier.classify("{1:F01AAAAGB2LAXXX0000000000}{2:I103BBBBUS33XXXXU}{4:\n:20:REF\n-}");
        assertThat(r.format()).isEqualTo("SWIFT_MT");
        assertThat(r.messageType()).isEqualTo("MT103");
        assertThat(r.networkPriority()).isEqualTo("U");
        assertThat(r.route()).isEqualTo("critical");
    }

    @Test void doesNotMistakeReceiverBicCharactersForPriority() {
        var r = classifier.classify("{2:I103SUNNUS33XXXXN}");
        assertThat(r.networkPriority()).isEqualTo("N");
        assertThat(r.route()).isEqualTo("standard");
    }

    @Test void identifiesNormalMt202AsStandard() {
        assertThat(classifier.classify("{2:I202BBBBUS33XXXXN}").route()).isEqualTo("standard");
    }

    @Test void routesMt940ToReporting() {
        assertThat(classifier.classify("{2:I940BBBBUS33XXXXN}").route()).isEqualTo("reporting");
    }

    @Test void detectsIso20022FromNamespace() {
        var r = classifier.classify("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08\"></Document>");
        assertThat(r.format()).isEqualTo("ISO_20022");
        assertThat(r.messageType()).isEqualTo("pacs.008.001.08");
        assertThat(r.route()).isEqualTo("standard");
    }

    @Test void quarantinesUnknownPayload() {
        assertThat(classifier.classify("unsupported").route()).isEqualTo("quarantine");
    }
}
