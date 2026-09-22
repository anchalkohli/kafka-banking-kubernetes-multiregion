package com.example.ingestion.service;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MessageClassifierTest {
    private final MessageClassifier classifier = new MessageClassifier();

    @Test void identifiesUrgentMt103AsCritical() {
        var r = classifier.classify("{1:F01AAAAGB2LAXXX0000000000}{2:I103BBBBUS33XXXXU}{4:\n:20:REF\n-}");
        assertThat(r.messageType()).isEqualTo("MT103");
        assertThat(r.route()).isEqualTo("critical");
    }

    @Test void identifiesNormalMt202AsStandard() {
        assertThat(classifier.classify("{2:I202BBBBUS33XXXXN}").route()).isEqualTo("standard");
    }

    @Test void routesMt940ToReporting() {
        assertThat(classifier.classify("{2:I940BBBBUS33XXXXN}").route()).isEqualTo("reporting");
    }

    @Test void quarantinesUnknownPayload() {
        assertThat(classifier.classify("unsupported").route()).isEqualTo("quarantine");
    }
}
