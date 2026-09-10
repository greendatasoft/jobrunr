package org.jobrunr.utils.exceptions;

import org.jobrunr.storage.StorageException;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class RepeatedExceptionFilterTest {

    private final RepeatedExceptionFilter repeatedExceptionFilter = new RepeatedExceptionFilter();

    @Test
    void theFirstExceptionIsAlwaysAFirstOccurrence() {
        assertThat(repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out after 30001ms"))).isTrue();
        assertThat(repeatedExceptionFilter.getRepeatCount()).isEqualTo(1);
    }

    @Test
    void anExceptionThatKeepsRepeatingIsOnlyAFirstOccurrenceOnce() {
        assertThat(repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out after 30001ms"))).isTrue();
        assertThat(repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out after 30014ms"))).isFalse();
        assertThat(repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out after 30007ms"))).isFalse();

        assertThat(repeatedExceptionFilter.getRepeatCount()).isEqualTo(3);
    }

    @Test
    void anotherExceptionIsAFirstOccurrenceAgainAndResetsTheRepeatCount() {
        repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out"));
        repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out"));

        assertThat(repeatedExceptionFilter.isFirstOccurrence(new IllegalStateException("something else"))).isTrue();
        assertThat(repeatedExceptionFilter.getRepeatCount()).isEqualTo(1);
    }

    @Test
    void anExceptionWithAnotherCauseIsAFirstOccurrence() {
        assertThat(repeatedExceptionFilter.isFirstOccurrence(storageException("connection timed out"))).isTrue();
        assertThat(repeatedExceptionFilter.isFirstOccurrence(new StorageException(new IllegalStateException("connection timed out")))).isTrue();
    }

    @Test
    void aCyclicCauseChainDoesNotResultInAnEndlessLoop() {
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        first.initCause(second);
        second.initCause(first);

        assertThat(repeatedExceptionFilter.isFirstOccurrence(first)).isTrue();
        assertThat(repeatedExceptionFilter.isFirstOccurrence(first)).isFalse();
    }

    private StorageException storageException(String message) {
        return new StorageException(new SQLException(message));
    }
}
