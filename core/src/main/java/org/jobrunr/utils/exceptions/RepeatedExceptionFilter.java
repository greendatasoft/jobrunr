package org.jobrunr.utils.exceptions;

/**
 * Detects exceptions that keep repeating so a full stacktrace is only logged once. Without it, an outage of the
 * StorageProvider results in a stacktrace per task per poll interval, drowning out everything else in the logs.
 */
public class RepeatedExceptionFilter {

    private static final int MAX_CAUSE_DEPTH = 10;

    private String lastSignature;
    private int repeatCount;

    /**
     * @return true if the given exception differs from the one passed on the previous invocation and thus deserves
     * a full stacktrace in the logs.
     */
    public synchronized boolean isFirstOccurrence(Throwable throwable) {
        String signature = signatureOf(throwable);
        if (signature.equals(lastSignature)) {
            repeatCount++;
            return false;
        }
        lastSignature = signature;
        repeatCount = 1;
        return true;
    }

    /**
     * @return how many times the current exception occurred in a row.
     */
    public synchronized int getRepeatCount() {
        return repeatCount;
    }

    private static String signatureOf(Throwable throwable) {
        StringBuilder signature = new StringBuilder();
        Throwable current = throwable;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            signature.append(current.getClass().getName());
            // why: the message often contains varying details (e.g. how long a connection timeout took), the
            // exception types and where they were thrown do not
            StackTraceElement[] stackTrace = current.getStackTrace();
            if (stackTrace.length > 0) signature.append('@').append(stackTrace[0]);
            signature.append('|');
            current = current.getCause() == current ? null : current.getCause();
        }
        return signature.toString();
    }
}
