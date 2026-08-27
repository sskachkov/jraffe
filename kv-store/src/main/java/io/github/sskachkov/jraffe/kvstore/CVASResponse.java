package io.github.sskachkov.jraffe.kvstore;

public class CVASResponse {
    public enum Status {
        SUCCESS(0), KEY_NOT_FOUND(1), VALUE_MISMATCH(2);

        public final byte code;
        Status(int code) { this.code = (byte) code; }

        static Status fromCode(byte code) {
            return switch (code) {
                case 0 -> SUCCESS;
                case 1 -> KEY_NOT_FOUND;
                case 2 -> VALUE_MISMATCH;
                default -> throw new IllegalArgumentException("unknown CVAS status code: " + code);
            };
        }
    }

    private final Status status;
    private final byte[] actualValue; // only set when status == VALUE_MISMATCH

    private CVASResponse(Status status, byte[] actualValue) {
        this.status = status;
        this.actualValue = actualValue;
    }

    public static CVASResponse success() { return new CVASResponse(Status.SUCCESS, null); }
    public static CVASResponse keyNotFound() { return new CVASResponse(Status.KEY_NOT_FOUND, null); }
    public static CVASResponse valueMismatch(byte[] actualValue) { return new CVASResponse(Status.VALUE_MISMATCH, actualValue); }

    public Status status() { return status; }
    public byte[] actualValue() { return actualValue; }
}