package edu.usfca.cs.dfs.exception;

public class RetrieveFileFailException extends RuntimeException{
    public RetrieveFileFailException() {
    }

    public RetrieveFileFailException(String message) {
        super(message);
    }

    public RetrieveFileFailException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetrieveFileFailException(Throwable cause) {
        super(cause);
    }

    public RetrieveFileFailException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
