package org.iq80.leveldb;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DBException extends RuntimeException {
    private static final long serialVersionUID = 3861338611872160117L;

    public DBException() {
    }

    public DBException(String s) {
        super(s);
    }

    public DBException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DBException(Throwable throwable) {
        super(throwable);
    }
}
