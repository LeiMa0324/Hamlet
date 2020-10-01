package hamlet.zutils;

public interface Observer {
    /**
     * Receive notification from observed object
     * @param object Whatever data might be passed in by the observed object
     */
    void activeNotify(Object object);
    void finishNotify(Object object);
}
