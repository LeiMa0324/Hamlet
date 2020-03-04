package Hamlet.Utils;

public interface Observer {
    /**
     * Receive notification from observed object
     * @param object Whatever data might be passed in by the observed object
     */
    void notify(Object object);
}
