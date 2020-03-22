package hamlet.utils;

public interface Observable {

        /**
         * Register an object as observing this class
         * @param o Object to register
         */
        void register(Observer o);

        /**
         * Notify observers of change
         */
        void notifyObservers();
        void finishingLastNotify();
    }


