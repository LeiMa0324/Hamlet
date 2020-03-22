package hamlet.utils;

import java.util.logging.Logger;

public class LoggerTests {
    public static void main(String[] args){
        Logger logger = Logger.getGlobal();
        logger.info("start processing...");
        logger.fine("fine");
        logger.warning("memory is running out...");
        logger.severe("severe");


    }
}
