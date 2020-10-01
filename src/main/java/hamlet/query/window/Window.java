package hamlet.query.window;

import lombok.Data;

/**
 * the Window in the query
 */

@Data
public class Window {

    private long startTime;
    private long window;
    private long slidingUnit;
    private boolean isExpired;

    public Window(long window, long slide){
        this.window = window;
        this.slidingUnit = slide;
    }

    /**
     * activate the Window
     * @param timestamp the start timestamp
     */
    public void activate(long timestamp){
        this.startTime = timestamp;
        this.isExpired = false;

    }

    /**
     * given a current timestamp, check if the Window expires
     * @param currentTimestamp the current timestamp
     */
    public boolean checkExpire(long currentTimestamp){

        this.isExpired = currentTimestamp>(this.startTime + window);
        return isExpired;

    }

    /**
     * slide the Window
     */
    public void slide(){
        this.startTime = this.startTime + slidingUnit;
        this.isExpired = false;
    }

    @Override
    public String toString(){
        return "WITHIN "+ window +" SLIDE "+ slidingUnit;
    }

}
