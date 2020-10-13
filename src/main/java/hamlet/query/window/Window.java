package hamlet.query.window;

import lombok.Data;

/**
 * the Window in the query
 */

@Data
public class Window {

    //milli
    private long startTime;
    private long windowInMinute;
    private long slidingMinute;
    private boolean isExpired;

    public Window(long window, long slide){
        this.windowInMinute = window;
        this.slidingMinute = slide;
    }

    /**
     * activate the Window
     */
    public void activate(long currentTimestamp){
        this.startTime = currentTimestamp;
        this.isExpired = false;

    }

    /**
     * given a current timestamp, check if the Window expires
     */
    public boolean checkExpire(long currentTimestamp){

        this.isExpired = currentTimestamp>(this.startTime + windowInMinute*1000*60);
        return isExpired;

    }

    /**
     * slide the Window
     */
    public void slide(){
        this.startTime = this.startTime + slidingMinute*1000*60;
        this.isExpired = false;
    }

    @Override
    public String toString(){
        return "WITHIN "+ windowInMinute +" min SLIDE "+ slidingMinute +" min";
    }

}
