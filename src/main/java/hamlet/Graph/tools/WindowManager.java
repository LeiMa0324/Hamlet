package hamlet.Graph.tools;

import hamlet.query.window.Window;

import java.util.ArrayList;

public class WindowManager {

    private ArrayList<Window> windows;

    public WindowManager(ArrayList<Window> windows){
        this.windows = windows;
    }

    public void initAllWindows(long currentTimeStamp){
        for (Window win:windows){
            win.activate(currentTimeStamp);
        }
    }

    public ArrayList<Integer> getExpiredQueries(long currentTimeStamp){

        ArrayList<Integer> expiredQueries = new ArrayList<>();
        for (int i =0; i< windows.size(); i++){
            Window win = windows.get(i);
            if (win.checkExpire(currentTimeStamp)){
                expiredQueries.add(i);
            }
        }
        return expiredQueries;
    }

    public void slideWindows(ArrayList<Integer> expiredQueries){
        for (Integer i: expiredQueries){
            //slide the window
            windows.get(i).slide();
        }

    }
}
