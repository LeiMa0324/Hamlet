package hamlet.stream;

import hamlet.base.Event;
import hamlet.workload.Workload;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * partition the stream into sub streams, according to the mini-workload
 */
public class streamPartitioner {

    private Workload miniworkload;
    private ArrayList<Event> events;

    public streamPartitioner(Workload miniWorkload, ArrayList<Event> events){
        this.miniworkload = miniWorkload;
        this.events = events;
    }

    /**
     * partition the stream into substreams according to the groupby
     * @return a hashmap of attrValue and substream
     */
    public HashMap<Object, ArrayList<Event>> partition(){
        HashMap<Object, ArrayList<Event>> substreams = new HashMap<Object, ArrayList<Event>>();

        //todo

        return substreams;

    }
}
