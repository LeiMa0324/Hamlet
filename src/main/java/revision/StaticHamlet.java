package revision;


import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * take in a mini-workload and one substream and run the static hamlet
 */
public class StaticHamlet {

    //a mini-workload
    private Workload miniWorkload;

    //a sub-stream
    private SubStream substream;

    // the burst size
    Integer burstSize;

    // the array of all bursts
    ArrayList<ArrayList<Event>> bursts;

    //the number of dense bursts
    Integer denseBurstnum = 0;


    public StaticHamlet(Workload miniworkload, SubStream subStream, Integer burstSize, double denseBurstPercent){

        this.substream = subStream;
        this.miniWorkload = miniworkload;
        this.burstSize = burstSize;
        this.bursts = new ArrayList<>();

        //the number of dense bursts
        denseBurstnum =new BigDecimal(denseBurstPercent+"")
                .multiply(new BigDecimal(bursts.size()+"")).intValue();

        loadBatches();

    }

    public void run(){
        for (ArrayList<Event> burst: bursts){
            shareBurst(burst, bursts.indexOf(burst));
        }
    }

    /**
     * force to share every burst
     * @param burst a single burst
     * @param index the index of the burst
     */
    public void shareBurst(ArrayList<Event> burst, int index){

    }


    /**
     * load events into bursts
     */
    void loadBatches(){

        int batchnum = ((substream.getEvents().size()% burstSize >0)?1:0)+(substream.getEvents().size()/ burstSize);

        for (int i =0;i<batchnum; i++){
            ArrayList<Event> batch = new ArrayList<>();
            for (int j = 0; j< burstSize; j++){
                if (i* burstSize +j==substream.getEvents().size()){
                    break;
                }
                batch.add(substream.getEvents().get(i* burstSize +j));
            }
            bursts.add(batch);

        }

    }
}
