package hamlet.stream;

import hamlet.base.Event;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

/**
 * load the stream file into a arraylist of events
 */
public class streamLoader {
    protected String streamFile;

    //the related column number and the corresponding
    protected HashMap<String, Integer> columns;

    public streamLoader(String streamFile, HashMap<String, Integer> columns){
        this.streamFile = streamFile;
        this.columns = columns;

    }

    public ArrayList<Event> stream(){

        ArrayList<hamlet.base.Event> events = new ArrayList<>();

        try{
            Scanner scanner = new Scanner(new File(streamFile));
            scanner.nextLine();

            while (scanner.hasNext()){
                String line = scanner.nextLine();
                String[] data = line.split(",");

                //todo:
                //if the event is a relevant one in the workload
                // create an event with HashMap columns

            }

        }catch (IOException e){
            e.printStackTrace();

        }

        return events;

    }
}
