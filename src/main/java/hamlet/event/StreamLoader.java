package hamlet.event;

import hamlet.template.Template;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * stream loader class loads the stream into a list of events with size epw
 */

public class StreamLoader {

    public ArrayList<Event> events;

    public StreamLoader(String streamFile, int epw, Template template){

        // list of relevant events
        events = new ArrayList<>();


        try {
            Scanner scanner = new Scanner(new File(streamFile));

            //keep track of the number of events
            int numofEvents = 0;

            //if met any of the start events
            boolean isStarted = false;

            //load epw relevant events
            while (scanner.hasNext() && numofEvents< epw) {
                String line = scanner.nextLine();
                String[] record = line.split(",");
                numofEvents++;

                //ignore irrelevant events
                if (!template.eventTypeExists(record[1])) {
                    continue;
                }

                //ignore events before the start type
                if (!isStarted){
                    if (template.getStartEvents().contains(record[1])){
                        isStarted = true;
                    }
                    else {
                        continue;
                    }
                }

                //add relevant events into the list
                if (isStarted){
                    Event e = new Event(line, template.getEventTypebyString(record[1]));
                    this.events.add(e);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * return all the relevant events
     * @return all the relevant events
     */
    public ArrayList<Event> getEvents(){
        return this.events;
    }
}
