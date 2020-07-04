package hamlet.event;

import hamlet.template.Template;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class StreamLoader {

    public ArrayList<Event> events;

    public StreamLoader(String streamFile, int epw, Template template){

        events = new ArrayList<>();


        try {    //load the stream into a list of events
            Scanner scanner = new Scanner(new File(streamFile));
            int numofEvents = 0;
            boolean isStarted = false;

            while (scanner.hasNext()&&numofEvents<epw) {
                String line = scanner.nextLine();
                String[] record = line.split(",");
                numofEvents++;

                //if e is in the template, ignore all dummy events
                if (!template.eventTypeExists(record[1])) {
                    continue;
                }
                if (!isStarted){
                    if (template.getStartEvents().contains(record[1])){
                        isStarted = true;
                    }
                    else {
                        continue;
                    }
                }
                if (isStarted){
                    Event e = new Event(line, template.getEventTypebyString(record[1]));
                    this.events.add(e);
                }

            }

            System.out.println("Relevant events number: "+ events.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Event> getEvents(){
        return this.events;
    }
}
