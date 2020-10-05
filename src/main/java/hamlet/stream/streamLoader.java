package hamlet.stream;

import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.workload.Workload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * load the stream file into a arraylist of events
 */
public class streamLoader {
    protected String streamFile;
    protected DatasetSchema schema;
    protected Workload workload;
    protected ArrayList<Event> events;

    public streamLoader(String streamFile, DatasetSchema schema, Workload workload) {
        this.streamFile = streamFile;
        this.schema = schema;
        this.workload = workload;
        this.events = new ArrayList<>();

    }

    public ArrayList<Event> stream() {
        ArrayList<EventType> eventTypes = workload.getAllEventTypes();
        ArrayList<String> eventTypeNames = new ArrayList<>();
        for (EventType et : eventTypes) {
            eventTypeNames.add(et.getName());
        }

        try {
            Scanner scanner = new Scanner(new File(streamFile));
            scanner.nextLine();

            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] data = line.split(",");
                if (checkEventType(data, eventTypeNames)) {
                    dataToEvent(data, eventTypes);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();

        }

        return events;

    }


    /**
     * check if a line of data belongs to a given event type
     * could be overrided by different loaders in diff users
     *
     * @param data
     * @param
     * @return
     */
    private boolean checkEventType(String[] data,
                                   ArrayList<String> allEventTypeNames) {
        return allEventTypeNames.contains(data[0]);

    }

    private void dataToEvent(String[] data, ArrayList<EventType> eventTypes){
        Event event = new Event(workload.getEventTypeByName(data[0]), data);
        events.add(event);
    }
}
