package hamlet.stream;

import hamlet.base.DatasetSchema;
import hamlet.base.Event;
import hamlet.base.EventType;
import hamlet.workload.Workload;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * load the stream file into a arraylist of events
 */
public class streamLoader {
    protected String streamFile;
    protected DatasetSchema schema;
    protected Workload wholeWorkload;
    protected ArrayList<Event> events;

    public streamLoader(String streamFile, DatasetSchema schema, Workload wholeWorkload) {
        this.streamFile = streamFile;
        this.schema = schema;
        this.wholeWorkload = wholeWorkload;
        this.events = new ArrayList<>();

    }

    public ArrayList<Event> stream() {
        ArrayList<EventType> eventTypes = wholeWorkload.getAllEventTypes();
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

        } catch (IOException | ParseException e) {
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

    private void dataToEvent(String[] data, ArrayList<EventType> eventTypes) throws ParseException {
        Event event = new Event(wholeWorkload.getEventTypeByName(data[0]), data);
        events.add(event);
    }
}
