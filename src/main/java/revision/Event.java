package revision;

import lombok.Data;

@Data
public class Event {
    private int vendorID;
    private int payment;
    private float tripDistance;
    private float totalAmount;

    public Event(int vendorID, int payment, float tripDistance, float totalAmount){
        this.vendorID = vendorID;
        this.payment = payment;
        this.tripDistance = tripDistance;
        this.totalAmount = totalAmount;
    }
}
