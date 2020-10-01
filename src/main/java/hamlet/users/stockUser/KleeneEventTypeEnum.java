package hamlet.users.stockUser;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public enum KleeneEventTypeEnum {
    TVIX,
    ARDX,
    QQQ,
    TQQQ,
    CDTX,
    NAKD,
    AMD,
    ROKU,
    SQQQ,
    ABEO;



    private static final List<KleeneEventTypeEnum> VALUES =
            Collections.unmodifiableList(Arrays.asList(values()));

    private static final int SIZE = VALUES.size();

    private static final Random RANDOM = new Random();

    public static KleeneEventTypeEnum randomEvent()  {
        return  VALUES.get(RANDOM.nextInt(SIZE));

    }
}
