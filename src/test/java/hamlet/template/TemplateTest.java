package hamlet.template;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class TemplateTest {
    private Template template;

    @Before
    public void Template_setup(){
        ArrayList<String> queries = new ArrayList<>();
        String q1="A,B+,E+";
        String q2="C,B+,E+";
        queries.add(q1);
        queries.add(q2);
        this.template = new Template(queries);

    }

    /**
     * main the shared events finding method
     */
    @Test
    public void findSharedEvents_test(){
        ArrayList<String> res = new ArrayList<>();
        res.add("B");
        res.add("E");
        assertEquals(template.getSharedEvents(),res);

    }

}