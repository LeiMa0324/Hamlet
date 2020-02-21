package executor;

import NewTemplate.QueryTemplate;
import template.Template;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.ArrayList;

public class Test {

	public static void main(String[] args) {
		String q1="A,B+";
		String q2 ="C,B+";
		String q3 ="X,B+";
		ArrayList<String> queries = new ArrayList<String>();
		queries.add(q1);
		queries.add(q2);
		queries.add(q3);
		QueryTemplate tem = new QueryTemplate(queries);
		System.out.println(tem.getNodeList());
	}
}
