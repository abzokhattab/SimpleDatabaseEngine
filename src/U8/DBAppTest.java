package U8;

import java.awt.Polygon;
import java.io.IOException;
import java.util.Hashtable;

public class DBAppTest {

	public static void main(String[] args) throws Exception {

		DBApp x = new DBApp();
		x.init();
		Hashtable y = new Hashtable();
		y.put("name", "java.lang.String");
		x.createTable("Student", "name", y);

		Hashtable f = new Hashtable();
		f.put("name", "sami");
		x.insertIntoTable("Student", f);

	}

}
