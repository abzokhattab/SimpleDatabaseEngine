import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Hashtable;


public class DBApp {

	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType )
			{
		
			
		
	}
	
	public static boolean isDuplicate(String tableName) throws Exception{
		FileInputStream x = null;
		ObjectInputStream y = null;
		ArrayList<String[]> z = null;
		try {
			x = new FileInputStream("classes/UnFound/Tables.class");
			z = (ArrayList<String[]>) (new ObjectInputStream(x).readObject());
			for (int i = 0; i < z.size(); i++) {
				if (z.get(i)[0].equals(tableName)) {
					x.close();
					y.close();
					return false;
				}
			}
			x.close();
			y.close();
			return true;
		} catch (Exception e) {
			return true;
		}

		
	}
}
