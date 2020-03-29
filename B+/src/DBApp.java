import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public class DBApp {

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) {
		try {
			if (!isDuplicate(strTableName))
				return;
			WriteCsv(strTableName, strClusteringKeyColumn, htblColNameType);
			writeTableData(strTableName, strClusteringKeyColumn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static void writeTableData(String strTableName, String strClusteringKey)
			throws IOException {
		FileInputStream x = null;
		ObjectInputStream y = null;
		Vector<String[]> z = null;
		try {
			x = new FileInputStream("classes/UnFound/Tables.class");
			y = new ObjectInputStream(x);
			z = (Vector<String[]>) y.readObject();
		} catch (Exception e) {
			z = new Vector<String[]>();
		}
		String[] k = { strTableName, "0", strClusteringKey };
		z.add(k);
		FileOutputStream a = new FileOutputStream(
				"classes/UnFound/Tables.class");
		ObjectOutputStream b = new ObjectOutputStream(a);
		b.writeObject(z);
		a.close();
		b.close();
	}


	
	
	private void WriteCsv(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) {
		// Table Name, Column Name, Column Type, ClusteringKey.
		Enumeration<String> e = htblColNameType.keys();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			FileWriter br = new FileWriter("data/metadata.csv", true);
			br.append(strTableName + ",");
			br.append(key + ",");

			if (!(htblColNameType.get(key).equals("java.lang.Integer")
					|| htblColNameType.get(key).equals("ava.lang.String")
					|| htblColNameType.get(key).equals("java.lang.Double")
					|| htblColNameType.get(key).equals("java.lang.Boolean,")
					|| htblColNameType.get(key).equals("java.util.Date") || htblColNameType
					.get(key).equals("java.awt.Polygon"))) {
				br.flush();
				br.close();
				throw new DBAppException("Wrong Formate");
			}
			br.append(htblColNameType.get(key) + ",");

			if (key.equals(strClusteringKeyColumn))
				br.append("True,");
			else
				br.append("False,");
			br.append("False,");

			br.append('\n');
			br.flush();
			br.close();
		}
	}
	public static boolean isDuplicate(String tableName) throws Exception {
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
