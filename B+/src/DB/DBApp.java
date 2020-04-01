package DB;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import javax.jws.Oneway;

public class DBApp {

	public static void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) {
		try {
			if (isDuplicate(strTableName))
				return;
			WriteCsv(strTableName, strClusteringKeyColumn, htblColNameType);
			writeTableData(strTableName, strClusteringKeyColumn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void writeTableData(String strTableName,
			String strClusteringKey) throws IOException {
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

	private static void WriteCsv(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws Exception {
		// Table Name, Column Name, Column Type, ClusteringKey.
		FileWriter br = new FileWriter("data/metadata.csv", true);
		Enumeration<String> e = htblColNameType.keys();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
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

		}
		br.append(strTableName
				+ ",TouchDate,"
				+ "java.util.Date"
				+ ","
				+ ("TouchDate".equals(strClusteringKeyColumn) ? "True"
						: "False") + ",False" + '\n');
		br.flush();
		br.close();
	}

	public static boolean isDuplicate(String tableName) throws Exception {
		FileInputStream x = null;
		ObjectInputStream y = null;
		Vector<String[]> z = null;
		try {
			x = new FileInputStream("classes/UnFound/Tables.class");
			z = (Vector<String[]>) (new ObjectInputStream(x).readObject());
			for (int i = 0; i < z.size(); i++) {
				if (z.get(i)[0].equals(tableName)) {
					x.close();
					y.close();
					return true;
				}
			}
			x.close();
			y.close();
			return false;
		} catch (Exception e) {
			return false;
		}

	}

	public static int numberofPages(String name) throws IOException, Exception {
		FileInputStream k = new FileInputStream("Tables.class");
		ObjectInputStream l = new ObjectInputStream(k);
		ArrayList<String[]> p = (ArrayList<String[]>) l.readObject();

		for (int i = 0; i < p.size(); i++) {
			if (p.get(i)[0].equals(name)) {
				k.close();
				l.close();
				return Integer.parseInt(p.get(i)[1]);
			}
		}
		return 0;
	}

	public static Boolean validateTable(String strTableName,
			Hashtable<String, String> htblColNameValue) {
		Enumeration<String> colName = htblColNameValue.keys();
		try {
			while (colName.hasMoreElements()) {
				String line = "";
				String cvsSplitBy = ",";
				BufferedReader br = new BufferedReader(new FileReader(
						"data/metadata.csv"));
				String el = colName.nextElement();
				boolean f = false;
				while ((line = br.readLine()) != null) {
					// use comma as separator
					String[] r = line.split(cvsSplitBy);
					if (!(r[0].equals(strTableName) && r[1].equals(el) && (((htblColNameValue
							.get(el)).getClass()).getName()).equals(r[2])))
						f = true;
				}
				if (!f)
					return false;
			}
			return true;
		} catch (Exception e) {
			// //////////////////////////
			return false;
		}
	}

	public static Object getKey(String name) throws Exception {

		FileInputStream k = new FileInputStream("Tables.class");
		ObjectInputStream l = new ObjectInputStream(k);
		ArrayList<String[]> p = (ArrayList<String[]>) l.readObject();

		for (int i = 0; i < p.size(); i++) {
			if (p.get(i)[0].equals(name)) {
				k.close();
				l.close();
				return Integer.parseInt(p.get(i)[2]);
			}
		}
		return 0;
	}

	public static Page readPage(String tableName, String id) throws Exception {
		FileInputStream x = new FileInputStream("classes/UnFound/" + tableName
				+ id + ".class");
		ObjectInputStream y = new ObjectInputStream(x);
		Page z = (Page) y.readObject();
		x.close();
		y.close();
		return z;
	}
	
	public void updateTable(String strTableName, String strClusteringKey,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			int n = numberofPages(strTableName);
			String z = getStrKey(strTableName);
			boolean f = false;
			for (int i = 0; i < n; i++) {
				Page p = readPage(strTableName, i + "");
				for (int j = 0; j < p.page.size(); j++) {
					String a = p.page.get(j).get(z).toString();
					if (a.equals(strClusteringKey)) {
						Enumeration<String> keyValues = htblColNameValue.keys();
						while (keyValues.hasMoreElements()) {
							String k = (String) keyValues.nextElement();
							p.page.get(j).put(k, htblColNameValue.get(k));
						}
						writePage(strTableName, i + "", p);
						f = true;
					}
				}	
			}
			if (!f)
				throw new DBAppException("Row not Found");

		}catch(Exception e){
			
		}

	}
	public static void writePage(String tableName, String id, Page p)
			throws Exception {

		FileOutputStream x = new FileOutputStream("classes/UnFound/"
				+ tableName + id + ".class");
		ObjectOutputStream y = new ObjectOutputStream(x);
		y.writeObject(p);
		x.close();
		y.close();
	}

	public static String getStrKey(String strTableName) throws Exception {
		FileInputStream x = new FileInputStream("classes/UnFound/Tables.class");
		ObjectInputStream y = new ObjectInputStream(x);
	//	@SuppressWarnings("unchecked")
		ArrayList<String[]> z = (ArrayList<String[]>) y.readObject();
		for (int i = 0; i < z.size(); i++) {
			if (z.get(i)[0].equals(strTableName)) {
				x.close();
				y.close();
				return z.get(i)[2];
			}
		}
		x.close();
		y.close();
		return "";
	}
	
	public static void main(String[] args) {
		Hashtable<String, String> a = new Hashtable<String, String>();
		a.put("ab", "java.lang.Integer");
		a.put("c", "java.lang.Integer");
		a.put("d", "java.lang.Double");
		createTable("abzo", "d", a);
	}
}
