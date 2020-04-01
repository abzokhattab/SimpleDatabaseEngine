package DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

//import UnFound.Page;

public class DB {

	class DBAppException extends Exception {

		public DBAppException(String x) {
			super(x);
		}

	}

	public static boolean checkDuplicates(String tableName) {
		try {
			boolean pp = false;
			FileInputStream x = new FileInputStream("Tables.class");
			ObjectInputStream y = new ObjectInputStream(x);
			ArrayList<String[]> z = (ArrayList<String[]>) y.readObject();
			for (int i = 0; i < z.size(); i++) {
				if (z.get(i)[0].equals(tableName))
					pp = true;
				else
					pp = false;
			}
			return (pp);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static void writeTable(String tableName, String clusterKey) {
		FileInputStream x;
		ObjectInputStream y;
		ArrayList<String[]> z;
		try {
			x = new FileInputStream("Tables.class");
			y = new ObjectInputStream(x);
			z = (ArrayList<String[]>) y.readObject();
		} catch (Exception e) {
			z = new ArrayList<String[]>();

		}
		String[] s = { tableName, "0", clusterKey };
		z.add(s);
		try {
			FileOutputStream a = new FileOutputStream("Tables.class");
			ObjectOutputStream b = new ObjectOutputStream(a);
			b.writeObject(z);
			a.close();
			b.close();
		} catch (Exception e) {

		}

	}

	private static void WriteCsv(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws Exception {
		// Table Name, Column Name, Column Type, ClusteringKey.
		FileWriter br = new FileWriter("metadata.csv", true);
		Enumeration<String> e = htblColNameType.keys();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			br.append(strTableName + ",");
			br.append(key + ",");

			if (!(htblColNameType.get(key).equals("java.lang.Integer")
					|| htblColNameType.get(key).equals("java.lang.String")
					|| htblColNameType.get(key).equals("java.lang.Double")
					|| htblColNameType.get(key).equals("java.lang.Boolean,")
					|| htblColNameType.get(key).equals("java.util.Date") || htblColNameType
					.get(key).equals("java.awt.Polygon"))) {
				br.flush();
				br.close();
				throw new Exception("Wrong Formate");
			}
			br.append(htblColNameType.get(key) + ",");
			// System.out.println(key);
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

	public static int numberofPages(String name) throws IOException,
			ClassNotFoundException {
		FileInputStream k = new FileInputStream("Tables.class");
		ObjectInputStream l = new ObjectInputStream(k);
		ArrayList<String[]> p = (ArrayList<String[]>) l.readObject();

		for (int i = 0; i < p.size(); i++) {
			if (p.get(i)[0].equals(name)) {
				k.close();
				l.close();
				// System.out.print(p.get(i)[1]);
				// System.out.println("k");
				return Integer.parseInt(p.get(i)[1]);

			}
		}
		k.close();
		l.close();
		return -1;
	}

	public static String findClusterkey(String name) throws IOException,
			ClassNotFoundException {

		FileInputStream k = new FileInputStream("Tables.class");
		ObjectInputStream l = new ObjectInputStream(k);
		ArrayList<String[]> p = (ArrayList<String[]>) l.readObject();
		for (int i = 0; i < p.size(); i++) {
			if (p.get(i)[0].equals(name)) {
				k.close();
				l.close();
				return (p.get(i)[2]);

			}
		}
		return null;
	}

	public static void writePage(String tableName, String id, Page p)
			throws IOException {
		FileOutputStream x = new FileOutputStream("classes/" + tableName + id
				+ ".class");
		ObjectOutputStream y = new ObjectOutputStream(x);
		y.writeObject(p);
		x.close();
		y.close();
	}

	public static void editNumberofPage(String Name, String n)
			throws IOException, ClassNotFoundException {

		FileInputStream x = new FileInputStream("Tables.class");
		ObjectInputStream y = new ObjectInputStream(x);

		ArrayList<String[]> z = (ArrayList<String[]>) y.readObject();
		for (int i = 0; i < z.size(); i++) {
			if (z.get(i)[0].equals(Name))
				// System.out.print(z.get(i)[1]);
				z.get(i)[1] = n;
			// System.out.print(z.get(i)[1]);
		}

		FileOutputStream o = new FileOutputStream("Tables.class");
		ObjectOutputStream p = new ObjectOutputStream(o);
		p.writeObject(z);

		y.close();
		x.close();
		y.close();

	}

	public static Page getPage(String tableName, String id) throws Exception {
		FileInputStream x = new FileInputStream("classes/" + tableName + id
				+ ".class");
		ObjectInputStream y = new ObjectInputStream(x);
		Page z = (Page) y.readObject();
		x.close();
		y.close();
		return z;
	}

	public static void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		if (checkDuplicates(strTableName))
			return;

		else {
			writeTable(strTableName, strClusteringKeyColumn);
			try {
				WriteCsv(strTableName, strClusteringKeyColumn, htblColNameType);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static void insertIntoTable(String strTabelName,
			Hashtable<String, Object> htblColNameValue) throws Exception {

		int x = numberofPages(strTabelName);

		if (x == 0) {

			Page P = new Page();
			P.addTuple(htblColNameValue, findClusterkey(strTabelName));
			writePage(strTabelName, x + "", P);
			editNumberofPage(strTabelName, (x + 1) + "");

		}

		else {
			Page p1 = getPage(strTabelName, (x - 1) + "");
			// System.out.print("l");
			if ((p1.isFull()) == false) {
				p1.addTuple(htblColNameValue, findClusterkey(strTabelName));
				writePage(strTabelName, (x - 1) + "", p1);

			} else {

				Page p2 = new Page();
				p2.addTuple(htblColNameValue, findClusterkey(strTabelName));
				// System.out.println(x);
				writePage(strTabelName, x + "", p2);
				editNumberofPage(strTabelName, (x + 1) + "");

			}

		}

	}

	public static int IsContain(Page page, String strClusteringKey, Object value) {
		String y = page.pageList.get(0).get(strClusteringKey).getClass()
				.getName();
		// System.out.println(this.pageList.get(i).get(strClusteringKey)
		// + "   " + x.get(strClusteringKey));
		if (y.equals("java.lang.Integer")) {
			// System.out.println(this.pageList.get(i).get(strClusteringKey)
			// .getClass().getName());
			if (((Integer) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Integer) value) != 1
					&& ((Integer) page.pageList.get(page.pageList.size() - 1)
							.get(strClusteringKey)).compareTo((Integer) value) != -1) {
				return 0;
			}
			else if (((Integer) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Integer) value) == 1)
				return 1 ; 
			else return -1 ; 
		} else if (y.equals("java.lang.Double")) {
			if (((Double) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Double) value) != 1
					&& ((Double) page.pageList.get(page.pageList.size() - 1)
							.get(strClusteringKey)).compareTo((Double) value) != -1) {
				return 0;
			}
			else if (((Double) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Double) value) == 1)
				return 1 ; 
			else return -1 ; 
		} else if (y.equals("java.util.Date")) {
			if (((Date) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Date) value) != 1
					&& ((Date) page.pageList.get(page.pageList.size() - 1).get(
							strClusteringKey)).compareTo((Date) value) != -1) {
				return 0;

			}
			else if (((Date) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Date) value) == 1)
				return 1 ; 
			else return -1 ; 
		} else if (y.equals("java.lang.String")) {
			if (((String) page.pageList.get(0).get(strClusteringKey))
					.compareTo((String) value) != 1
					&& ((String) page.pageList.get(page.pageList.size() - 1)
							.get(strClusteringKey)).compareTo((String) value) != -1) {
				return 0;

			}
			else if (((String) page.pageList.get(0).get(strClusteringKey))
					.compareTo((String) value) == 1)
				return 1 ; 
			else return -1 ; 
		} else if (y.equals("java.lang.Boolean")) {
			if (((Boolean) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Boolean) value) != 1
					&& ((Boolean) page.pageList.get(page.pageList.size() - 1)
							.get(strClusteringKey)).compareTo((Boolean) value) != -1) {
				return 0;
			}
			else if (((Boolean) page.pageList.get(0).get(strClusteringKey))
					.compareTo((Boolean) value) == 1)
				return 1 ; 
			else return -1 ; 
		}

		return 0;

	}

	public int binarySearch(String strClusteringKey,Object value, String tableName) throws Exception {
		int l = 0, r = this.numberofPages(tableName) - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (IsContain(getPage(tableName, m+""),strClusteringKey,value)==0)
				return m;
			// If x greater, ignore left half
			if (IsContain(getPage(tableName, m+""),strClusteringKey,value)==-1)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		return -1;
	}

	public static void updateTable(String strTableName,
			String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws Exception {
		boolean f = false;
		int n = numberofPages(strTableName);
		String y = findClusterkey(strTableName);

		for (int i = 0; i < n; i++) {
			Page p = getPage(strTableName, i + "");
			for (int j = 0; j < p.pageList.size(); j++) {

				if (strClusteringKey
						.equals(p.pageList.get(j).get(y).toString())) {
					f = true;
					// Hashtable<String, Object> g=p.pageList.get(j);
					Enumeration<String> keyValues = htblColNameValue.keys();

					while (keyValues.hasMoreElements()) {

						String k = (String) keyValues.nextElement();
						p.pageList.get(j).replace(k, htblColNameValue.get(k));
						// .pageList.get(j).put(k, htblColNameValue.get(k));
					}

				}

				writePage(strTableName, i + "", p);

			}
		}
		if (!f)
			System.out.print("row not found");
	}

	public static void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws Exception {
		int n = numberofPages(strTableName);
		// String y=findClusterkey(strTableName);

		for (int i = 0; i < n; i++) {
			Page p = getPage(strTableName, i + "");

			for (int j = 0; j < p.pageList.size(); j++) {
				Enumeration<String> r = htblColNameValue.keys();
				while (r.hasMoreElements()) {
					String v = r.nextElement();
					if ((htblColNameValue.get(v)).equals(p.pageList.get(j)
							.get(v).toString())) {

						p.pageList.remove(j);
						j--;

					}
				}
			}
			writePage(strTableName, i + "", p);
		}

	}

	public static void main(String[] args) throws Exception {

		// Hashtable<String,String> a =new Hashtable<String, String>();
		// a.put("ab","java.lang.Integer");
		// a.put("c","java.lang.Integer");
		// a.put("d","java.lang.Double");
		// try {
		// createTable("abzo", "d", a);
		// } catch (DBAppException e) {System.out.print("lol");
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// Hashtable<String,String> a =new Hashtable<String, String>();
		// a.put("name","java.long.String");
		// createTable("amr","Id",a);
		// //
		// editNumberofPage("amr","0");
		//
		Hashtable x = new Hashtable<String, String>();
		x.put("age", "java.lang.Integer");
		x.put("Name", "java.lang.String");
		x.put("ID", "java.lang.Integer");
		createTable("student", "ID", x);
		editNumberofPage("student", "0");
		Hashtable y = new Hashtable<String, String>();
		y.put("Name", "zeyad");
		y.put("ID", new Integer(100));
		y.put("age", "20");
		insertIntoTable("student", y);

		y = new Hashtable<String, String>();
		y.put("Name", "zeyad");
		y.put("ID", new Integer(30));
		y.put("age", "20");
		insertIntoTable("student", y);

		y = new Hashtable<String, String>();
		y.put("Name", "zeyad");
		y.put("ID", new Integer(50));
		y.put("age", "20");
		insertIntoTable("student", y);

		y = new Hashtable<String, String>();
		y.put("Name", "zeyad");
		y.put("ID", new Integer(40));
		y.put("age", "20");
		insertIntoTable("student", y);

		y = new Hashtable<String, String>();
		y.put("Name", "zeyad");
		y.put("ID", new Integer(102));
		y.put("age", "20");
		insertIntoTable("student", y);
		
		System.out.println(IsContain(getPage("student", "0"), "ID", 200));
		getPage("student", "0").ToString();
		// updateTable("student","1",y);
		// System.out.print(numberofPages("student"));}
		// deleteFromTable("student",y);
		// }try {
		// FileInputStream k=new FileInputStream("classes/student0.class");
		// ObjectInputStream l=new ObjectInputStream(k);
		// Page j= (Page)l.readObject();///////////////////////////////////////
		// System.out.print((j.pageList.get(0)).get("ID"));
		// }catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();}

		// //
		// } catch (ClassNotFoundException e) {
		//
		// TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		//
		// Hashtable x=new Hashtable<String,String>();
		// x.put("Id","java.lang.Integer");
		// //x.put("Name", "String");
		// //x.put("age", "int");
		//
		// try {
		// WriteCsv("Stusen","Id",x);
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// Hashtable<String,String> a =new Hashtable<String, String>();
		// a.put("ab","java.lang.Integer");
		// a.put("c","java.lang.Integer");
		// a.put("d","java.lang.Double");
		// try {
		// createTable("abkk", "d", a);
		// } catch (DBAppException e) {System.out.print("lol");
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// editNumberofPage("amr","0");
		//
		// Hashtable x=new Hashtable<String,int[]>();
		// //x.put("int",);
		// x.put("Name", "Amr");
		//
		// insertIntoTable("amr",x);
		//
		// x.replace("Name","amr");
		// System.out.print(x.get("Name"));
		//

	}
}