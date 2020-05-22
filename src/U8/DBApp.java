package U8;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

//import UnFound.Page;

public class DBApp {

	public void init() {
		try {
			File file = new File("data/DBApp.class");
			if (!file.exists()) {
				file.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean checkDuplicates(String tableName) throws EOFException {
		try {
			boolean pp = false;
			FileInputStream x = new FileInputStream("data/DBApp.class");
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
			// e.printStackTrace();
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
			x = new FileInputStream("data/DBApp.class");
			y = new ObjectInputStream(x);
			z = (ArrayList<String[]>) y.readObject();
		} catch (Exception e) {
			z = new ArrayList<String[]>();

		}
		String[] s = { tableName, "0", clusterKey };
		z.add(s);
		try {
			FileOutputStream a = new FileOutputStream("data/DBApp.class");
			ObjectOutputStream b = new ObjectOutputStream(a);
			b.writeObject(z);
			a.close();
			b.close();
		} catch (Exception e) {

		}

	}

	private static void WriteCsv(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws Exception {
		// Table Name, Column Name, Column Type, ClusteringKey.
		FileWriter br = new FileWriter("data/metadata.csv", true);
		Enumeration<String> e = htblColNameType.keys();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			br.append(strTableName + ",");
			br.append(key + ",");

			if (!(htblColNameType.get(key).equals("java.lang.Integer")
					|| htblColNameType.get(key).equals("java.lang.String")
					|| htblColNameType.get(key).equals("java.lang.Double")
					|| htblColNameType.get(key).equals("java.lang.Boolean")
					|| htblColNameType.get(key).equals("java.util.Date")
					|| htblColNameType.get(key).equals("java.awt.Polygon"))) {
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
		br.append(strTableName + ",TouchDate," + "java.util.Date" + ","
				+ ("TouchDate".equals(strClusteringKeyColumn) ? "True" : "False") + ",False" + '\n');
		br.flush();
		br.close();
	}

	public static int numberofPages(String name) throws IOException, ClassNotFoundException {
		FileInputStream k = new FileInputStream("data/DBApp.class");
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

	public static String findClusterkey(String name) throws IOException, ClassNotFoundException {

		FileInputStream k = new FileInputStream("data/DBApp.class");
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

	public static void writePage(String tableName, String id, Page p) throws IOException {
		FileOutputStream x = new FileOutputStream("data/" + tableName + id + ".class");
		ObjectOutputStream y = new ObjectOutputStream(x);
		y.writeObject(p);
		x.close();
		y.close();
	}

	public static void editNumberofPage(String Name, String n) throws IOException, ClassNotFoundException {

		FileInputStream x = new FileInputStream("data/DBApp.class");
		ObjectInputStream y = new ObjectInputStream(x);

		ArrayList<String[]> z = (ArrayList<String[]>) y.readObject();
		for (int i = 0; i < z.size(); i++) {
			if (z.get(i)[0].equals(Name))
				// System.out.print(z.get(i)[1]);
				z.get(i)[1] = n;
			// System.out.print(z.get(i)[1]);
		}

		FileOutputStream o = new FileOutputStream("data/DBApp.class");
		ObjectOutputStream p = new ObjectOutputStream(o);
		p.writeObject(z);

		y.close();
		x.close();
		y.close();

	}

	public static Page getPage(String tableName, String id) throws Exception {
		FileInputStream x = new FileInputStream("data/" + tableName + id + ".class");
		ObjectInputStream y = new ObjectInputStream(x);
		Page z = (Page) y.readObject();
		x.close();
		y.close();
		return z;
	}

	public static tablePages gettablePages(String tableName) throws ClassNotFoundException, IOException {// 000000000000
		FileInputStream x = new FileInputStream("data/" + tableName + ".class");
		ObjectInputStream y = new ObjectInputStream(x);
		tablePages z = (tablePages) y.readObject();
		x.close();
		y.close();
		return z;

	}

	public static void updatetablePages(Page p, int i, String tableName) throws ClassNotFoundException, IOException {// 000000000000
		tablePages t = gettablePages(tableName);
		t.updatetable(p, i, tableName);

	}

	public static void writetablePages(tablePages t, String tableName) throws IOException {// 0000000000000000000000

		FileOutputStream x = new FileOutputStream("data/" + tableName + ".class");
		ObjectOutputStream y = new ObjectOutputStream(x);
		y.writeObject(t);
		x.close();
		y.close();

	}

	public static Hashtable<String, Object> passByValue(Hashtable<String, Object> obj) {
		return obj;
	}

	public static void deletePage(String tableName, int index) {
		File file = new File("data/" + tableName + index + ".class");

		if (file.delete()) {
			System.out.println("File deleted successfully");
		} else {
			System.out.println("Failed to delete the file");
		}
	}

	public static void shift(tablePages x, String tableName) throws ClassNotFoundException, IOException {
		int k = 0;
		for (int i = 0; i < x.Pages.size(); i++) {

			if (((Page) x.Pages.get(i)).pageList.size() == 0) {
				x.Pages.remove(i);
				deletePage(tableName, k);
				i--;

			}

			k++;
		}
		x.writetablePages(tableName);
		editNumberofPage(tableName, x.Pages.size() + "");

		for (int i = 0; i < x.Pages.size() - 1; i++) {

			Page y = (Page) x.Pages.get(i);

			while (!(y.isFull())) {

				Page f = (Page) x.Pages.get(i + 1);
				y.pageList.add(f.pageList.remove(0));

				x.updatetable(f, i + 1, tableName);

				if (f.pageList.size() == 0) {
					x.Pages.remove(i + 1);
					x.writetablePages(tableName);
					editNumberofPage(tableName, x.Pages.size() + "");
					deletePage(tableName, i);

					break;

				}

			}
			x.updatetable(y, i, tableName);

		}

	}

	public static int comparePol(Polygon x, Polygon y) {

		Dimension dim1 = x.getBounds().getSize();
		double nThisArea1 = dim1.width * dim1.height;

		Dimension dim2 = y.getBounds().getSize();
		double nThisArea2 = dim2.width * dim2.height;
		// System.out.println(nThisArea1+" "+nThisArea2);
		if (nThisArea1 > nThisArea2)
			return 1;
		else
			return 0;
	}

	public static void inserttuple(tablePages x, Hashtable y, String tabelName)
			throws ClassNotFoundException, IOException {

		String z = findClusterkey(tabelName);

		for (int i = 0; i < x.Pages.size(); i++) {
			int l = ((Page) x.Pages.get(i)).pageList.size();
			Page f = ((Page) x.Pages.get(i));

			if ((i + 1) == x.Pages.size()) {
				f.addTuple(y, z);
				updatetablePages(f, i, tabelName);
				writePage(tabelName, i + "", f);

				return;
			}

			if (belongs(f, z, f.pageList.get(i).get(z))) {

				if (f.isFull()) {
					Hashtable<String, Object> d = f.pageList.get(l - 1);

					Page b = ((Page) x.Pages.get(i + 1));

					if (!((Page) x.Pages.get(i + 1)).isFull()) {

						f.pageList.remove(l - 1);

						f.addTuple(y, z);
						updatetablePages(f, i, tabelName);
						writePage(tabelName, i + "", f);

						b.addTuple(d, z);

						updatetablePages(b, i + 1, tabelName);
						writePage(tabelName, (i + 1) + "", b);
						return;
					} else {
						int a;
						int j;

						for (j = i; j < x.Pages.size() && ((((Page) x.Pages.get(j)).isFull())); j++) {

							Page g = ((Page) x.Pages.get(j));
							Hashtable<String, Object> r = g.pageList.remove(g.pageList.size() - 1);

							g.addTuple(y, z);
							updatetablePages(g, j, tabelName);
							writePage(tabelName, j + "", f);

							y = passByValue(r);

//					 

						}
						a = j;
						Page g = ((Page) x.Pages.get(a));

						g.addTuple(y, z);

						updatetablePages(g, a, tabelName);
						writePage(tabelName, a + "", f);
						return;

					}

				}

				f.addTuple(y, z);
				updatetablePages(f, i, tabelName);
				writePage(tabelName, i + "", f);
				return;

			}
		}

	}

	public static boolean belongs(Page page, String strClusteringKey, Object value) {
		if (!page.isFull())
			return true;
		String y = page.pageList.get(0).get(strClusteringKey).getClass().getName();

		if (y.equals("java.lang.Integer")) {
			// System.out.println(this.pageList.get(i).get(strClusteringKey)
			// .getClass().getName());
			if (((Integer) page.pageList.get(0).get(strClusteringKey)).compareTo((Integer) value) != 1
					&& ((Integer) page.pageList.get(page.pageList.size() - 1).get(strClusteringKey))
							.compareTo((Integer) value) != -1) {
				return true;
			} else if (((Integer) page.pageList.get(0).get(strClusteringKey)).compareTo((Integer) value) == 1)
				return false;
			else
				return false;
		} else if (y.equals("java.lang.Double")) {
			if (((Double) page.pageList.get(0).get(strClusteringKey)).compareTo((Double) value) != 1
					&& ((Double) page.pageList.get(page.pageList.size() - 1).get(strClusteringKey))
							.compareTo((Double) value) != -1) {
				return true;
			} else if (((Double) page.pageList.get(0).get(strClusteringKey)).compareTo((Double) value) == 1)
				return false;
			else
				return false;
		} else if (y.equals("java.util.Date")) {
			if (((Date) page.pageList.get(0).get(strClusteringKey)).compareTo((Date) value) != 1
					&& ((Date) page.pageList.get(page.pageList.size() - 1).get(strClusteringKey))
							.compareTo((Date) value) != -1) {
				return true;

			} else if (((Date) page.pageList.get(0).get(strClusteringKey)).compareTo((Date) value) == 1)
				return false;
			else
				return false;
		} else if (y.equals("java.lang.String")) {
			if (((String) page.pageList.get(0).get(strClusteringKey)).compareTo((String) value) != 1
					&& ((String) page.pageList.get(page.pageList.size() - 1).get(strClusteringKey))
							.compareTo((String) value) != -1) {
				return true;

			} else if (((String) page.pageList.get(0).get(strClusteringKey)).compareTo((String) value) == 1)
				return false;
			else
				return false;
		} else if (y.equals("java.lang.Boolean")) {
			if (((Boolean) page.pageList.get(0).get(strClusteringKey)).compareTo((Boolean) value) != 1
					&& ((Boolean) page.pageList.get(page.pageList.size() - 1).get(strClusteringKey))
							.compareTo((Boolean) value) != -1) {
				return true;
			} else if (((Boolean) page.pageList.get(0).get(strClusteringKey)).compareTo((Boolean) value) == 1)
				return false;
			else
				return false;
		} else if (y.equals("java.awt.Polygon")) {

			if ((comparePol((Polygon) page.pageList.get(0).get(strClusteringKey), ((Polygon) value))) == 0
					&& comparePol(((Polygon) page.pageList.get(page.pageList.size() - 1).get(strClusteringKey)),
							((Polygon) value)) == 1) {
				return true;
			} else if (comparePol(((Polygon) page.pageList.get(0).get(strClusteringKey)), ((Polygon) value)) == 1)
				return false;
			else
				return false;
		}

		return true;

	}

	public static boolean fullRecord(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException {
		Enumeration<String> colName = htblColNameValue.keys();
		int i = 0;
		while (colName.hasMoreElements()) {
			colName.nextElement();

			i++;

		}
		String line = "";
		String cvsSplitBy = ",";
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		boolean f = false;
		String[] r = null;
		int j = 0;
		while ((line = br.readLine()) != null) {
			// use comma as separator
			r = line.split(cvsSplitBy);
			if (r[0].equals(strTableName))
				j++;

		}
		j--;

		if (i == j)
			return true;

		else
			return false;

	}

	public static Boolean validateTable(String strTableName, Hashtable<String, Object> htblColNameValue) {
		Enumeration<String> colName = htblColNameValue.keys();
		try {
			while (colName.hasMoreElements()) {
				String line = "";
				String cvsSplitBy = ",";
				BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
				String el = colName.nextElement();
				boolean f = false;
				while ((line = br.readLine()) != null) {
					// use comma as separator
					String[] r = line.split(cvsSplitBy);

					if ((r[0].equals(strTableName) && r[1].equals(el)
							&& (((htblColNameValue.get(el)).getClass()).getName()).equals(r[2]))) {
						f = true;

					}

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

	public static boolean clusterisPol(String tableName) throws IOException {

		String line = "";
		String cvsSplitBy = ",";
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
		boolean f = false;
		String[] r = null;

		while ((line = br.readLine()) != null) {
			// use comma as separator
			r = line.split(cvsSplitBy);
			if ((r[0].equals(tableName)) && r[3].equals("True") && r[2].equals("java.awt.Polygon"))
				return true;

		}

		return false;

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		if (checkDuplicates(strTableName)) {

			return;
		}

		else {

			// 000000000000000000000000000000000000
			try {
				WriteCsv(strTableName, strClusteringKeyColumn, htblColNameType);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writeTable(strTableName, strClusteringKeyColumn);
			tablePages x = new tablePages(strTableName);
			writetablePages(x, strTableName);

		}

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {

		if (validateTable(strTableName, htblColNameValue) == false
				|| fullRecord(strTableName, htblColNameValue) == false) {
			System.out.print("wrong input");
			return;
		} else {
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			htblColNameValue.put("TouchDate", dtf.format(now));
			tablePages g = gettablePages(strTableName);
			shift(g, strTableName);
			int x = numberofPages(strTableName);

			if (x == 0) {

				Page P = new Page();
				P.addTuple(htblColNameValue, findClusterkey(strTableName));
				writePage(strTableName, x + "", P);
				editNumberofPage(strTableName, (x + 1) + "");

				tablePages t = gettablePages(strTableName);// 00000000000000000
				t.addpages(P, strTableName);// 0000000000000000

			}

			else {

				Page p1 = getPage(strTableName, (x - 1) + "");

				if ((p1.isFull()) == false) {

////	///		
					inserttuple(g, htblColNameValue, strTableName);// 000000000000000000

				} else {

					Page p2 = new Page();
					g.addpages(p2, strTableName);// 000000000000000000000

					writePage(strTableName, x + "", p2);
					editNumberofPage(strTableName, (x + 1) + "");
					inserttuple(g, htblColNameValue, strTableName);

				}

			}
		}

	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws Exception {
		if (validateTable(strTableName, htblColNameValue) == false) {
			System.out.print("wrong input");
			return;
		} else {
			if (clusterisPol(strTableName) == true) {

				ArrayList<String> g = new ArrayList();

				StringTokenizer tokenizer = new StringTokenizer(strClusteringKey, "),(");

				while (tokenizer.hasMoreElements()) {
					g.add(tokenizer.nextToken());
				}

				boolean f = false;
				int n = numberofPages(strTableName);
				String y = findClusterkey(strTableName);

				for (int i = 0; i < n; i++) {

					Page p = getPage(strTableName, i + "");
					for (int j = 0; j < p.pageList.size(); j++) {
						Polygon h = (Polygon) p.pageList.get(j).get(y);
						boolean z = true;
						int t = 0;
						for (int m = 0; m < g.size(); m = m + 2) {

							if (!(g.get(m).equals("" + h.xpoints[t]) && g.get(m + 1).equals("" + h.ypoints[t]))) {
								z = false;

								break;
							}
							t++;
						}
						if (z == true) {
							f = true;
							DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
							LocalDateTime now = LocalDateTime.now();

							p.pageList.get(j).replace("TouchDate", dtf.format(now));
							// Hashtable<String, Object> g=p.pageList.get(j);
							Enumeration<String> keyValues = htblColNameValue.keys();

							while (keyValues.hasMoreElements()) {

								String k = (String) keyValues.nextElement();
								p.pageList.get(j).replace(k, htblColNameValue.get(k));
								// .pageList.get(j).put(k, htblColNameValue.get(k));
							}

						}
						writePage(strTableName, i + "", p);
						updatetablePages(p, i, strTableName);// 0000000000000000000000

					}

				}
				if (!f)
					System.out.print("row not found");
			}

			else {

				boolean f = false;
				int n = numberofPages(strTableName);
				String y = findClusterkey(strTableName);

				for (int i = 0; i < n; i++) {

					Page p = getPage(strTableName, i + "");
					for (int j = 0; j < p.pageList.size(); j++) {

						if (strClusteringKey.equals(p.pageList.get(j).get(y).toString())) {
							f = true;
							DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
							LocalDateTime now = LocalDateTime.now();

							p.pageList.get(j).replace("TouchDate", dtf.format(now));
							// Hashtable<String, Object> g=p.pageList.get(j);
							Enumeration<String> keyValues = htblColNameValue.keys();

							while (keyValues.hasMoreElements()) {

								String k = (String) keyValues.nextElement();
								p.pageList.get(j).replace(k, htblColNameValue.get(k));

							}

						}

						writePage(strTableName, i + "", p);
						updatetablePages(p, i, strTableName);// 0000000000000000000000
					}
				}
				if (!f)
					System.out.print("row not found");
			}
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {
		int n = numberofPages(strTableName);
		String y = findClusterkey(strTableName);

		tablePages x = gettablePages(strTableName);
		for (int i = 0; i < x.Pages.size(); i++) {
			Page z = (Page) x.Pages.get(i);

			for (int j = 0; j < z.pageList.size(); j++) {
				Enumeration<String> r = htblColNameValue.keys();

				while (r.hasMoreElements()) {
					if (z.pageList.size() == 0)
						break;

					if (j < 0)
						j++;
					String v = r.nextElement();

					if ((htblColNameValue.get(v)).equals(z.pageList.get(j).get(v))) {

						z.pageList.remove(j);
						j--;

//				
//				
						updatetablePages(z, i, strTableName);
						writePage(strTableName, i + "", z);
						break;
					}

				}

			}
		}

	}

}
