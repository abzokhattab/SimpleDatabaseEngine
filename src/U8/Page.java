package U8;

import java.awt.Dimension;
import java.awt.Polygon;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {

	private static final long serialVersionUID = -4544542885377264750L;
	int max;
	public Vector<Hashtable<String, Object>> pageList;

	public Page() {
		pageList = new Vector<Hashtable<String, Object>>();

		File configFile = new File("config/DBApp.config");
		try {
			FileReader reader = new FileReader(configFile);
			Properties props = new Properties();
			props.load(reader);
			max = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));

			reader.close();
		} catch (Exception ex) {

			// I/O error
		}
	}

	public static int comparePol(Polygon x, Polygon y) {

		Dimension dim1 = x.getBounds().getSize();
		double nThisArea1 = dim1.width * dim1.height;

		Dimension dim2 = y.getBounds().getSize();
		double nThisArea2 = dim2.width * dim2.height;

		if (nThisArea1 > nThisArea2)
			return 1;
		else
			return 0;
	}

	public boolean isFull() {
		// System.out.print(pageList.size()+" "+max);
		if (this.pageList.size() >= max) {
			// System.out.println("True");
			return true;
		} else {
			// System.out.println("False");
			return false;
		}

	}

	public void addTuple(Hashtable<String, Object> x, String strClusteringKey) {
		String y = x.get(strClusteringKey).getClass().getName();

		for (int i = 0; i < this.pageList.size(); i++) {
//			System.out.println(this.pageList.get(i).get(strClusteringKey)
//					+ "   " + x.get(strClusteringKey));
			if (y.equals("java.lang.Integer")) {
//				System.out.println(this.pageList.get(i).get(strClusteringKey)
//						.getClass().getName());
				if (((Integer) this.pageList.get(i).get(strClusteringKey))
						.compareTo((Integer) x.get(strClusteringKey)) == 1) {
					this.pageList.add(i, x);
					return;
				}
			} else if (y.equals("java.lang.Double")) {
				if (((Double) this.pageList.get(i).get(strClusteringKey))
						.compareTo((Double) x.get(strClusteringKey)) == 1) {

					this.pageList.add(i, x);
					return;
				}
			} else if (y.equals("java.util.Date")) {
				if (((Date) this.pageList.get(i).get(strClusteringKey))
						.compareTo((Date) x.get(strClusteringKey)) == 1) {
					this.pageList.add(i, x);
					return;
				}
			} else if (y.equals("java.lang.String")) {
				if (((String) this.pageList.get(i).get(strClusteringKey))
						.compareTo((String) x.get(strClusteringKey)) >= 0) {
					this.pageList.add(i, x);
					return;
				}
			} else if (y.equals("java.lang.Boolean")) {
				if (((Boolean) this.pageList.get(i).get(strClusteringKey))
						.compareTo((Boolean) x.get(strClusteringKey)) == 1) {
					this.pageList.add(i, x);
					return;
				}
			} else if (y.equals("java.awt.Polygon")) {

				if (comparePol(((Polygon) this.pageList.get(i).get(strClusteringKey)),
						((Polygon) x.get(strClusteringKey))) == 1) {
					this.pageList.add(i, x);
					System.out.print("l");
					return;
				}
			}
		}

		this.pageList.add(x);

	}

}
