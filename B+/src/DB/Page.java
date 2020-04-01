package DB;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {

	int max = 200;
	public Vector<Hashtable<String, Object>> pageList;

	public Page() {
		pageList = new Vector<Hashtable<String, Object>>();
	}

	public boolean isFull() {

		if (this.pageList.size() == max) {
			return true;
		} else {
			return false;
		}

	}

	public Hashtable<String, Object> addTuple2(Hashtable<String, Object> x,
			String strClusteringKey) {

		for (int i = 0; i < this.pageList.size(); i++) {
			if (((String) this.pageList.get(i).get(strClusteringKey))
					.compareTo(strClusteringKey) == -1) {
				this.pageList.add(i, x);
				if (!this.isFull())
					return null;
				return this.pageList.remove(this.pageList.size() - 1);
			}
		}
		this.pageList.add(x);

		return null;
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
						.compareTo((String) x.get(strClusteringKey)) == 1) {
					this.pageList.add(i, x);
					return;
				}
			} else if (y.equals("java.lang.Boolean")) {
				if (((Boolean) this.pageList.get(i).get(strClusteringKey))
						.compareTo((Boolean) x.get(strClusteringKey)) == 1) {
					this.pageList.add(i, x);
					return;
				}
			}
		}

		this.pageList.add(x);

	}

	public void addrecordIndex(Hashtable<String, Object> x) {

		pageList.add(x);
		// System.out.println(pageList.size());

	}

	public int binarySearch(String strClusteringKey) {
		int l = 0, r = this.pageList.size() - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (this.pageList.get(m).get(strClusteringKey)
					.equals(strClusteringKey))
				return m;

			// If x greater, ignore left half
			if (((String) this.pageList.get(m).get(strClusteringKey))
					.compareTo(strClusteringKey) == -1)
				l = m + 1;

			// If x is smaller, ignore right half
			else
				r = m - 1;
		}

		// if we reach here, then element was
		// not present
		return -1;
	}

	public void ToString() {
		for (int i = 0; i < this.pageList.size(); i++) {
			System.out.println(this.pageList.get(i).get("ID"));
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		;
	}

}
