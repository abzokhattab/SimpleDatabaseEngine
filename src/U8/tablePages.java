package U8;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class tablePages implements Serializable {
	ArrayList Pages;
	private static final long serialVersionUID = 6529685098267757690L;

	public tablePages(String tableName) throws IOException {
		Pages = new ArrayList();

	}

	public void updatetable(Page p, int i, String tableName) throws IOException {
		Pages.remove(i);
		Pages.add(i, p);
		writetablePages(tableName);
	}

	public void addpages(Page p, String tableName) throws IOException {
		Pages.add(p);
		this.writetablePages(tableName);

	}

	public void writetablePages(String tableName) throws IOException {
		FileOutputStream x = new FileOutputStream("data/" + tableName + ".class");
		ObjectOutputStream y = new ObjectOutputStream(x);
		y.writeObject(this);
		x.close();
		y.close();
	}

}
