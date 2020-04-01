package DB;
import java.io.Serializable;
import java.util.Vector;
public class Table implements Serializable{
	String strClustringKey;
	Vector<Page> pages ; 
public Table(){
	this.pages = new Vector<Page>();
	this.strClustringKey= strClustringKey;
}

public void insertPage(Page a){
	pages.add(a);
}
public int getSize(){
	return this.pages.size();
}
public void deleteLast(){
	this.pages.remove(this.pages.size()-1);
}
}
