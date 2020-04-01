package DB;
import java.util.*;
//Java program for implementation of Insertion Sort 
public class InsertionSort { 
	/*Function to sort array using insertion sort*/
	void sort(Vector <Integer> a ,int x) 
	{ 
		for (int i =0 ;i<a.size();i++){
       if (a.get(i)>=x){
         a.add(i,x);
         return ;
       }
     }
        a.add(a.size(),x);

 }

	

	/* A utility function to print array of size n*/
	static void printArray(Vector <Integer>arr) 
	{ 
		int n = arr.size(); 
		for (int i = 0; i < n; ++i) 
			System.out.print(arr.get(i) + " "); 

		System.out.println(); 
	} 

	// Driver method 
	public static void main(String args[]) 
	{ 
		Vector<Integer> a  = new Vector <Integer>();
		a.add(1);
		a.add(2);
		a.add(4);
		a.add(5);
		a.add(7);
		a.add(10);


		InsertionSort ob = new InsertionSort(); 
		ob.sort(a,15); 

		printArray(a); 
	} 
} /* This code is contributed by Rajat Mishra. */
