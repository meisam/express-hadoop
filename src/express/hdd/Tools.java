package express.hdd;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

public class Tools {
	public static int[] StringArray2IntArray(String[] sarray) throws Exception {
		if (sarray != null) {
			int intarray[] = new int[sarray.length];
			for (int i = 0; i < sarray.length; i++) {
					intarray[i] = Integer.parseInt(sarray[i]);
			}
			return intarray;
		}
		return null;
	}
	
	public static int cumulativeProduction(int[] nums){
		int res = 1; 
		for (int i=0; i<nums.length; i++)
			res = res * nums[i];
		return res;
	}
	
	public static int[] vectorDotX(int[] a, int[] b) {
		if (a.length != b.length)
			return null;
		int[] ret = new int[a.length];
		for (int i=0; i<a.length; i++)
			ret[i] = a[i] * b[i];
		return ret;
	}
	
	public static int[] mergeSortedArray(int[] a, int c){
		int[] b = new int[1];
		b[0] = c;
		return mergeSortedArray(a, b);
	}
	
	public static int[] mergeSortedArray(int a, int[] b){
		return mergeSortedArray(b, a);
	}
	
	public static int[] mergeSortedArray(int[] a, int[] b){
		if (a == null)
			return b;
		if (b == null)
			return a;
		
		int i=0, j=0, repeat=0;
		while(i<a.length && j<b.length){
			if (a[i] == b[j]) {
				repeat++;
				i++; j++;
			} else if (a[i] > b[j])
				j++;
			else if (a[i] < b[j])
				i++;
		}
		
		int[] ret = new int[a.length + b.length - repeat];
		i=0; j=0;
		int k = 0;
		while(i<a.length && j<b.length){
			if (a[i] == b[j]) {
				ret[k] = a[i];
				k++; i++; j++;
			}else if (a[i] > b[j]) {
				ret[k] = b[j]; 
				k++; j++;
			}else if (a[i] < b[j]) {
				ret[k] = a[i];
				k++; i++;
			}
		}
		
		while (i<a.length){
			ret[k] = a[i];
			k++; i++;
		}
		
		while (j<b.length){
			ret[k] = b[j];
			k++; j++;
		}
		
		return ret;
	}
	
	public static void TestOperation4SortedArray(){
		int[] a = {1, 5, 6, 11, 14};
		int[] b = {2, 6, 7, 14};
		System.out.println("merge " + Arrays.toString(a) + " and " + Arrays.toString(b) + " yield " + 
				Arrays.toString(Tools.mergeSortedArray(a,b)));
		System.out.println("intersect " + Arrays.toString(a) + " and " + Arrays.toString(b) + " yield " + 
				Arrays.toString(Tools.intersectSortedArray(a,b)));
	}
	
	public static int[] intersectSortedArray(int[] a, int[] b){
		if ((a == null) || (b == null))
			return null;
		
		int i=0, j=0, repeat=0;
		while(i<a.length && j<b.length){
			if (a[i] == b[j]) {
				repeat++;
				i++; j++;
			} else if (a[i] > b[j])
				j++;
			else if (a[i] < b[j])
				i++;
		}
		
		int[] ret = new int[repeat];
		i=0; j=0;
		int k = 0;
		while(i<a.length && j<b.length){
			if (a[i] == b[j]) {
				ret[k] = a[i];
				k++; i++; j++;
			}else if (a[i] > b[j]) { 
				j++;
			}else if (a[i] < b[j]) {
				i++;
			}
		}
		
		return ret;
	}
	
	public static boolean isWithinSortedArray(int x, int[] a){
		if (a == null)
			return false;
		
		for (int i=0; i<a.length; i++)
			if (x == a[i])
				return true;
		return false;
	}
	
	public static int[] getHDFVectorFromConf(JobConf job, String name) throws Exception {
		return StringArray2IntArray(job.get(name).toString().split(","));
	}
	

	/**
	 * @param text
	 * @return (offset,length)
	 * @throws Exception 
	 */
	public static Pair<int[], int[]> text2Pair(final Text key) throws Exception{
		String[] pair = key.toString().split(";");
		
		//System.out.println("Pair has " + pair.length + " elements");
		//for (int i=0; i<pair.length; i++)
		//	System.out.println("Pair (" + i + ") = " + pair[i]);
		String[] soffset = pair[0].replace("[", "").replace("]", "").replace(" ", "").split(",");
		String[] slength = pair[1].replace("[", "").replace("]", "").replace(" ", "").split(",");
		slength[slength.length-1] = slength[slength.length-1].trim();
		//for (int i=0; i<slength.length; i++)
		//	System.out.println("Length (" + i + ") = [" + slength[i] + "]");
		int[] offset = Tools.StringArray2IntArray(soffset); 
		//System.out.println("offset has " + offset.length + " elements");
		int[] length = Tools.StringArray2IntArray(slength);		
		//System.out.println("length has " + length.length + " elements");
		
		Pair<int[], int[]> c = new Pair<int[],int[]>(offset, length);
		return c;  
	}
	
	/**
	 * @param (offset,length)
	 * @return text
	 * @throws Exception 
	 */
	public static Text pair2Text(final int[] offset, final int[] length) throws Exception{
		Text pair = new Text();
		pair.set((Arrays.toString(offset)+ ";" + Arrays.toString(length)+ "\t").getBytes());  
		return pair;
	}	
	
	public static String list2String(List<Integer> list) {
		if (list == null)
			return "";
		String listString = "";
		for (Integer s : list)
		{
		    listString += s.toString() + " ";
		}
		return listString;
	}
}
