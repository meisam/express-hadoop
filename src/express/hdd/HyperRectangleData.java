package express.hdd;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

@SuppressWarnings("unused")
public class HyperRectangleData implements Iterable<int[]>, Iterator<int[]> {
	//pre-calculated
	private int [] dataSize;
	private int [] coffset;
	private int [] clength;
	private int dimension;
	//calculated
	private int NumOfChunks;
	//for iteration
	private int[] _cursor;
	private int[] cursorBoundary;

	public HyperRectangleData(int[] dataSize, int[] coffset, int[] clength, int dimension){
		this.dataSize = dataSize.clone();
		this.coffset = coffset.clone();
		this.clength = clength.clone();
		this.dimension = dimension;
		
		this.cursorBoundary = new int[dimension];
		NumOfChunks = 1;
	     for (int i=0; i<dimension; i++) {
	    	 NumOfChunks = NumOfChunks * dataSize[i]/clength[i];
	    	 cursorBoundary[i] = dataSize[i]/clength[i];
	     }
	}
	
	public static int getVolume(int[] length){
		return Tools.cumulativeProduction(length);
	}
	
	public int getNumOfChunks(){
		return NumOfChunks;
	}
	
	public int[] getChunkLength(){
		return clength;
	}
	
	public int getChunkSize() {
		return getVolume(clength);
	}
	
	public int getChunkNumber(int[] offset) {
		int[] coord = new int[dimension];
		for (int i=0; i<offset.length; i++)
			coord[i] = offset[i]/clength[i];
		
		int num = coord[0];
		int len = 1;
		for (int i=1; i<coord.length; i++) {
			len = len * dataSize[i-1]/clength[i-1];
			num = num + coord[i] * len;
		}
		return num;
	}
	
	public int[] getChunkOffsetByNumber(int cid) {
		if (cid >= NumOfChunks)
			return null;
		int[] coff = new int[dimension];
		//TODO: init with offset
		for (int i=0, base=1; i<dimension; i++) {
			coff[i] = (cid / base) % cursorBoundary[i];
			base = base * cursorBoundary[i];			 
		}
		
		int[] ret = new int[dimension];
		for (int i=0; i<dimension; i++)
			ret[i] = coff[i] * clength[i];
		
		return ret;
	}
	
	public static void TestGetChunkOffsetByNumber(){
		int dataSize[] = {8,4,8};
		int chunkOffset[] = {0,0,0};
		int chunkLength[] = {4,2,2};

		HyperRectangleData testRec = new HyperRectangleData(dataSize, chunkOffset, chunkLength, 3);
		for (int i=0; i<16; i++)
			System.out.println("TestGetChunkOffsetByNumber:: chunk[" + i + "] " + Arrays.toString(testRec.getChunkOffsetByNumber(i)));
	}
	
	public int[] getOverlappedChunks(int[] offset, int[] length){
		int[] overlappedVolume = new int[NumOfChunks];
		int zeroCount = 0; 
		for (int i=0,j=0; i<NumOfChunks; i++){
			int[] coff = getChunkOffsetByNumber(i);
			Pair<int[], int[]> xrect =
				getHyperRectangleIntersection(coff, clength, offset, length, dimension);
			if (xrect == null) {
				zeroCount++;
				overlappedVolume[i] = 0;
			} else
				overlappedVolume[i] = getVolume(xrect.getRight());
		}
		// remove all zero entries
		if (NumOfChunks == zeroCount)
			return null;
		int[] ret = new int[NumOfChunks - zeroCount];
		for (int i=0,j=0; i<NumOfChunks; i++)
			if (overlappedVolume[i] > 0) {
				ret[j] = i;
				j++;
			}
		
		return ret;
	}

	public static void TestGetOverlappedChunks(){
		int dataSize[] = {4,4,4};
		int chunkOffset[] = {0,0,0};
		int chunkLength[] = {1,1,1};
		
		int boffset[] = {1,1,2};
		int blength[] = {2,2,2};

		HyperRectangleData testRec = new HyperRectangleData(dataSize, chunkOffset, chunkLength, 3);
		System.out.println("TestGetOverlappedChunks:: " + Arrays.toString(testRec.getOverlappedChunks(boffset, blength)));
	}
	
	public static int[] getRelativeOffset(int[] base, int[] incr, int dim){
		if (base.length > dim || incr.length > dim)
			return null;
		int[] res = new int[dim];
		for(int i=0; i<dim; i++)
			res[i] = incr[i] - base[i]; 
		return res;
	}
	
	/**
	 * @param a,b,c,d
	 * @return (m1,m2)
	 * 
	 * input line sections (a,b) and (c,d).
	 * sort a,b,c,d as m0,m1,m2,m3
	 * output line intersection (m1,m2)
	 *     or null
	 */
	public static int[] getIntersection(final int a, final int b, final int c, final int d) {
		int res[] = new int[2];
		if (b <= c || d <= a) {		
			return null;
		} 
		
		int m[] = {a,b,c,d};
		Arrays.sort(m);
		res[0] = m[1]; res[1] = m[2];
		
		//System.out.printf("FUNC getIntersection(%d,%d,%d,%d) = (%d, %d)\n", a,b,c,d,res[0],res[1]);
		
		return res;
	}
	
	/**
	 * @param a,al,b,bl,dimension
	 * @return c
	 * 
	 * input two hyperrectangles (a,al) and (b,bl), with the format (offset, length)
	 * output intersection hyperrectangle(c,cl)
	 *     or null
	 */
	public static Pair<int[], int[]> getHyperRectangleIntersection(final int[] a, final int[] al, final int[] b, final int[] bl, final int dim){
		int[] offset = new int[dim]; 
		int[] length = new int[dim];
		Pair<int[], int[]> c = new Pair<int[],int[]>(offset, length);
		
		for (int i=0; i<dim; i++){
			int [] tmp = getIntersection(a[i], a[i] + al[i], b[i], b[i] + bl[i]);
			if (tmp == null)
				return null;
			offset[i] = tmp[0];
			length[i] = tmp[1] - tmp[0];
		}
		return c;
	}
	
	public static Pair<int[], int[]> getHyperRectangleIntersection(Pair<int[], int[]> a, Pair<int[], int[]> b, final int dim){
		return getHyperRectangleIntersection(a.getLeft(), a.getRight(), b.getLeft(), b.getRight(), dim);
	}
	
	public static Pair<int[], int[]> getHyperRectangleIntersection(Pair<int[], int[]> a, Pair<int[], int[]> b){
		if (a.getLeft().length != b.getLeft().length)
			return null;
		return getHyperRectangleIntersection(a.getLeft(), a.getRight(), b.getLeft(), b.getRight(), a.getLeft().length);
	}
	
	public static boolean getChunkOfHighDimData(byte[] data, int[] dataSize, int dim, byte[] buffer, int[] chunkOffset, int[] chunkLength) {
		if (dim <= 0 || dataSize.length < dim || chunkLength.length < dim || chunkOffset.length < dim)
			return false;
		
		ByteBuffer chunkBuffer = ByteBuffer.wrap(buffer);
		int remain = Tools.cumulativeProduction(chunkLength);
		int cursor[] = new int[dim];
		int length = chunkLength[0];
		
		//calculate skip array
		int skipForward[] = new int[dim];
		int skipBackward[] = new int[dim];
		skipForward[0] = 1;
		for (int i=1; i<dim; i++){
			skipForward[i] = skipForward[i-1] * dataSize[i-1];
			skipBackward[i] = skipForward[i] * chunkLength[i];
		}
		int offset = 0;
		for (int i=0; i<dim; i++)
			offset = offset + skipForward[i] * chunkOffset[i];
		//System.out.println("skipForward: " + Arrays.toString(skipForward));
		//System.out.println("skipBackward: " + Arrays.toString(skipBackward));
			
		while (remain > 0){
			int index = 1;
			//copy data
			//System.out.println("offset:" + offset + "; length:" + length);
			chunkBuffer.put(data, offset, length);
			remain = remain - length;
			//update cursor and offset
			cursor[index] = cursor[index] + 1;
			offset = offset + skipForward[index];
			while (cursor[index] >= chunkLength[index]) {
				//reset cursor in current dimension  
				cursor[index] = 0;
				offset = offset - skipBackward[index];
				
				//forward cursor in higher dimension
				index = index + 1;
				if(index >= dim)
					break;
				cursor[index] = cursor[index] + 1;
				offset = offset + skipForward[index]; 
			}
		}
		return true;
	}
	
	public static boolean putChunkIntoHighDimData(byte[] data, int[] dataSize, int dim, byte[] buffer, int[] chunkOffset, int[] chunkLength){
		if (dim <= 0 || dataSize.length < dim || chunkLength.length < dim || chunkOffset.length < dim)
			return false;
		
		ByteBuffer chunkBuffer = ByteBuffer.wrap(buffer);
		int remain = Tools.cumulativeProduction(chunkLength);
		int cursor[] = new int[dim];
		int length = chunkLength[0];
		
		//calculate skip array
		int skipForward[] = new int[dim];
		int skipBackward[] = new int[dim];
		skipForward[0] = 1;
		for (int i=1; i<dim; i++){
			skipForward[i] = skipForward[i-1] * dataSize[i-1];
			skipBackward[i] = skipForward[i] * chunkLength[i];
		}
		int offset = 0;
		for (int i=0; i<dim; i++)
			offset = offset + skipForward[i] * chunkOffset[i];
		//System.out.println("skipForward: " + Arrays.toString(skipForward));
		//System.out.println("skipBackward: " + Arrays.toString(skipBackward));
			
		while (remain > 0){
			int index = 1;
			//copy data
			//System.out.println("offset:" + offset + "; length:" + length);
			chunkBuffer.get(data, offset, length);
			remain = remain - length;
			//update cursor and offset
			cursor[index] = cursor[index] + 1;
			offset = offset + skipForward[index];
			while (cursor[index] >= chunkLength[index]) {
				//reset cursor in current dimension  
				cursor[index] = 0;
				offset = offset - skipBackward[index];
				
				//forward cursor in higher dimension
				index = index + 1;
				if(index >= dim)
					break;
				cursor[index] = cursor[index] + 1;
				offset = offset + skipForward[index]; 
			}
		}
		return true;
	}
	
	public static void printHighDimData(byte[] data, int[] dim){
		if (dim.length < 3)
			return;
		int cursor[] = new int[dim.length];
		int remain = Tools.cumulativeProduction(dim);
		int decrease = dim[0] * dim[1];
		int offset = 0;
		int index = 2;
		while(remain > 0){
			//print cursor
			System.out.printf("CURSOR=(*,*");
			for(int i=2; i<dim.length; i++){
				System.out.printf(",%d", cursor[i]);
			}
			System.out.printf(")\n");
			
			//print data
			for (int i=0; i<dim[1]; i++) {
				for (int j=0; j<dim[0]; j++) {
					int pos = offset + i * dim[0] + j;
					System.out.printf("%s\t", Byte.toString(data[pos]));
				}
				System.out.printf("\n");
			}
			//update remain and offset
			remain = remain - decrease;
			offset = offset + decrease;
					
			//update cursor
			index = 2;
			cursor[index] = cursor[index] + 1;
			while (cursor[index] >= dim[index]){
				cursor[index] = 0;
				index = index + 1;
				if(index >= dim.length)
					break;
				cursor[index] = cursor[index] + 1;
			}
		}		
	}

	// iterate all chunk offsets
	@Override
	public Iterator<int[]> iterator() {
		this._cursor = new int[dimension+1];
		return this;
	}

	@Override
	public boolean hasNext() {
		if(_cursor[dimension] > 0)
			return false;
		else
			return true;
	}

	@Override
	public int[] next() {
		int[] offset = new int[dimension];
		for (int i=0; i<dimension; i++)
			offset[i] = _cursor[i] * clength[i];
		//update cursor
		int index = 0;
		_cursor[index] = _cursor[index] + 1;
		while (_cursor[index] >= cursorBoundary[index]){
			_cursor[index] = 0;
			index = index + 1;
			_cursor[index] = _cursor[index] + 1;
			if (index >= dimension)
				break;
		}
		return offset;
	}

	@Override
	public void remove() {
		// unused		
	}
	
	public static void test(){
		System.out.printf("TEST getIntersection(3,8,4,10) = (%d, %d)\n", HyperRectangleData.getIntersection(3,8,4,10)[0], 
				HyperRectangleData.getIntersection(3,8,4,10)[1]);
		int[] a = {0,0,0}; int[] al = {10,10,10};
		int[] b = {5,5,5}; int[] bl = {3,4,10};
		System.out.printf("TEST getHyperRectangleIntersection([0,0,0],[10,10,10],[5,5,5],[3,4,10]) = ([%d,%d,%d],[%d,%d,%d])\n"
				, HyperRectangleData.getHyperRectangleIntersection(a,al,b,bl,3).getLeft()[0],
				HyperRectangleData.getHyperRectangleIntersection(a,al,b,bl,3).getLeft()[1],
				HyperRectangleData.getHyperRectangleIntersection(a,al,b,bl,3).getLeft()[2],
				HyperRectangleData.getHyperRectangleIntersection(a,al,b,bl,3).getRight()[0],
				HyperRectangleData.getHyperRectangleIntersection(a,al,b,bl,3).getRight()[1],
				HyperRectangleData.getHyperRectangleIntersection(a,al,b,bl,3).getRight()[2]);
		
		byte[] data = new byte[27];
		byte[] buffer = new byte[8];
		for (int j=0; j<3; j++)
			for(int i=0; i < 9; i++)
				data[i+j*9] = (byte)('0' + i);
		int dataSize[] = {3,3,3};
		int chunkOffset[] = {1,1,1};
		int chunkOffset2[] = {1,0,0};
		int chunkLength[] = {2,2,2};

		System.out.printf("@@@DATA:\n");
		System.out.println(Arrays.toString(data));
		HyperRectangleData.printHighDimData(data, dataSize);
		HyperRectangleData.getChunkOfHighDimData(data, dataSize, 3, buffer, chunkOffset, chunkLength);
		System.out.printf("@@@BUFFER:\n");
		HyperRectangleData.printHighDimData(buffer, chunkLength);
		HyperRectangleData.putChunkIntoHighDimData(data, dataSize, 3, buffer, chunkOffset2, chunkLength);
		System.out.printf("@@@DATA:\n");
		HyperRectangleData.printHighDimData(data, dataSize);

		int[] dsize = {8,16,8}; int[] coffset = {0,0,0}; int[] clength = {2,8,4};
		HyperRectangleData recData = new HyperRectangleData( dsize, coffset, clength, 3); 
		Iterator<int[]> chunkOffsetIterator = recData.iterator();
		while(chunkOffsetIterator.hasNext()){
			int[] offset = chunkOffsetIterator.next();
			System.out.println(Arrays.toString(offset));
		}
	}
}
