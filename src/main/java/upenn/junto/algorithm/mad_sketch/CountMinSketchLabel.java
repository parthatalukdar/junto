package upenn.junto.algorithm.mad_sketch;

public class CountMinSketchLabel {
	public float[][] table;
	public int depth;
	public int width; 
	
	private CountMinSketchLabel() {}
	
	public CountMinSketchLabel(int depth, int width) {
		this.depth = depth;
		this.width = width;
		this.table = new float[depth][width];
		
		for (int di = 0; di < this.depth; ++di) {
			for (int wi = 0; wi < this.width; ++wi) {
				table[di][wi] = 0; 
			}
		}
	}
}
