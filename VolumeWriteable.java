package tfidf; //needs to be changed to reflect our project



//figure out if there is Spark versions of these import statements
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

//output value class
public class VolumeWriteable implements Writable {
	private IntWritable count;
	private MapWritable volumeIds;

	public VolumeWriteable(MapWritable volumeIds, IntWritable count) {
		// #TODO#: Initialize class variables using the constructor parameters
		this.volumeIds = volumeIds;
		this.count = count;
	}

	public VolumeWriteable() {
		// #TODO#: Initialize class variables with appropriate default values
		count = new IntWritable(0);
		volumeIds = new MapWritable();
	}

	public IntWritable getCount() {
		return count;
	}

	public MapWritable getVolumeIds() {
		return volumeIds;
	}

	public void set(MapWritable volumeIds, IntWritable count) {
		// #TODO#: Update class variables with the provided values
		this.volumeIds = volumeIds;
		this.count = count;
	}

	public void insertMapValue(Text key, IntWritable value) {
		// #TODO#: Add the key-value pair to the volumeIds MapWritable
		volumeIds.put(key,value);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		count.readFields(arg0);
		volumeIds.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		count.write(arg0);
		volumeIds.write(arg0);
	}

	@Override
	public String toString() {
		return count.get() + "\t" + volumeIds.size();
	}

	@Override
	public int hashCode() {
		return volumeIds.hashCode();
	}
}