package input;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightDataWritable implements WritableComparable<FlightDataWritable> {

    private String DEST_COUNTRY_NAME;
    private String ORIGIN_COUNTRY_NAME;
    private String count;

    public FlightDataWritable(Text text){
        String[] tokens = text.toString().split(",");

        DEST_COUNTRY_NAME = tokens[0];
        ORIGIN_COUNTRY_NAME = tokens[1];
        count = tokens[2];
    }

    public String getDEST_COUNTRY_NAME() {
        return DEST_COUNTRY_NAME;
    }

    public String getORIGIN_COUNTRY_NAME() {
        return ORIGIN_COUNTRY_NAME;
    }

    public String getCount() {
        return count;
    }

    @Override
    public int compareTo(FlightDataWritable o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, DEST_COUNTRY_NAME);
        WritableUtils.writeString(out, ORIGIN_COUNTRY_NAME);
        WritableUtils.writeString(out, count);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        DEST_COUNTRY_NAME = WritableUtils.readString(in);
        ORIGIN_COUNTRY_NAME = WritableUtils.readString(in);
        count = WritableUtils.readString(in);
    }
}
