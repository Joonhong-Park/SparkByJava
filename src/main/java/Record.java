import java.io.Serializable;

public class Record implements Serializable {
    String DEST_COUNTRY_NAME;
    String ORIGIN_COUNTRY_NAME;
    int count;

    public Record(String DEST_COUNTRY_NAME, String ORIGIN_COUNTRY_NAME, int count) {
        this.DEST_COUNTRY_NAME = DEST_COUNTRY_NAME;
        this.ORIGIN_COUNTRY_NAME = ORIGIN_COUNTRY_NAME;
        this.count = count;
    }

    public String getDEST_COUNTRY_NAME() {
        return DEST_COUNTRY_NAME;
    }

    public String getORIGIN_COUNTRY_NAME() {
        return ORIGIN_COUNTRY_NAME;
    }

    public int getCount() {
        return count;
    }

    public void setDEST_COUNTRY_NAME(String DEST_COUNTRY_NAME) {
        this.DEST_COUNTRY_NAME = DEST_COUNTRY_NAME;
    }

    public void setORIGIN_COUNTRY_NAME(String ORIGIN_COUNTRY_NAME) {
        this.ORIGIN_COUNTRY_NAME = ORIGIN_COUNTRY_NAME;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
