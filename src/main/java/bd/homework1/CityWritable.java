package bd.homework1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CityWritable implements Writable {

    private String cityId;
    private String cityName;

    public CityWritable() { }

    public CityWritable(String cityId, String cityName) {
        set(cityId, cityName);
    }

    public String getCityId() {
        return cityId == null ? "" : cityId;
    }

    public void setCityId(String cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName == null ? "" : cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public void set(String cityId, String cityName) {
        setCityId(cityId);
        setCityName(cityName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getCityId());
        dataOutput.writeUTF(getCityName());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cityId = dataInput.readUTF();
        cityName = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)",
                getCityId(),
                getCityName());
    }

    //переопределено для работы testReducer
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CityWritable)) return false;
        CityWritable that = ((CityWritable) o);
        return this.toString().equals(that.toString());
    }
}
