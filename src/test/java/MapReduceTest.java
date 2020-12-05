import bd.homework1.CityWritable;
import bd.homework1.HW1Mapper;
import bd.homework1.HW1Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapReduceTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, CityWritable, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, CityWritable, IntWritable> mapReduceDriver;

    private final URI cacheUri = new Path("file:///home/islam/Downloads/city.en.txt").toUri();

    private String getInputString(String cityId, String price) {
        return getInputString(cityId, price, " ");
    }

    private String getInputString(String fillValue) {
        return getInputString(fillValue, fillValue, fillValue);
    }

    private String getInputString(String cityId, String price, String fillValue) {
        String[] tokens = new String[24];
        Arrays.fill(tokens, fillValue);
        tokens[7] = cityId;
        tokens[19] = price;
        return String.join("\t", tokens);
    }

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        String input = getInputString("226", "277");
        mapDriver
                .withInput(new LongWritable(), new Text(input))
                .withOutput(new Text("226"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapperEmptyString() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(""))
                .runTest();
    }

    @Test
    public void testMapperTrashString() throws IOException {
        String input = getInputString("?");
        mapDriver
                .withInput(new LongWritable(), new Text(input))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("7"), values)
                .withCacheFile(cacheUri)
                .withOutput(new CityWritable("7", "handan"), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testReducerWithoutCache() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("7"), values)
                .withOutput(new CityWritable("7", null), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducerWithInvalidCacheUri() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("7"), values)
                .withCacheFile(new Path("file:///home/islam/Downloads/qwerty.txt").toUri())
                .withOutput(new CityWritable("7",null), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testReducerNoNameCity() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("1"), values)
                .withCacheFile(cacheUri)
                .withOutput(new CityWritable("1", null), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        String input1 = getInputString("7", "300");
        String input2 = getInputString("7", "300");
        String input3 = getInputString("7", "250");
        String input4 = getInputString("sometext", "300");

        mapReduceDriver
                .withInput(new LongWritable(), new Text(input1))
                .withInput(new LongWritable(), new Text(input2))
                .withInput(new LongWritable(), new Text(input3))
                .withInput(new LongWritable(), new Text(input4))
                .withCacheFile(cacheUri)
                .withOutput(new CityWritable("7", "handan"), new IntWritable(2))
                .withOutput(new CityWritable("sometext", null), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapReduceUnknownCity() throws IOException {
        String input1 = getInputString("0", "300");
        String input2 = getInputString("0", "sometext");

        mapReduceDriver
                .withInput(new LongWritable(), new Text(input1))
                .withInput(new LongWritable(), new Text(input2))
                .withCacheFile(cacheUri)
                .withOutput(new CityWritable("0", "unknown"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapReduceNoNameCity() throws IOException {
        String input = getInputString("1", "300");

        mapReduceDriver
                .withInput(new LongWritable(), new Text(input))
                .withCacheFile(cacheUri)
                .withOutput(new CityWritable("1", null), new IntWritable(1))
                .runTest();
    }
}
