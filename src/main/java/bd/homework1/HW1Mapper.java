package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text cityId = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String row = value.toString();
        String[] tokens = row.split("\t"); //входной текст разбивается по табам
        if (tokens.length < 24) return; //проверка наличия значений всех атрибутов

        //если BiddingPrice - не число, то результат не записывается
        Integer price = null;
        try {
            price = Integer.parseInt(tokens[19]);
        }
        catch (NumberFormatException ignored) {
            return;
        }

        //если BiddingPrice больше 250, то записывается CityId и единица
        if (price > 250) {
            cityId.set(tokens[7]);
            context.write(cityId, one);
        }
    }
}
