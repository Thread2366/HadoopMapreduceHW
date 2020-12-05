package bd.homework1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;

public class HW1Reducer extends Reducer<Text, IntWritable, CityWritable, IntWritable> {

    private Hashtable<String, String> cityNames;
    private final CityWritable cityWritable = new CityWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        //подсчет количества полученных из маппера записей
        int count = 0;
        while (values.iterator().hasNext()) {
            count += values.iterator().next().get();
        }

        //получение названия города по его идентификатору из хэш-таблицы
        String cityName;
        if (cityNames == null) {
            cityName = "";
        }
        else {
            cityName = cityNames.getOrDefault(key.toString(), null);
        }

        //запись идентификатора города с его названием и их количества
        cityWritable.set(key.toString(), cityName);
        context.write(cityWritable, new IntWritable(count));
    }

    @Override
    protected void setup(Context context) throws IOException {

        //получение кеш файлов
        URI[] uris = context.getCacheFiles();

        if (uris == null) return;

        List<String> lines = null;
        for (URI uri : uris) {
            Path path = new Path(uri);
            if (!path.getName().contains("city.en.txt")) continue;

            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            lines = reader.lines().collect(Collectors.toList()); //запись всех строк кеш файла в список
            reader.close();
        }

        if (lines == null) return;

        //заполнение хэш-таблицы с идентификаторами городов и их названиями
        cityNames = new Hashtable<>();
        for (String line : lines) {
            String[] tokens = line.split("\\s");
            cityNames.put(tokens[0], tokens[1]);
        }
    }
}
