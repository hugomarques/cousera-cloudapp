import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class PopularityLeague extends Configured implements Tool {

    private static final String DEL = ": ";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job linkCountJob = Job.getInstance(conf, "Link Count");

        linkCountJob.setOutputKeyClass(IntWritable.class);
        linkCountJob.setOutputValueClass(IntWritable.class);

        linkCountJob.setMapperClass(LinkCountMap.class);
        linkCountJob.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(linkCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(linkCountJob, tmpPath);

        linkCountJob.setJarByClass(PopularityLeague.class);

        linkCountJob.waitForCompletion(true);

        return 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                String link = stringTokenizer.nextToken().trim();
                if (!link.endsWith(":")) {
                    context.write(new IntWritable(Integer.valueOf(link)), new IntWritable(1));
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        Job popularityLeague = Job.getInstance(this.getConf(), "PopularityLeague");

        popularityLeague.setMapOutputKeyClass(NullWritable.class);
        popularityLeague.setMapOutputValueClass(IntArrayWritable.class);

        popularityLeague.setOutputKeyClass(IntWritable.class);
        popularityLeague.setOutputValueClass(IntWritable.class);

        popularityLeague.setMapperClass(PopularLeagueMap.class);
        popularityLeague.setReducerClass(PopularLeagueReduce.class);

        FileInputFormat.setInputPaths(popularityLeague, tmpPath);
        FileOutputFormat.setOutputPath(popularityLeague, new Path(args[1]));

        popularityLeague.setInputFormatClass(KeyValueTextInputFormat.class);
        popularityLeague.setOutputFormatClass(TextOutputFormat.class);

        popularityLeague.setJarByClass(PopularityLeague.class);

        return popularityLeague.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {

        public static final Log log = LogFactory.getLog(LinkCountMap.class);

        List<String> leagueLinks = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leagueLinksPath = conf.get("league");

            leagueLinks = Arrays.asList(readHDFSFile(leagueLinksPath, conf).split("\n"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), DEL);
            String parent = stringTokenizer.nextToken();
            if (leagueLinks.contains(parent)) {
                log.info("PARENT: "+parent);
                context.write(new IntWritable(Integer.valueOf(parent)), new IntWritable(0));
            }
            while (stringTokenizer.hasMoreTokens()) {
                String child = stringTokenizer.nextToken().trim();
                if (leagueLinks.contains(child)) {
                    log.info("CHILD: "+child);
                    context.write(new IntWritable(Integer.valueOf(child)), new IntWritable(1));
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public static final Log log = LogFactory.getLog(LinkCountReduce.class);

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            log.info(String.format("KEY, VALUE: [%s, %s]", key.get(), sum));
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PopularLeagueMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {

        public static final Log log = LogFactory.getLog(PopularLeagueMap.class);

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer link = Integer.parseInt(key.toString());
            Integer count = Integer.parseInt(value.toString());
            Integer[] integers = {link, count};
            IntArrayWritable val = new IntArrayWritable(integers);
            context.write(NullWritable.get(), val);
        }
    }

    public static class PopularLeagueReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

            TreeSet<Pair<Integer, Integer>> links = new TreeSet<>();
            for (IntArrayWritable val: values) {
                IntWritable[] integers = (IntWritable[])val.toArray();
                links.add(new Pair<>(integers[1].get(), integers[0].get()));
            }
            int index = 0;
            int acc = 1;
            Integer previousVal = null;
            for (Pair<Integer, Integer> pair:links) {
                if (previousVal != null && !pair.first.equals(previousVal)) {
                    index += acc;
                    acc = 1;
                } else if (previousVal != null) {
                    acc++;
                }
                context.write(new IntWritable(pair.second), new IntWritable(index));
                previousVal = pair.first;
            }


        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
>>>>>>> Finished MP2 and 3 questions from MP3
