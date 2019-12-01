import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Toptenbuy {

    public static class TopMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mkey = new Text();
        private Text mvalue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map");

            String[] lines = value.toString().split(",");

            if (lines.length < 1 || !lines[7].equals("2"))
                return;

            mkey.set(lines[10]);
            mvalue.set(lines[1]);
            context.write(mkey, mvalue);
        }
    }

    static class MyCom implements Comparable<MyCom> {
        private String name;
        private Long num;

        MyCom(String name) {
            this.name = name;
            num = 1L;
        }

        String getName(){
            return name;
        }

        Long getNum(){
            return num;
        }

        void addOne(){
            this.num = num + 1;
        }

        @Override
        public int compareTo(MyCom t){
            return this.num.compareTo(t.getNum());
        }
    }

    public static class TopReducer extends Reducer<Text, Text, Text, Text>{
        private Text rvalue = new Text();
        @Override
        protected void reduce(Text mkey, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
            System.out.println("reduce");
            List<MyCom> glist = new ArrayList<>();

            for (Text it : iter){
                String name = it.toString();

                boolean flag = false;
                for (MyCom myCom : glist) {
                    if (name.equals(myCom.getName())) {
                        flag = true;
                        myCom.addOne();
                        break;
                    }
                }
                if (!flag){
                    MyCom tmp = new MyCom(name);
                    glist.add(tmp);
                }
            }

            Collections.sort(glist);
            Collections.reverse(glist);

            for (int k = 0; k < 10; k ++){
                MyCom g = glist.get(k);
                rvalue.set("No." + (k + 1) + " Good: "+ g.getName() + " Bought by " + g.getNum());
                context.write(mkey, rvalue);
            }

            context.write(new Text(""), new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration cfg= new Configuration();
        String[] otherArgs = new GenericOptionsParser(cfg, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Toptenbuy <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(cfg);
        //设置主方法所在的类
        job.setJarByClass(Toptenbuy.class);
        job.setMapperClass(TopMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TopReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(7);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}