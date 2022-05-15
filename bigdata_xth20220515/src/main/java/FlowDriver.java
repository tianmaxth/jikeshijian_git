import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);

        job.setReducerClass(FlowReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        String inptPath = args != null && args.length >= 2 ? args[0] : "D:\\jksj_data";
        String onptPath = args != null && args.length >= 2 ? args[1] : "D:\\\\jksj_data\\output";

        //FileInputFormat.setInputPaths(job, new Path("D:\\BigData\\input"));
        //FileOutputFormat.setOutputPath(job, new Path("D:\\BigData\\output1"));
        FileInputFormat.setInputPaths(job, new Path[]{new Path(inptPath)});
        FileOutputFormat.setOutputPath(job, new Path(onptPath));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);




    }
}
