import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>  {

 private Text outK= new Text();
 private FlowBean outV = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        String phoneno = split[1];
        String upflow = split[split.length - 3];
        String downflow = split[split.length - 2];

        outK.set(phoneno);
        outV.setUpFlow(Long.parseLong(upflow));
        outV.setDownFlow(Long.parseLong(downflow));
        outV.setSumFlow();

        context.write(outK,outV);
    }
}
