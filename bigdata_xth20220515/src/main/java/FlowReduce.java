import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean outV=new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //1.遍历集合累加值
        long totalup = 0;
        long totaldown = 0;
        for (FlowBean value : values) {
            totalup += value.getUpFlow();
            totaldown += value.getDownFlow();
        }
        //2.封装
        outV.setUpFlow(totalup);
        outV.setDownFlow(totaldown);
        outV.setSumFlow();

        //3.写出
        context.write(key,outV);


    }
}
