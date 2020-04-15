import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text,Text,Text,Text>
{
    //输入key：词语 输入value：作品;出现次数
    //reducer 输出key：词语 输出value：平均出现次数,作品:出现次数; ...
    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        int bookn=0;//这个词语出现的作品数量
        int appearn=0;//这个词语出现的总次数
        for(Text v:value)
        {
            bookn++;
            appearn+=Integer.parseInt(v.toString().split(";")[1]);
        }
        int avg=appearn/bookn;
        StringBuilder out= new StringBuilder(String.valueOf(avg) + ",");//StringBuilder:IDE的建议
        for(Text v:value)
        {
            out.append(v.toString()).append(";");
        }
        context.write(key,new Text(out.toString()));
    }
}
