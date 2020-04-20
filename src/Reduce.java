import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text,Text,Text,Text>
{
    //输入key：词语 输入value：作品:出现次数
    //reducer 输出key：词语 输出value：平均出现次数,作品:出现次数; ...
    private static Text index=new Text();
    private static StringBuilder out=new StringBuilder(256);

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException
    {
        int bookn=0;//这个词语出现的作品数量
        int appearn=0;//这个词语出现的总次数
        //String out="";
        for(Text v:value)
        {
            bookn++;
            appearn+=Integer.parseInt(v.toString().split(":")[1]);
            //System.err.println("Reduce:"+v.toString());
            //Thread.sleep(100);
            out.append(v.toString()).append(";");
        }
        double avg=(double)appearn/(double)bookn;
        //String out= new String(avg + ",");//StringBuilder:IDE的建议
//        for(Text v:value)
//        {
//            System.err.println("Reduce:"+v.toString());
//            out+=v.toString()+";";
//        }
        index.set(avg+","+out.toString());
        context.write(key,index);
        out.delete(0,out.length());
    }
}
