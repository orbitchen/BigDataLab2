import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner extends Reducer<Text,Text,Text,Text>
{
    //输入key：词语;作品 输入value：出现次数（1）
    //Combiner 输出key：词语 输出value：作品:出现次数
    private static Text word=new Text();
    private static Text index=new Text();
    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException
    {
        //key完全相同，所有的作品相同
        int num=0;
        for(Text v:value)
            num++;
        String book;
        //int s=key.toString().indexOf(";");
        word.set(key.toString().split(" ")[0]);
        book=key.toString().split(" ")[1];
        index.set(book+":"+num);
        //System.err.println("Combine:"+book+";"+ num);
        context.write(word,index);
    }
}
