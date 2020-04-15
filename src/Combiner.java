import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner extends Reducer<Text,Text,Text,Text>
{
    //输入key：词语;作品 输入value：出现次数（1）
    //Combiner 输出key：词语 输出value：作品:出现次数
    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException
    {
        //key完全相同，所有的作品相同
        int num=0;
        for(Text v:value)
            num+=Integer.parseInt(v.toString());
        String word,book;
        int s=key.toString().indexOf(";");
        word=key.toString().substring(0,s);
        book=key.toString().substring(s+1);
        //System.err.println("Combine:"+book+";"+ num);
        context.write(new Text(word),new Text(book+":"+ num));
    }
}
