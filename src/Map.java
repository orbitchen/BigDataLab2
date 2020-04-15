import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<Object,Text,Text, Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map 输出key：词语;作品 输出value：1
    //Combiner 输出key：词语 输出value：作品;出现次数
    //reducer 输出key：词语 输出value：作品;出现次数 ...

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        FileSplit fs=(FileSplit) context.getInputSplit();
        String fileName=fs.getPath().getName();//文件名（作品名）
        StringTokenizer str=new StringTokenizer(value.toString());
        for(;str.hasMoreTokens();)
        {
            //词语;作品 出现次数
            context.write(new Text(str.nextToken()+";"+fileName),new Text("1"));
        }
    }
}