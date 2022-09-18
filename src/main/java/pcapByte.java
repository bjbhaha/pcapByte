import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class pcapByte {
    public static void main(String args[]) throws IOException {
        SparkConf conf = new SparkConf().setAppName("pcaptest").setMaster("local[12]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<IntWritable, BytesWritable> javaRDD =sc.sequenceFile("/home/bjbhaha/Desktop/music31.seq", IntWritable.class, BytesWritable.class);
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("/home/bjbhaha/Desktop/music31.pcap"));
        byte pcapHeader[] = new byte[]{(byte) 0xD4, (byte) 0xC3, (byte) 0xB2, (byte) 0xA1, 0x02, 0x00, 0x04,
                0x00,0x00, 0x00, 0x00, 0x00, 0x00, 0x00,0x00,0x00,0x00,0x00,0x04,0x00,0x01,0x00,0x00,0x00};
        dos.write(pcapHeader,0,24);//
        //javaRDD.foreach(x->System.out.println(x._2));
        List<byte[]> list=new ArrayList<>();
        list=javaRDD.map(x->{
            int l=x._2.getLength();
            byte[] a=new byte[l];
            System.arraycopy(x._2.getBytes(),0,a,0,l);
            return a;
        }).collect();
        //javaRDD.foreach(x->list.add(x._2));
//        javaRDD.foreach(tt-> {
////            byte src[]={0x7c, (byte) 0xc3, (byte) 0x85, (byte) 0x89, (byte) 0x87,0x1a};
////            byte dst[]={0x6c, (byte) 0xc3, (byte) 0x85, (byte) 0x89, (byte) 0x87,0x1a};
////            byte type[]={0x08,0x00};
////            dos.write(src,0,6);
////            dos.write(src,0,6);
////            dos.write(type,0,2);
////            list.add(tt._2.getBytes());}
////                    try {
////                        //if(tt._2.getLength()!=16)
////                            dos.write(tt._2.getBytes(), 0, tt._2.getLength());
////                    } catch (IOException e) {
////                        e.printStackTrace();
////                    }
//            System.out.println(tt._2);
//                }
//        );
        list.forEach(tt->{
//            for(int i=0;i<tt.length;i++){
//                System.out.print(tt[i]+" ");
//            }
//            System.out.println();
            //BytesWritable b=new BytesWritable(tt);
           // b.setSize(tt.length);
            System.out.println(tt);
            try {
                //if(tt._2.getLength()!=16)
                dos.write(tt, 0, tt.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
//        for(Iterator iterator=list.listIterator();iterator.hasNext();){
//            byte a[]=byte[](iterator.next();
//            dos.write(iterator.next(),0,);
//        }
//        dos.write(tt._2.getBytes(),0,tt._2.getLength());

    }
}
