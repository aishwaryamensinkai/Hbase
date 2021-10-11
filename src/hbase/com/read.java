package hbase.com;
import java.io.BufferedReader;  
import java.io.File;  
import java.io.FileReader;  
import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.util.Bytes;  
  
  
public class read {  
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException{  
            Configuration conf = HBaseConfiguration.create(new Configuration());  
            conf.set("hbase.master","localhost:9000");
            conf.set("hbase.zookeeper.property.clientPort", "2222");
            conf.set("hbase.zookeeper.quorum", "localhost");
            System.out.println("Config set");
            HBaseAdmin hba = new HBaseAdmin(conf);  
            if(!hba.tableExists("users"))
            {  
                HTableDescriptor ht = new HTableDescriptor(args[0]);  
                ht.addFamily(new HColumnDescriptor("id"));  
                ht.addFamily(new HColumnDescriptor("email_id"));  
                ht.addFamily(new HColumnDescriptor("name"));  
                ht.addFamily(new HColumnDescriptor("password"));  
                ht.addFamily(new HColumnDescriptor("user_name"));   
                hba.createTable(ht);  
                System.out.println("New Table Created");  
                  
                HTable table = new HTable(conf,"users");  
              
                File f = new File("/home/pelatro/eclipse-workspace/Server/src/user.csv");  
                BufferedReader br = new BufferedReader(new FileReader(f));  
                String line = br.readLine();  
                int i =1;  
                String rowname="row";  
                while(line!=null && line.length()!=0)
                {  
                    StringTokenizer tokens = new StringTokenizer(line,",");  
                    rowname = "row"+i;  
                    Put p = new Put(Bytes.toBytes(rowname));  
                    p.add(Bytes.toBytes("id"),Bytes.toBytes("UserId"),Bytes.toBytes(tokens.nextToken()));  
                    p.add(Bytes.toBytes("email_id"),Bytes.toBytes("EmailId"),Bytes.toBytes(tokens.nextToken()));  
                    p.add(Bytes.toBytes("name"),Bytes.toBytes("Name"),Bytes.toBytes(tokens.nextToken()));  
                    p.add(Bytes.toBytes("password"),Bytes.toBytes("Password"),Bytes.toBytes(tokens.nextToken()));  
                    p.add(Bytes.toBytes("user_name"),Bytes.toBytes("UserName"),Bytes.toBytes(tokens.nextToken()));  
                    i++;  
                    table.put(p);  
                    line = br.readLine();  
                    System.out.println(rowname);
                }  
                    br.close();  
                    table.close();  
            }  
            else  
            {
                System.out.println("Table Already exists.Please enter another table name");  
            }
    }  
}  