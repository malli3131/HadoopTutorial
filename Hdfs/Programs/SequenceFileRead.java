package twok.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
public class SequenceFileRead {

        /**
         * @author malli
         * @param args
         */
	
        public static void main(String[] args) throws IOException, URISyntaxException {
                String uri = "/sequenceFiles/stocks";
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(new URI("hdfs://prasad:9000"), conf);
                Path path = new Path(uri);
                SequenceFile.Reader reader = null;
                try
                {
                        reader = new SequenceFile.Reader(fs, path, conf);
                        System.out.println(reader.getKeyClassName());
                        System.out.println(reader.getValueClassName());
                }
                finally {
                	  IOUtils.closeStream(reader);
                }
        }
}