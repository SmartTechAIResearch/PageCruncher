package be.sizingservers.pagecruncher;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.zip.GZIPInputStream;

/**
 * Created by wannes on 7/7/15.
 */
public class ClassifierStore {

    public static String path = SparkApp.pathPrefix() + "english.muc.7class.distsim.crf.ser.gz";
    public static Configuration hadoopConf;

    public static final transient ThreadLocal<AbstractSequenceClassifier<CoreLabel>> classifier_TL = new ThreadLocal<AbstractSequenceClassifier<CoreLabel>>() {
        @Override
        public AbstractSequenceClassifier<CoreLabel> initialValue() {
            try {
                Logger.getLogger(ClassifierStore.class).info("Loading NLP classifiers from " + path);
                    FileSystem fileSystem = FileSystem.get(new Configuration());
                    return CRFClassifier.getClassifier(new GZIPInputStream(fileSystem.open(new Path(path))));
                /*} else {
                    path = path.replace("file://", "");
                    return CRFClassifier.getClassifier(path);
                }*/
            } catch (Exception ex) {
                ex.printStackTrace(System.err);
            }

            return null;
        }
    };

    public static AbstractSequenceClassifier<CoreLabel> get_TL() {
        AbstractSequenceClassifier<CoreLabel> asc = classifier_TL.get();
        assert asc != null;
        return asc;
    }

    private static AbstractSequenceClassifier<CoreLabel> instance = null;
    public static final  AbstractSequenceClassifier<CoreLabel> get() {
        if (instance == null) {
            try {
                if (hadoopConf != null) {
                    FileSystem fileSystem = FileSystem.get(hadoopConf);
                    Logger.getLogger(ClassifierStore.class).info("Loading classifiers from " + path);
                    instance = CRFClassifier.getClassifier(new GZIPInputStream(fileSystem.open(new Path(path))));
                } else {
                    instance = CRFClassifier.getClassifier(path);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return instance;
    }
}
