import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class FileReaderSpout implements IRichSpout {
  private final String filename;
  private SpoutOutputCollector _collector;
  private TopologyContext context;

  private FileReader fileReader;

  public FileReaderSpout(String fileName) {
    this.filename = fileName;
  }


  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
    try {
      fileReader = new FileReader(filename);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */

    final BufferedReader bReader = new BufferedReader(fileReader);

    String line;
    try {
      while((line = bReader.readLine()) != null) {
        line = line.trim();
        if (line.length() > 0) {
          _collector.emit(new Values(line));
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    Utils.sleep(1000);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */

  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
