/**
 * Put your copyright and license info here.
 */
package com.datatorrent.snaptest;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.google.common.collect.Maps;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");

    AppDataSnapshotServerMap snapshotServer = dag.addOperator("SnapshotServer", new AppDataSnapshotServerMap());
    RandomNumberGenerator rand = dag.addOperator("RandomNumberGenerator", new RandomNumberGenerator());

    String snapshotServerJSON = SchemaUtils.jarResourceFileToString("schema.json");
    snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);

    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.setTopic("SnapshotExampleQuery");
    wsQuery.setUri(uri);
    snapshotServer.setEmbeddableQueryInfoProvider(wsQuery);

    PubSubWebSocketAppDataResult wsResult = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsResult.setUri(uri);
    wsResult.setTopic("SnapshotExampleResult");

    dag.addStream("Random to Snapshot", rand.out, snapshotServer.input);
    dag.addStream("Result", snapshotServer.queryResult, wsResult.input);
  }
}
