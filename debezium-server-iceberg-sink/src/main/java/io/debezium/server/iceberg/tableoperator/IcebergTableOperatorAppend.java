/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import io.debezium.engine.ChangeEvent;

import java.util.ArrayList;
import java.util.function.Predicate;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
@Named("IcebergTableOperatorAppend")
public class IcebergTableOperatorAppend extends AbstractIcebergTableOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIcebergTableOperator.class);

  @Override
  public void addToTable(Table icebergTable, ArrayList<ChangeEvent<Object, Object>> events) throws InterruptedException {

    ArrayList<Record> icebergRecords = toIcebergRecords(icebergTable.schema(), events);
    DataFile dataFile = getDataFile(icebergTable, icebergRecords);
    LOGGER.debug("Committing new file as Append '{}' !", dataFile.path());
    AppendFiles c = icebergTable.newAppend()
        .appendFile(dataFile);

    c.commit();
    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());
  }

  @Override
  public Predicate<Record> filterEvents() {
    return p -> true;
  }
}
