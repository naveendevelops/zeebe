package io.zeebe.distributedlog.restore.impl;

import io.zeebe.distributedlog.restore.RestoreServer.SnapshotInfoRequestHandler;
import io.zeebe.logstreams.spi.SnapshotController;

public class DefaultSnapshotInfoRequestHandler implements SnapshotInfoRequestHandler {

  private final SnapshotController[] controllers;

  public DefaultSnapshotInfoRequestHandler(SnapshotController... controllers) {
    this.controllers = controllers;
  }

  @Override
  public Integer onSnapshotInfoRequest(Void request) {
    int numSnapshotsToReplicate = 0;
    for (SnapshotController controller : controllers) {
      if (controller.getValidSnapshotsCount() > 0) {
        numSnapshotsToReplicate++;
      }
    }
    return numSnapshotsToReplicate;
  }
}
