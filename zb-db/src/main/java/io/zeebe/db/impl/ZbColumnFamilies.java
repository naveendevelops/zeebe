/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.db.impl;

public enum ZbColumnFamilies {
  DEFAULT,

  // util
  KEY,

  // workflow
  WORKFLOW_VERSION,

  // workflow cache
  WORKFLOW_CACHE,
  WORKFLOW_CACHE_BY_ID_AND_VERSION,
  WORKFLOW_CACHE_LATEST_KEY,

  // element instance
  ELEMENT_INSTANCE_PARENT_CHILD,
  ELEMENT_INSTANCE_KEY,
  STORED_INSTANCE_EVENTS,
  STORED_INSTANCE_EVENTS_PARENT_CHILD,

  // variable state
  ELEMENT_INSTANCE_CHILD_PARENT,
  VARIABLES,
  TEMPORARY_VARIABLE_STORE,

  // timer state
  TIMERS,
  TIMER_DUE_DATES,

  // pending deployments
  PENDING_DEPLOYMENT,

  // jobs
  JOBS,
  JOB_STATES,
  JOB_DEADLINES,
  JOB_ACTIVATABLE,

  // message
  MESSAGE_KEY,
  MESSAGES,
  MESSAGE_DEADLINES,
  MESSAGE_IDS,
  MESSAGE_CORRELATED,

  // message subscription
  MESSAGE_SUBSCRIPTION_BY_KEY,
  MESSAGE_SUBSCRIPTION_BY_SENT_TIME,
  MESSAGE_SUBSCRIPTION_BY_NAME_AND_CORRELATION_KEY,

  // message start event subscription
  MESSAGE_START_EVENT_SUBSCRIPTION_BY_NAME_AND_KEY,
  MESSAGE_START_EVENT_SUBSCRIPTION_BY_KEY_AND_NAME,

  // workflow instance subscription
  WORKFLOW_SUBSCRIPTION_BY_KEY,
  WORKFLOW_SUBSCRIPTION_BY_SENT_TIME,

  // incident
  INCIDENTS,
  INCIDENT_WORKFLOW_INSTANCES,
  INCIDENT_JOBS,

  // event
  EVENT_SCOPE,
  EVENT_TRIGGER,

  BLACKLIST,

  EXPORTER,

  // log block index
  BLOCK_POSITION_ADDRESS
}
