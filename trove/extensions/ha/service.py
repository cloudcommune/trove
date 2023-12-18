# Copyright 2021 Yovole, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

from oslo_log import log as logging
from trove.cluster.models import DBCluster
from trove.common import cfg
from trove.common import exception
from trove.common import wsgi
from trove.datastore import models as datastore_models
from trove.extensions.common.service import ExtensionController
from trove.extensions.ha.models import HighAvailabilityAgent
from trove.instance.models import DBInstance, Instance


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class HighAvailabilityController(ExtensionController):
    def index(self, req, tenant_id, instance_id):
        """Return ha status"""
        return self.get_ha(req, tenant_id, instance_id)

    def create(self, req, tenant_id, instance_id, body=None):
        """Enable the root user for the db instance."""
        datastore_manager, is_cluster = self._get_datastore(tenant_id,
                                                            instance_id)
        if datastore_manager != 'redis':
            raise exception.DatastoreOperationNotSupported(
                operation="high availability create",
                datastore=datastore_manager)
        return self.create_ha(req, body, tenant_id, instance_id, is_cluster)

    def delete(self, req, tenant_id, instance_id):
        LOG.info("Delete instance ha\n"
                 "req : '%(req)s'\n\n",
                 {"id": instance_id, "req": req})
        datastore_manager, is_cluster = self._get_datastore(tenant_id,
                                                            instance_id)
        context = req.environ[wsgi.CONTEXT_KEY]
        self._validate_can_perform_action(tenant_id, instance_id, is_cluster,
                                          "delete_ha")
        slave_instances = self._get_slaves(tenant_id, instance_id)
        for id in [instance_id] + slave_instances:
            instance = Instance.load(context, id)
            if instance.status != 'ACTIVE':
                raise exception.InvalidInstanceState(status=instance.status)
        HighAvailabilityAgent.delete_ha(context, instance_id)
        for slave_id in slave_instances:
            HighAvailabilityAgent.delete_ha(context, slave_id)
        return wsgi.Result(None, 202)

    def _get_datastore(self, tenant_id, instance_or_cluster_id):
        """
        Returns datastore manager and a boolean
        showing if instance_or_cluster_id is a cluster id
        """
        args = {'id': instance_or_cluster_id, 'tenant_id': tenant_id}
        is_cluster = False
        try:
            db_info = DBInstance.find_by(**args)
        except exception.ModelNotFoundError:
            is_cluster = True
            db_info = DBCluster.find_by(**args)

        ds_version = (datastore_models.DatastoreVersion.
                      load_by_uuid(db_info.datastore_version_id))
        ds_manager = ds_version.manager
        return (ds_manager, is_cluster)

    def _get_config(self, body):
        return body

    def _set_ha(self, req, instance_id, ha_config, slave_instances=None):
        LOG.info("Set HA config for instance'%s': %s.", instance_id, ha_config)
        LOG.info("req : '%s'\n\n", req)
        context = req.environ[wsgi.CONTEXT_KEY]
        ha_config.setdefault('sentinel_enabled', bool(slave_instances))
        _quorum = 1 if len(slave_instances) <= 1 else\
            (len(slave_instances) + 1) / 2 + 1
        ha_config.setdefault('quorum', _quorum)
        ha_config['number_of_nodes'] = len(slave_instances) + 1
        HighAvailabilityAgent.set_ha(context, instance_id, ha_config)
        for slave_id in slave_instances:
            HighAvailabilityAgent.set_ha(context, slave_id, ha_config)
        return wsgi.Result(None, 200)

    def get_ha(self, req, tenant_id, instance_id, is_cluster=False):
        context = req.environ[wsgi.CONTEXT_KEY]
        return HighAvailabilityAgent.get_ha(context, instance_id)

    def create_ha(self, req, body, tenant_id, instance_id, is_cluster):
        """Rename commands for redis
        """
        self._validate_can_perform_action(tenant_id, instance_id, is_cluster,
                                          "create_ha")
        config = self._get_config(body)
        context = req.environ[wsgi.CONTEXT_KEY]
        slave_instances = self._get_slaves(tenant_id, instance_id)
        for id in [instance_id] + slave_instances:
            instance = Instance.load(context, id)
            if instance.status != 'ACTIVE':
                raise exception.InvalidInstanceState(status=instance.status)
        return self._set_ha(req, instance_id, config, slave_instances)

    @staticmethod
    def _is_slave(tenant_id, instance_id):
        args = {'id': instance_id, 'tenant_id': tenant_id}
        instance_info = DBInstance.find_by(**args)
        return instance_info.slave_of_id

    @staticmethod
    def _get_slaves(tenant_id, instance_or_cluster_id, deleted=False):
        LOG.info("Getting non-deleted slaves of instance '%s', "
                 "if any.", instance_or_cluster_id)
        args = {'slave_of_id': instance_or_cluster_id, 'tenant_id': tenant_id,
                'deleted': deleted}
        db_infos = DBInstance.find_all(**args)
        slaves = []
        for db_info in db_infos:
            slaves.append(db_info.id)
        return slaves

    @staticmethod
    def check_instace_status(context, id_list, status):
        LOG.info("Is instance status ACTIVE")
        for id in id_list:
            instance = Instance.load(context, id)
            if instance.status != status:
                return False
        return True

    def _validate_can_perform_action(self, tenant_id, instance_id, is_cluster,
                                     operation):
        if is_cluster:
            raise exception.ClusterOperationNotSupported(
                operation=operation)
        is_slave = self._is_slave(tenant_id, instance_id)
        if is_slave:
            raise exception.SlaveOperationNotSupported(operation=operation)
