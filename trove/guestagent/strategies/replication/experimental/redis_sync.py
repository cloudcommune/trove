# Copyright 2014 Tesora, Inc.
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
from oslo_utils import netutils

from trove.guestagent.common import operating_system
from trove.guestagent.strategies.replication import base

LOG = logging.getLogger(__name__)


class RedisSyncReplication(base.Replication):
    """Redis Replication strategy."""

    __strategy_ns__ = 'trove.guestagent.strategies.replication.experimental'
    __strategy_name__ = 'RedisSyncReplication'

    CONF_LABEL_REPLICATION_MASTER = 'replication_master'
    CONF_LABEL_REPLICATION_SLAVE = 'replication_slave'

    def get_master_ref(self, service, snapshot_info):
        master_ref = {
            'host': netutils.get_my_ipv4(),
            'port': service.get_port(),
            'requirepass': service.get_auth_password(),
            'ha': service.get_ha()
        }
        return master_ref

    def backup_required_for_replication(self):
        LOG.debug('Request for replication backup: no backup required')
        return False

    def snapshot_for_replication(self, context, service,
                                 location, snapshot_info):
        return None, None

    @operating_system.synchronized('enable_as_master')
    def enable_as_master(self, service, master_config):
        service.configuration_manager.apply_system_override(
            master_config, change_id=self.CONF_LABEL_REPLICATION_MASTER)
        ha = service.get_ha()
        if ha.get('enabled'):
            if ha['number_of_nodes'] == 1 and not ha.get('sentinel_enabled'):
                ha['sentinel_enabled'] = True
            ha['number_of_nodes'] += 1
            _quorum = 1 if ha['number_of_nodes'] <= 2 else \
                ha['number_of_nodes'] / 2 + 1
            ha['quorum'] = _quorum
            service.set_ha(ha)
        service.restart()

    def enable_as_slave(self, service, snapshot, slave_config):
        service.configuration_manager.apply_system_override(
            slave_config, change_id=self.CONF_LABEL_REPLICATION_SLAVE)
        master_info = snapshot['master']
        master_host = master_info['host']
        master_port = master_info['port']
        connect_options = {'slaveof': [master_host, master_port]}
        master_passwd = master_info.get('requirepass')
        if master_passwd:
            connect_options['requirepass'] = master_passwd
            connect_options['masterauth'] = master_passwd
            service.enable_root(master_passwd)
        else:
            service.admin.config_set('masterauth', "")
        service.configuration_manager.apply_system_override(
            connect_options, change_id=self.CONF_LABEL_REPLICATION_SLAVE)
        service.admin.set_master(host=master_host, port=master_port)
        service.set_ha(master_info.get('ha', {'enabled': False}))
        LOG.debug('Enabled as slave.')

    def detach_slave(self, service, for_failover):
        service.configuration_manager.remove_system_override(
            change_id=self.CONF_LABEL_REPLICATION_SLAVE)
        service.admin.set_master(host=None, port=None)
        service.admin.config_set('masterauth', "")
        service.delete_ha()
        service.stop_db()
        return None

    def cleanup_source_on_replica_detach(self, service, replica_info):
        # Nothing needs to be done to the master when a replica goes away.
        ha = service.get_ha()
        if ha.get('enabled'):
            ha['number_of_nodes'] -= 1
            if ha['number_of_nodes'] == 1 and ha.get('sentinel_enabled'):
                ha['sentinel_enabled'] = False
            _quorum = 1 if ha['number_of_nodes'] <= 2 else \
                ha['number_of_nodes'] / 2 + 1
            ha['quorum'] = _quorum
            service.set_ha(ha)

    def get_replica_context(self, service):
        return {
            'master': self.get_master_ref(service, None),
        }

    def demote_master(self, service):
        service.configuration_manager.remove_system_override(
            change_id=self.CONF_LABEL_REPLICATION_MASTER)

    def get_sync_info(self, service, info_type):
        if info_type == 'detach':
            service.sentinel_command("sentinel reset '*'")
        return service.get_ha()

    def sync_from_master_info(self, service, info_type, info):
        if info_type == 'detach':
            service.sentinel_command("sentinel reset '*'")
        return service.set_ha(info)
