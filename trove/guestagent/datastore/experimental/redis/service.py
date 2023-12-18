# Copyright (c) 2013 Rackspace
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

import hashlib
import math
import netifaces
import os
import redis
from redis.exceptions import BusyLoadingError, ConnectionError

from oslo_log import log as logging

from trove.common import cfg
from trove.common.db.redis.models import RedisRootUser
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common.stream_codecs import PropertiesCodec, StringConverter
from trove.common import utils
from trove.guestagent.common.configuration import ConfigurationManager
from trove.guestagent.common.configuration import OneFileOverrideStrategy
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.redis import system
from trove.guestagent.datastore import service
from trove.guestagent import pkg
from trove.taskmanager.models import NotifyMixin

LOG = logging.getLogger(__name__)
TIME_OUT = 1200
CONF = cfg.CONF
CLUSTER_CFG = 'clustering'
SYS_OVERRIDES_AUTH = 'auth_password'
Redis_Sentinel_Quonum = 1
packager = pkg.Package()
ALLOW_DISABLED_COMMANDS = ["FLUSHALL", "FLUSHDB", "KEYS", "HGETALL",
                           "EVAL", "EVALSHA", "SCRIPT", "MONITOR", "SHUTDOWN"]


class RedisAppStatus(service.BaseDbStatus):
    """
    Handles all of the status updating for the redis guest agent.
    """

    def __init__(self, client):
        super(RedisAppStatus, self).__init__()
        self.__client = client

    def set_client(self, client):
        self.__client = client

    def _get_actual_db_status(self):
        try:
            if self.__client.ping():
                return rd_instance.ServiceStatuses.RUNNING
        except ConnectionError:
            return rd_instance.ServiceStatuses.SHUTDOWN
        except BusyLoadingError:
            return rd_instance.ServiceStatuses.BLOCKED
        except Exception:
            LOG.exception("Error getting Redis status.")

        return rd_instance.ServiceStatuses.CRASHED

    def cleanup_stalled_db_services(self):
        utils.execute_with_timeout('pkill', '-9',
                                   'redis-server',
                                   run_as_root=True,
                                   root_helper='sudo')

    def _get_actual_db_custom_status(self):
        """Find and report role of DB on this machine.
        The database is updated and the status is also returned.
        """
        if self.is_installed and not self._is_restarting:
            LOG.debug("Determining role of DB server.")
            custom = self._check_redis_role()
            LOG.debug("role is %s and is_change is %s." % (
                custom['role'],
                custom['is_change'])
            )
        sentinel_enabled = operating_system.custom_config_ini_get(
            key='sentinel_enabled'
        )

        status = {"is_notify": False}
        if sentinel_enabled == "True":
            # update_quorum = self._check_redis_quorum()
            # if update_quorum['is_change']:
            #     kwargs = {'quorum': update_quorum["quorum"]}
            #     RedisApp().upload_redis_sentinel_config(**kwargs)
            if custom['is_change']:
                operating_system.custom_config_ini_set(
                    dict_args={'role': custom['role']}
                )
                self.is_master(custom['role'])
                if "master" in custom['role']:
                    status['is_notify'] = True
                    status.update({"role": "master"})
                else:
                    status.update({"role": "slave"})
        else:
            LOG.info("local node redis haproxy disable")
        LOG.debug("custom status is: %s" % status)
        return status

    def _check_redis_role(self):
        value = {
            'is_change': False,
            'status': 'active'
        }
        try:
            if self.__client.ping():
                info = self.__client.info(section='Replication')
                value['role'] = info['role']
                value['is_change'] = self._change_check(
                    'role', value['role']
                )
                return value
        except Exception:
            LOG.exception("Error getting Redis role.")

    def _check_redis_quorum(self):
        quorum = 1
        is_change = True
        try:
            context = operating_system.read_file(
                '/etc/redis-sentinel.conf',
                as_root=True
            )
            slave_num = context.count('known-sentinel')
            if int(slave_num) >= 2:
                quorum = int(math.floor((slave_num + 1) / 2) + 1)
            is_change = self._change_check('quorum', quorum)
        except Exception:
            LOG.exception("Error getting Redis quorum.")
        finally:
            return {'quorum': quorum, 'is_change': is_change}

    def _change_check(self, custom_key, value):
        is_change = True
        old = operating_system.custom_config_ini_get(key=custom_key)
        if str(value) in str(old):
            is_change = False
        if is_change:
            LOG.info("%s is_change true from %s to %s." % (
                custom_key, old, value)
            )
        return is_change

    def is_master(self, role):
        vip, device = self._get_ha_info()
        if "master" in role:
            LOG.info("update role to master")
            if self._check_vip(vip, device):
                LOG.info("Add vip %s for %s" % (vip, device))
                operating_system.set_ha_master(vip, device)
        elif "slave" in role:
            LOG.info("update role to slave")
            if not self._check_vip(vip, device):
                LOG.info("Remove vip %s for %s" % (vip, device))
                operating_system.set_ha_slave(vip, device)

    def _get_ha_info(self):
        devs = netifaces.gateways()[netifaces.AF_INET]
        dev = list(devs[0])[1]
        vip = operating_system.custom_config_ini_get(key='vip')
        LOG.debug("Get vip address: %s device: %s" % (vip, dev))
        return vip, dev

    def _check_vip(self, vip, device):
        devs = netifaces.ifaddresses(device)[netifaces.AF_INET]
        for dev in devs:
            if vip in dev.values():
                LOG.info("vip address: %s already exists" % vip)
                return False
        return True

    def set_custom_status(self, status, force=False):
        """Use guest to update the DB status."""

        if force or self.is_installed:
            LOG.debug("Casting redis role status message to notify "
                      "( role is '%s' )." % status)
            if status == 'master':
                self.promote_to_master_in_db()
            info = NotifyMixin()
            kwargs = {
                "role": status
            }
            info.send_update_event('update', CONF.guest_id, **kwargs)
            LOG.debug("Successfully cast set_status.")
        else:
            LOG.debug("Prepare has not completed yet, skipping redis role "
                      "check.")

    def manual_update_custom_status(self):
        status = self._get_actual_db_custom_status()
        LOG.debug("manual update custom status: %s" % status)
        if status['is_notify']:
            self.set_custom_status(status['role'])


class RedisApp(object):
    """
    Handles installation and configuration of redis
    on a trove instance.
    """

    def __init__(self, state_change_wait_time=None):
        """
        Sets default status and state_change_wait_time
        """
        if state_change_wait_time:
            self.state_change_wait_time = state_change_wait_time
        else:
            self.state_change_wait_time = CONF.state_change_wait_time

        revision_dir = guestagent_utils.build_file_path(
            os.path.dirname(system.REDIS_CONFIG),
            ConfigurationManager.DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)
        config_value_mappings = {'yes': True, 'no': False, "''": None}
        self._value_converter = StringConverter(config_value_mappings)
        command_value_mappings = {'yes': True, 'no': False, "": None}
        self._command_value_converter = StringConverter(command_value_mappings)
        self.configuration_manager = ConfigurationManager(
            system.REDIS_CONFIG,
            system.REDIS_OWNER, system.REDIS_OWNER,
            PropertiesCodec(
                unpack_singletons=False,
                string_mappings=config_value_mappings
            ), requires_root=True,
            override_strategy=OneFileOverrideStrategy(revision_dir))
        self.admin = self._build_admin_client()
        self.status = RedisAppStatus(self.admin)
        self._sentinel_configration_manager = None

    def _build_admin_client(self):
        password = self.get_configuration_property('requirepass')
        socket = self.get_configuration_property('unixsocket')
        cmd = self.get_config_command_name()

        return RedisAdmin(password=password, unix_socket_path=socket,
                          config_cmd=cmd)

    def _refresh_admin_client(self):
        self.admin = self._build_admin_client()
        self.status.set_client(self.admin)
        return self.admin

    def install_if_needed(self, packages):
        """
        Install redis if needed do nothing if it is already installed.
        """
        LOG.info('Preparing Guest as Redis Server.')
        if not packager.pkg_is_installed(packages):
            LOG.info('Installing Redis.')
            self._install_redis(packages)
        LOG.info('Redis installed completely.')

    def _install_redis(self, packages):
        """
        Install the redis server.
        """
        LOG.debug('Installing redis server.')
        LOG.debug("Creating %s.", system.REDIS_CONF_DIR)
        operating_system.create_directory(system.REDIS_CONF_DIR, as_root=True)
        pkg_opts = {}
        packager.pkg_install(packages, pkg_opts, TIME_OUT)
        self.start_db()
        LOG.debug('Finished installing redis server.')

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        self.status.stop_db_service(
            system.SERVICE_CANDIDATES, self.state_change_wait_time,
            disable_on_boot=do_not_start_on_reboot, update_db=update_db)

    def restart(self):
        self.status.restart_db_service(
            system.SERVICE_CANDIDATES, self.state_change_wait_time)

    def update_overrides(self, context, overrides, remove=False):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)
            # apply requirepass at runtime
            # TODO(zhaochao): updating 'requirepass' here will be removed
            # in the future releases, Redis only use enable_root/disable_root
            # to set this parameter.
            if 'requirepass' in overrides:
                self.admin.config_set('requirepass', overrides['requirepass'])
                self._refresh_admin_client()

    def apply_overrides(self, client, overrides):
        """Use the 'CONFIG SET' command to apply configuration at runtime.

        Commands that appear multiple times have values separated by a
        white space. For instance, the following two 'save' directives from the
        configuration file...

            save 900 1
            save 300 10

        ... would be applied in a single command as:

            CONFIG SET save "900 1 300 10"

        Note that the 'CONFIG' command has been renamed to prevent
        users from using it to bypass configuration groups.
        """
        for prop_name, prop_args in overrides.items():
            args_string = self._join_lists(
                self._command_value_converter.to_strings(prop_args), ' ')
            client.config_set(prop_name, args_string)
            # NOTE(zhaochao): requirepass applied in update_overrides is
            # only kept for back compatibility. Now requirepass is set
            # via enable_root/disable_root, Redis admin client should be
            # refreshed here.
            if prop_name == "requirepass":
                client = self._refresh_admin_client()

    def _join_lists(self, items, sep):
        """Join list items (including items from sub-lists) into a string.
        Non-list inputs are returned unchanged.

        _join_lists('1234', ' ') = "1234"
        _join_lists(['1','2','3','4'], ' ') = "1 2 3 4"
        _join_lists([['1','2'], ['3','4']], ' ') = "1 2 3 4"
        """
        if isinstance(items, list):
            return sep.join([sep.join(e) if isinstance(e, list) else e
                             for e in items])
        return items

    def remove_overrides(self):
        self.configuration_manager.remove_user_override()

    def make_read_only(self, read_only):
        # Redis has no mechanism to make an instance read-only at present
        pass

    def start_db_with_conf_changes(self, config_contents):
        LOG.info('Starting redis with conf changes.')
        if self.status.is_running:
            format = 'Cannot start_db_with_conf_changes because status is %s.'
            LOG.debug(format, self.status)
            raise RuntimeError(format % self.status)
        LOG.info("Initiating config.")
        self.configuration_manager.save_configuration(config_contents)
        # The configuration template has to be updated with
        # guestagent-controlled settings.
        self.apply_initial_guestagent_configuration()
        self.start_db(True)

    def start_db(self, update_db=False):
        self.status.start_db_service(
            system.SERVICE_CANDIDATES, self.state_change_wait_time,
            enable_on_boot=True, update_db=update_db)

    def apply_initial_guestagent_configuration(self, overrides):
        """Update guestagent-controlled configuration properties.
        """

        # Hide the 'CONFIG' command from end users by mangling its name.
        self.admin.set_config_command_name(
            self._mangle_config_command_name(overrides)
        )

        self.configuration_manager.apply_system_override(
            {'daemonize': 'yes',
             'protected-mode': 'no',
             'supervised': 'systemd',
             'pidfile': system.REDIS_PID_FILE,
             'logfile': system.REDIS_LOG_FILE,
             'dir': system.REDIS_DATA_DIR})

    def get_config_command_name(self):
        """Get current name of the 'CONFIG' command.
        """
        renamed_cmds = self.configuration_manager.get_value('rename-command')
        if renamed_cmds:
            for name_pair in renamed_cmds:
                if name_pair[0] == 'CONFIG':
                    return name_pair[1]

        return None

    def _mangle_config_command_name(self, overrides):
        """Hide the 'CONFIG' command from the clients by renaming it to a
        random string known only to the guestagent.
        Return the mangled name.
        """
        if 'vip' in overrides:
            mangled = self._update_config_by_vip(overrides['vip'])
        else:
            mangled = utils.generate_random_password()
        mangled = "redisconfig"
        self._rename_command('CONFIG', mangled)
        return mangled

    def _update_config_by_vip(self, vip):
        md5hash = hashlib.md5(vip)
        mangled = md5hash.hexdigest()
        operating_system.custom_config_ini_set(
            dict_args={
                'vip': vip,
                'mangled': mangled,
                'quorum': 1,
                'port': 6379,
                'passwd': "",
                'enable_auth': "false"
            }
        )
        return mangled

    def upload_redis_sentinel_config(self, **kwargs):
        LOG.debug("upload_redis_sentinel_config: %s", kwargs)
        if 'sentinel_enabled' in kwargs.keys():
            sentinel_enabled = kwargs['sentinel_enabled']
            operating_system.custom_config_ini_set(
                {'sentinel_enabled': sentinel_enabled}
            )
        else:
            sentinel_enabled = operating_system.custom_config_ini_get(
                key='sentinel_enabled'
            )
        service_candidates = ['redis-sentinel']
        if 'type' in operating_system.service_discovery(service_candidates):
            if str(sentinel_enabled) == 'True':
                operating_system.stop_service(service_candidates)
                config_content = \
                    self.sentinel_configration_manager.parse_configuration()
                LOG.debug(config_content)
                myid = guestagent_utils.get_value_from_properties_dict(
                    config_content, 'sentinel', 'myid')
                if not myid:
                    operating_system.copy(
                        self._get_config_template('redis-sentinel.conf.j2'),
                        system.SENTINEL_CONFIG, force=True, as_root=True)
                    config_content = \
                        self.sentinel_configration_manager\
                            .parse_configuration()
                    LOG.debug(config_content)
                monitor = guestagent_utils.get_value_from_properties_dict(
                    config_content, 'sentinel', 'monitor')

                master_name = monitor[0] if monitor else 'mymaster'
                monitor = monitor if monitor else \
                    [master_name, '127.0.0.1', '6379', 1]
                if 'master' in kwargs and kwargs.get('master'):
                    monitor[1] = kwargs['master']
                if 'port' in kwargs and kwargs.get('port'):
                    monitor[2] = kwargs['port']
                if 'quorum' in kwargs and kwargs.get('quorum'):
                    monitor[3] = kwargs['quorum']
                guestagent_utils.set_value_from_properties_dict(
                    config_content, 'sentinel', 'monitor', monitor)
                if 'mangled' in kwargs:
                    guestagent_utils.set_value_from_properties_dict(
                        config_content, 'sentinel', 'rename-command',
                        [master_name, 'CONFIG', kwargs['mangled']])
                if 'passwd' in kwargs and kwargs.get('passwd'):
                    guestagent_utils.set_value_from_properties_dict(
                        config_content, 'sentinel', 'auth-pass',
                        [master_name, kwargs['passwd']])
                if not myid:
                    notification_script = \
                        self._get_config_template('notify.sh')
                    operating_system.chown(notification_script,
                                           system.REDIS_OWNER,
                                           system.REDIS_OWNER,
                                           as_root=True)
                    operating_system.chmod(
                        notification_script,
                        operating_system.FileMode.SET_USR_RWX,
                        as_root=True)
                    guestagent_utils.set_value_from_properties_dict(
                        config_content, 'sentinel', 'notification-script',
                        [master_name, notification_script])
                    failover_script = self._get_config_template('failover.sh')
                    operating_system.chown(failover_script, system.REDIS_OWNER,
                                           system.REDIS_OWNER, as_root=True)
                    operating_system.chmod(
                        failover_script,
                        operating_system.FileMode.SET_USR_RWX,
                        as_root=True)
                    operating_system.chmod(
                        failover_script,
                        operating_system.FileMode.SET_USR_RWX,
                        as_root=True)
                    guestagent_utils.set_value_from_properties_dict(
                        config_content, 'sentinel', 'client-reconfig-script',
                        [master_name, failover_script])
                LOG.debug('before save: %s, %s',
                          type(config_content), config_content)
                self.sentinel_configration_manager.save_configuration(
                    config_content)
                operating_system.copy(
                    system.SENTINEL_CONFIG, "/etc/redis-sentinel.conf.bak",
                    force=True, as_root=True)
                operating_system.enable_service_on_boot(service_candidates)
                operating_system.start_service(service_candidates)
            else:
                operating_system.stop_service(service_candidates)
                operating_system.disable_service_on_boot(service_candidates)
            operating_system.custom_config_ini_set(dict_args=kwargs)
        else:
            LOG.warn("Can't find % service", service_candidates)

    def _get_config_template(self, name):
        dirpath = os.path.dirname(os.path.abspath(__file__))
        template_file = os.path.join(dirpath, 'templates', name)
        return template_file

    def _redis_host_ip(self, device=None, vip=None):
        devs = netifaces.ifaddresses(device)[netifaces.AF_INET]
        for dev in devs:
            if vip not in dev['addr']:
                ip = dev['addr']
        return ip

    def _rename_command(self, old_name, new_name):
        """It is possible to completely disable a command by renaming it
        to an empty string.
        """
        self.configuration_manager.apply_system_override(
            {'rename-command': [old_name, new_name]})

    def get_renamed_commands(self):
        commands = self.configuration_manager.get_value("rename-command")
        return [command for command in commands if command[0] != 'CONFIG']

    def rename_commands(self, commands):
        """
        :param commands: [['command1','new command1'],
                           ['command2','new command2']...]
        :return:
        """
        old_commands = \
            self.configuration_manager.get_value("rename-command")
        for i in range(len(commands)):
            command, new_command = commands[i]
            if new_command is None:
                commands[i][1] = utils.generate_random_password()
            if command.upper() == 'CONFIG':
                raise exception.ConfigurationNotSupported(
                    "CONFIG command disabled by default")
            if command.upper() not in ALLOW_DISABLED_COMMANDS:
                raise exception.ConfigurationNotSupported(
                    "Rename-command %s is not allowed" % command)
        for command, renamed_command in old_commands:
            if command.upper() == 'CONFIG':
                commands.append([command, renamed_command])
        self.configuration_manager.apply_system_override(
            {'rename-command': commands})

    def get_logfile(self):
        """Specify the log file name. Also the empty string can be used to
        force Redis to log on the standard output.
        Note that if you use standard output for logging but daemonize,
        logs will be sent to /dev/null
        """
        return self.get_configuration_property('logfile')

    def get_db_filename(self):
        """The filename where to dump the DB.
        """
        return self.get_configuration_property('dbfilename')

    def get_working_dir(self):
        """The DB will be written inside this directory,
        with the filename specified the 'dbfilename' configuration directive.
        The Append Only File will also be created inside this directory.
        """
        return self.get_configuration_property('dir')

    def get_persistence_filepath(self):
        """Returns the full path to the persistence file."""
        return guestagent_utils.build_file_path(
            self.get_working_dir(), self.get_db_filename())

    def get_port(self):
        """Port for this instance or default if not set."""
        return self.get_configuration_property('port', system.REDIS_PORT)

    def get_auth_password(self):
        """Client authentication password for this instance or None if not set.
        """
        return self.get_configuration_property('requirepass')

    def is_appendonly_enabled(self):
        """True if the Append Only File (AOF) persistence mode is enabled.
        """
        return self.get_configuration_property('appendonly', False)

    def get_append_file_name(self):
        """The name of the append only file (AOF).
        """
        return self.get_configuration_property('appendfilename')

    def is_cluster_enabled(self):
        """Only nodes that are started as cluster nodes can be part of a
        Redis Cluster.
        """
        return self.get_configuration_property('cluster-enabled', False)

    def enable_cluster(self):
        """In order to start a Redis instance as a cluster node enable the
        cluster support
        """
        self.configuration_manager.apply_system_override(
            {'cluster-enabled': 'yes'}, CLUSTER_CFG)

    def get_cluster_config_filename(self):
        """Cluster node configuration file.
        """
        return self.get_configuration_property('cluster-config-file')

    def set_cluster_config_filename(self, name):
        """Make sure that instances running in the same system do not have
        overlapping cluster configuration file names.
        """
        self.configuration_manager.apply_system_override(
            {'cluster-config-file': name}, CLUSTER_CFG)

    def get_cluster_node_timeout(self):
        """Cluster node timeout is the amount of milliseconds a node must be
        unreachable for it to be considered in failure state.
        """
        return self.get_configuration_property('cluster-node-timeout')

    def get_configuration_property(self, name, default=None):
        """Return the value of a Redis configuration property.
        Returns a single value for single-argument properties or
        a list otherwise.
        """
        return utils.unpack_singleton(
            self.configuration_manager.get_value(name, default))

    def cluster_meet(self, ip, port):
        try:
            utils.execute_with_timeout('redis-cli', 'cluster', 'meet',
                                       ip, port)
        except exception.ProcessExecutionError:
            LOG.exception('Error joining node to cluster at %s.', ip)
            raise

    def cluster_addslots(self, first_slot, last_slot):
        try:
            group_size = 200
            # Create list of slots represented in strings
            # eg. ['10', '11', '12', '13']
            slots = list(map(str, range(first_slot, last_slot + 1)))
            while slots:
                cmd = (['redis-cli', 'cluster', 'addslots']
                       + slots[0:group_size])
                out, err = utils.execute_with_timeout(*cmd, run_as_root=True,
                                                      root_helper='sudo')
                if 'OK' not in out:
                    raise RuntimeError(_('Error executing addslots: %s')
                                       % out)
                del slots[0:group_size]
        except exception.ProcessExecutionError:
            LOG.exception('Error adding slots %(first_slot)s-%(last_slot)s'
                          ' to cluster.',
                          {'first_slot': first_slot, 'last_slot': last_slot})
            raise

    def _get_node_info(self):
        try:
            out, _ = utils.execute_with_timeout('redis-cli', '--csv',
                                                'cluster', 'nodes')
            return [line.split(' ') for line in out.splitlines()]
        except exception.ProcessExecutionError:
            LOG.exception('Error getting node info.')
            raise

    def _get_node_details(self):
        for node_details in self._get_node_info():
            if 'myself' in node_details[2]:
                return node_details
        raise exception.TroveError(_("Unable to determine node details"))

    def get_node_ip(self):
        """Returns [ip, port] where both values are strings"""
        return self._get_node_details()[1].split(':')

    def get_node_id_for_removal(self):
        node_details = self._get_node_details()
        node_id = node_details[0]
        my_ip = node_details[1].split(':')[0]
        try:
            slots, _ = utils.execute_with_timeout('redis-cli', '--csv',
                                                  'cluster', 'slots')
            return node_id if my_ip not in slots else None
        except exception.ProcessExecutionError:
            LOG.exception('Error validating node to for removal.')
            raise

    def remove_nodes(self, node_ids):
        try:
            for node_id in node_ids:
                utils.execute_with_timeout('redis-cli', 'cluster',
                                           'forget', node_id)
        except exception.ProcessExecutionError:
            LOG.exception('Error removing node from cluster.')
            raise

    def enable_root(self, password=None):
        if not password:
            password = utils.generate_random_password()
        redis_password = RedisRootUser(password=password)
        try:
            self.configuration_manager.apply_system_override(
                {'requirepass': password, 'masterauth': password},
                change_id=SYS_OVERRIDES_AUTH)
            self.apply_overrides(
                self.admin, {'requirepass': password, 'masterauth': password})
            kwargs = {'enable_auth': "true", 'passwd': password}
            self.upload_redis_sentinel_config(**kwargs)
        except exception.TroveError:
            LOG.exception('Error enabling authentication for instance.')
            raise
        return redis_password.serialize()

    def disable_root(self):
        try:
            self.configuration_manager.remove_system_override(
                change_id=SYS_OVERRIDES_AUTH)
            self.apply_overrides(self.admin,
                                 {'requirepass': '', 'masterauth': ''})
            kwargs = {'enable_auth': "false"}
            self.upload_redis_sentinel_config(**kwargs)
        except exception.TroveError:
            LOG.exception('Error disabling authentication for instance.')
            raise

    @property
    def sentinel_configration_manager(self):
        if self._sentinel_configration_manager is None:
            revision_dir = guestagent_utils.build_file_path(
                os.path.dirname(system.REDIS_CONFIG), "sentinel_overrides")
            config_value_mappings = {'yes': True, 'no': False, "''": None}
            self._sentinel_configration_manager = ConfigurationManager(
                system.SENTINEL_CONFIG,
                system.REDIS_OWNER, system.REDIS_OWNER,
                PropertiesCodec(
                    unpack_singletons=False,
                    string_mappings=config_value_mappings
                ), requires_root=True,
                override_strategy=OneFileOverrideStrategy(revision_dir))
        return self._sentinel_configration_manager

    @staticmethod
    def get_sync_ha_lock():
        return operating_system.lock('sync_ha', lock_file_prefix='trove')

    def get_ha(self, other_keys=[]):
        enabled = operating_system.custom_config_ini_get('enabled')
        if str(operating_system.custom_config_ini_get(
                'enabled')) == 'True':
            LOG.debug("enabled:%s %s", enabled, type(enabled))
            keys = ['enabled', 'vip', 'quorum',
                    'sentinel_enabled', 'number_of_nodes'] + other_keys
            keys_map = {'enabled': bool,
                        'sentinel_enabled': bool,
                        'quorum': int,
                        'number_of_nodes': int}
            return {key: keys_map.get(key, str)(
                operating_system.custom_config_ini_get(key)) for key in keys}
        else:
            return {'enabled': False}

    def set_ha(self, ha_config):
        config = {
            'enabled': True,
            'vip': None,
            'master': None,
            'mangled': None,
            'quorum': 1,
            'port': 6379,
            'passwd': "",
            'enable_auth': "false",
            'sentinel_enabled': True,
            'number_of_nodes': 1
        }
        LOG.debug("set_ha with configuration: %s", ha_config)
        config.update(ha_config)
        config["mangled"] = self.get_config_command_name()
        old_vip = operating_system.custom_config_ini_get(key='vip')
        vip = ha_config.get('vip', 'None')
        config['host'] = self._redis_host_ip(
            device='eth0', vip=vip)
        port = self.get_port()
        config['port'] = port
        password = self.get_configuration_property('requirepass')
        passwd_opt = ""
        if password:
            config['enable_auth'] = "true"
            config['passwd'] = password
            passwd_opt = "-a %s" % password
        if 'master' not in ha_config:
            master = os.popen(
                "redis-cli %s info Replication | grep master_host |"
                " awk -F: '{print $2}'" % passwd_opt).read().strip()
            config['master'] = master if master else config['host']
        operating_system.custom_config_ini_set(config)
        try:
            replication_info = self.admin.get_info("Replication")
            devs = netifaces.gateways()[netifaces.AF_INET]
            dev = list(devs[0])[1]
            LOG.debug("GOT replication info: %s", replication_info)
            LOG.debug("GOT devs: %s", devs)
            if old_vip and old_vip != vip:
                LOG.debug("GOT old_vip: %s, vip: %s", old_vip, vip)
                operating_system.remove_vip(vip, dev)
            if replication_info.get('role') == 'master':
                operating_system.add_vip(vip, dev)
        except exception.ProcessExecutionError as e:
            LOG.debug("GOT EORRR: %s", e)
        self.upload_redis_sentinel_config(**config)

    def delete_ha(self):
        config = {
            'enabled': False,
            'vip': None,
            'master': None,
            'mangled': None,
            'quorum': 1,
            'port': 6379,
            'sentinel_enabled': False,
            'number_of_nodes': 1
        }
        LOG.debug("delete_ha")
        service_candidates = ['redis-sentinel']
        if 'type' in operating_system.service_discovery(service_candidates):
            operating_system.stop_service(service_candidates)
            operating_system.disable_service_on_boot(service_candidates)
        else:
            LOG.warn("Not found service %s", service_candidates)
        devs = netifaces.gateways()[netifaces.AF_INET]
        vip = operating_system.custom_config_ini_get(key='vip')
        dev = list(devs[0])[1]
        operating_system.custom_config_ini_set(config)
        try:
            operating_system.remove_vip(vip, dev)
        except BaseException as e:
            LOG.debug("GOT ERROR: %s", e)

    def sentinel_command(self, command):
        cmd = "redis-cli -p 26379"
        passwd = operating_system.custom_config_ini_get(key='passwd')
        if passwd:
            cmd = cmd + " -a " + passwd
        cmd = cmd + " " + command
        ret = os.popen(cmd).read()
        LOG.debug("exec cmd: %s \noutput: %s", cmd, ret)
        return ret


class RedisAdmin(object):
    """Handles administrative tasks on the Redis database.
    """

    DEFAULT_CONFIG_CMD = 'CONFIG'

    def __init__(self, password=None, unix_socket_path=None, config_cmd=None):
        self.__client = redis.StrictRedis(password=password)
        self.__config_cmd_name = config_cmd or self.DEFAULT_CONFIG_CMD

    def set_config_command_name(self, name):
        """Set name of the 'CONFIG' command or None for default.
        """
        self.__config_cmd_name = name or self.DEFAULT_CONFIG_CMD

    def ping(self):
        """Ping the Redis server and return True if a response is received.
        """
        return self.__client.ping()

    def info(self, section=None):
        """Get the Redis server info and return results if a response is received.
        """
        return self.__client.info(section=section)

    def get_info(self, section=None):
        return self.__client.info(section=section)

    def persist_data(self):
        save_cmd = 'SAVE'
        last_save = self.__client.lastsave()
        LOG.debug("Starting Redis data persist")
        save_ok = True
        try:
            save_ok = self.__client.bgsave()
        except redis.exceptions.ResponseError as re:
            # If an auto-save is in progress just use it, since it must have
            # just happened
            if "Background save already in progress" in str(re):
                LOG.info("Waiting for existing background save to finish")
            else:
                raise
        if save_ok:
            save_cmd = 'BGSAVE'

            def _timestamp_changed():
                return last_save != self.__client.lastsave()

            try:
                utils.poll_until(_timestamp_changed, sleep_time=2,
                                 time_out=TIME_OUT)
            except exception.PollTimeOut:
                raise RuntimeError(_("Timeout occurred waiting for Redis "
                                   "persist (%s) to complete.") % save_cmd)

        # If the background save fails for any reason, try doing a foreground
        # one.  This blocks client connections, so we don't want it to be
        # the default.
        elif not self.__client.save():
            raise exception.BackupCreationError(_("Could not persist "
                                                "Redis data (%s)") % save_cmd)
        LOG.debug("Redis data persist (%s) completed", save_cmd)

    def set_master(self, host=None, port=None):
        LOG.debug("SLAVEOF %s %s" % (host, port))
        self.__client.slaveof(host, port)

    def config_set(self, name, value):
        response = self.execute(
            '%s %s' % (self.__config_cmd_name, 'SET'), name, value)
        if not self._is_ok_response(response):
            raise exception.UnprocessableEntity(
                _("Could not set configuration property '%(name)s' to "
                  "'%(value)s'.") % {'name': name, 'value': value})

    def _is_ok_response(self, response):
        """Return True if a given Redis response is 'OK'.
        """
        return response and redis.client.bool_ok(response)

    def execute(self, cmd_name, *cmd_args, **options):
        """Execute a command and return a parsed response.
        """
        try:
            return self.__client.execute_command(cmd_name, *cmd_args,
                                                 **options)
        except Exception as e:
            LOG.exception(e)
            raise exception.TroveError(
                _("Redis command '%(cmd_name)s %(cmd_args)s' failed.")
                % {'cmd_name': cmd_name, 'cmd_args': ' '.join(cmd_args)})

    def wait_until(self, key, wait_value, section=None, timeout=None):
        """Polls redis until the specified 'key' changes to 'wait_value'."""
        timeout = timeout or CONF.usage_timeout
        LOG.debug("Waiting for Redis '%(key)s' to be: %(value)s.",
                  {'key': key, 'value': wait_value})

        def _check_info():
            redis_info = self.get_info(section)
            if key in redis_info:
                current_value = redis_info[key]
                LOG.debug("Found '%(value)s' for field %(key)s.",
                          {'value': current_value, 'key': key})
            else:
                LOG.error('Output from Redis command: %s', redis_info)
                raise RuntimeError(_("Field %(field)s not found "
                                     "(Section: '%(sec)s').") %
                                   ({'field': key, 'sec': section}))
            return current_value == wait_value

        try:
            utils.poll_until(_check_info, time_out=timeout)
        except exception.PollTimeOut:
            raise RuntimeError(_("Timeout occurred waiting for Redis field "
                                 "'%(field)s' to change to '%(val)s'.") %
                               {'field': key, 'val': wait_value})
