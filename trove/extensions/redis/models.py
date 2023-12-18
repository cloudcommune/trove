# Copyright 2017 Eayun, Inc.
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

from trove.common.clients import create_guest_client
from trove.extensions.common.models import load_and_verify
from trove.extensions.common.models import Root


class RedisRoot(Root):
    @classmethod
    def get_auth_password(cls, context, instance_id):
        load_and_verify(context, instance_id)
        password = create_guest_client(context,
                                       instance_id).get_root_password()
        return password


class RedisGuestAgent(object):

    @classmethod
    def get_renamed_commands(cls, context, instance_id):
        load_and_verify(context, instance_id)
        return dict(create_guest_client(context,
                                        instance_id).get_renamed_commands())

    @classmethod
    def rename_commands(cls, context, instance_id, commands,
                        cluster_instances_list=[]):
        commands = [[k, v] for k, v in commands.items()]
        load_and_verify(context, instance_id)
        create_guest_client(context, instance_id).rename_commands(
            commands)
        for instance in cluster_instances_list:
            create_guest_client(context, instance).rename_commands(
                commands)
