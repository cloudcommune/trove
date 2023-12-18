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

from trove.common.clients import create_guest_client
from trove.extensions.common.models import load_and_verify


class HighAvailabilityAgent(object):

    @classmethod
    def get_ha(cls, context, instance_id):
        load_and_verify(context, instance_id)
        return dict(create_guest_client(context, instance_id).get_ha())

    @classmethod
    def set_ha(cls, context, instance_id, ha_config,
               cluster_instances_list=[]):
        load_and_verify(context, instance_id)
        create_guest_client(context, instance_id).set_ha(ha_config)
        for instance in cluster_instances_list:
            create_guest_client(context, instance).set_ha(ha_config)

    @classmethod
    def delete_ha(cls, context, instance_id, cluster_instances_list=[]):
        load_and_verify(context, instance_id)
        create_guest_client(context, instance_id).delete_ha()
        for instance in cluster_instances_list:
            create_guest_client(context, instance).delete_ha()
