# Copyright 2011 OpenStack Foundation
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

from trove.common import extensions
from trove.extensions.redis import service as reids_service


class Redis(extensions.ExtensionDescriptor):

    def get_name(self):
        return "Redis"

    def get_description(self):
        return "Redis"

    def get_alias(self):
        return "REDIS"

    def get_namespace(self):
        return "http://TBD"

    def get_updated(self):
        return "2020-12-21T13:25:27-06:00"

    def get_resources(self):
        resources = []

        resource = extensions.ResourceExtension(
            'renamed_commands',
            reids_service.RedisCommandController(),
            parent={'member_name': 'instance',
                    'collection_name': '{tenant_id}/instances'})
        resources.append(resource)
        return resources
