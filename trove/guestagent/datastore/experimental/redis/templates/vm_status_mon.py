#!/usr/bin/python
#
# Copyright 2021 Yovole Networks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_log import log as logging
import sys
from trove.common import cfg
from trove import rpc
from trove.taskmanager.models import NotifyMixin

config_file = "/etc/trove/conf.d/trove-guestagent.conf"
guest_info = "/etc/trove/conf.d/guest_info.conf"

cfg.CONF(
    args=[],
    project='trove',
    default_config_files=[config_file, guest_info]
)

CONF = cfg.CONF

logging.setup(CONF, None)
LOG = logging.getLogger(__name__)


class VM_POWER_STATUS(object):

    def __init__(self):
        self.event_type = 'powerstatus'
        self.topic = CONF.taskmanager_queue
        self.info = NotifyMixin()
        rpc.init(cfg.CONF)

    def sent_notify(self, status=None, host=None):
        kwargs = {
            "powerstatus": status,
            "failed_host": host
        }
        self.info.send_update_event(self.event_type, CONF.guest_id, **kwargs)
        LOG.debug("Successfully cast update power status.")

if __name__ == "__main__":
    power_status = VM_POWER_STATUS()
    if len(sys.argv) == 3:
        power_status.sent_notify(
            status=sys.argv[1],
            host=sys.argv[2]
        )
