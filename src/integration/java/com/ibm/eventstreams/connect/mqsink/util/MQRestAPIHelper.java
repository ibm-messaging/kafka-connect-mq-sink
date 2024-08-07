/**
 * Copyright 2022, 2023, 2024 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsink.util;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class MQRestAPIHelper {
    private String qmgrname;
    private int portnum;
    private String password;

    public MQRestAPIHelper(final String qmgrname, final int portnum, final String password) {
        this.qmgrname = qmgrname;
        this.portnum = portnum;
        this.password = password;
    }

    public final Logger log = LoggerFactory.getLogger(MQRestAPIHelper.class);

    public static final String STOP_CHANNEL = "{"
            + "  \"type\": \"runCommand\","
            + "  \"parameters\": {"
            + "    \"command\": \"STOP CHANNEL('DEV.APP.SVRCONN') MODE(QUIESCE)\""
            + "  }"
            + "}";

    public static final String START_CHANNEL = "{"
            + "  \"type\": \"runCommand\","
            + "  \"parameters\": {"
            + "    \"command\": \"START CHANNEL('DEV.APP.SVRCONN')\""
            + "  }"
            + "}";

    public int sendCommand(final String request) throws IOException, KeyManagementException, NoSuchAlgorithmException {
        try {

            final String url = "https://localhost:" + portnum + "/ibmmq/rest/v2/admin/action/qmgr/" + qmgrname
                    + "/mqsc";
            final JSONObject commandResult = JsonRestApi.jsonPost(url, "admin", password, request);

            log.debug("result = " + commandResult);

            final int completionCode = commandResult.getInt("overallCompletionCode");
            final int reasonCode = commandResult.getInt("overallReasonCode");

            if (completionCode == 2 && reasonCode == 3008) {
                return 0;
            } else if (completionCode == 0 && reasonCode == 0) {
                return commandResult.getJSONArray("commandResponse").length();
            } else {
                return -1;
            }
        } catch (final JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static class MQRestAPIHelperBuilder {
        private String qmgrname;
        private int portnum;
        private String password;

        public MQRestAPIHelperBuilder qmgrname(final String qmgrname) {
            this.qmgrname = qmgrname;
            return this;
        }

        public MQRestAPIHelperBuilder portnum(final int portnum) {
            this.portnum = portnum;
            return this;
        }

        public MQRestAPIHelperBuilder password(final String password) {
            this.password = password;
            return this;
        }

        public MQRestAPIHelper build() {
            return new MQRestAPIHelper(qmgrname, portnum, password);
        }

    }
}
