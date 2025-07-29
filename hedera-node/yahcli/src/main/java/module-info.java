// SPDX-License-Identifier: Apache-2.0
module com.hedera.node.yahcli {
    exports com.hedera.services.yahcli;

    opens com.hedera.services.yahcli to
            info.picocli;
    opens com.hedera.services.yahcli.commands.keys to
            info.picocli;
    opens com.hedera.services.yahcli.commands.accounts to
            info.picocli;
    opens com.hedera.services.yahcli.commands.fees to
            info.picocli;
    opens com.hedera.services.yahcli.commands.files to
            info.picocli;
    opens com.hedera.services.yahcli.commands.system to
            info.picocli;
    opens com.hedera.services.yahcli.commands.schedules to
            info.picocli;
    opens com.hedera.services.yahcli.commands.nodes to
            info.picocli;

    exports com.hedera.services.yahcli.config.domain;
    exports com.hedera.services.yahcli.config;

    requires com.hedera.node.app.hapi.utils;
    requires com.hedera.node.app.service.addressbook;
}
