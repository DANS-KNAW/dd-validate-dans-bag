Configuration
=============

This module can be configured by editing the configuration file. This file is installed in `/etc/opt/dans.knaw.nl/dd-validate-dans-bag/config.yml` when using the RPM.
The settings are explained with comments in the file itself. An on-line version of the latest configuration file can be found
[here](https://github.com/DANS-KNAW/dd-validate-dans-bag/blob/master/src/main/assembly/dist/cfg/config.yml){:target=_blank}.

The backend configuration selects the ruleset:

* `dataverse` configured: Data Station rules.
* `vaultCatalog` configured: Vault as a Service rules.
* Neither configured: **LegacyVaas**, based on the [DANS BagIt Profile 0.0.0](https://dans-knaw.github.io/dans-bagit-profile/versions/0.0.0/).
  A missing section, a null value, and an empty object (`{}`) all count as unconfigured.

Only one backend may be configured. LegacyVaas excludes all AIP-only rules and sections 3.3, 3.4, and 4 of the legacy profile.
