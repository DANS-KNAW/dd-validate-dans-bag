Configuration
=============

This module can be configured by editing the configuration file. This file is installed in `/etc/opt/dans.knaw.nl/dd-validate-dans-bag/config.yml` when using the RPM.
The settings are explained with comments in the file itself. An on-line version of the latest configuration file can be found
[here](https://github.com/DANS-KNAW/dd-validate-dans-bag/blob/master/src/main/assembly/dist/cfg/config.yml){:target=_blank}.

The optional top-level boolean `easyLegacyMode` defaults to `false`. The configuration selects the ruleset:

* `dataverse` configured: Data Station rules.
* `vaultCatalog` configured: Vault as a Service rules.
* `easyLegacyMode: true`: **LegacyVaas**, based on the [DANS BagIt Profile 0.0.0](https://dans-knaw.github.io/dans-bagit-profile/versions/0.0.0/).
  Both `dataverse` and `vaultCatalog` are ignored, including their backend validation and client initialization.

When `easyLegacyMode` is absent or `false`, exactly one backend must be configured.
A missing section, a null value, and an empty object (`{}`) all count as unconfigured.
LegacyVaas excludes all AIP-only rules and sections 3.3, 3.4, and 4 of the legacy profile.
