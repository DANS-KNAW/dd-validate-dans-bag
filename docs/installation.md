Installation
============

Currently, this project is built as an RPM package for RHEL8 and later. The RPM will install the binaries to `/opt/dans.knaw.nl/dd-validate-dans-bag` and the
configuration files to `/etc/opt/dans.knaw.nl/dd-validate-dans-bag`.

Building from source
--------------------

Prerequisites:

* Java 21 or higher
* Maven 3.8.7 or higher
* RPM

Steps:

    git clone https://github.com/DANS-KNAW/dd-validate-dans-bag.git
    cd dd-validate-dans-bag 
    mvn clean install
