dd-validate-dans-bag
====================

Validates whether a given bag complies with the DANS BagIt Profile v1

Purpose
-------
This module checks whether a given BagIt bag complies with the [DANS BagIt Profile v1]{:target=_blank}.

Interfaces
----------

This service has the following interfaces:

![](img/overview.png){:width=50%}

### Provided interfaces

#### API

* _Protocol type_: HTTP
* _Internal or external_: **internal**
* _Purpose_: to receive commands to validate DANS bags and return the results of the validation

#### Deposit directories

* _Protocol type_: Shared filesystem
* _Internal or external_: **internal**
* _Purpose_: to receive bags to be validated

Processing
----------

The API accepts requests to validate a bag located in a deposit directory. The bag is validated against the DANS BagIt Profile v1. The results of the validation
are returned in the API response. Note, that this means the processing is synchronous. For large bags this may take a considerable amount of time.

It is also possible to send the bag itself as a ZIP file in a `POST` request to the API. This is intended as a service for client developers, so that they can
verify whether the bags they create comply with the DANS BagIt Profile v1. This interface is not exposed in the production environment.

For details about the API, see the [API documentation](./to-api.md).

[DANS BagIt Profile v1]: {{ dans_bagit_profile_url }}