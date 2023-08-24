# Overview

To use Kicksaw's integration app, install [this package](https://github.com/Kicksaw-Consulting/integration-app-sfdx) in your Salesforce organization.
To do that, navigate to a url `https://<instance>.com/packaging/installPackage.apexp?p0=<package-version-id>`.

Once your org is ready to-go, instantiate the `KicksawSalesforce` class and operate like normal, but note:

- You need to pass the AWS Step Function payload to the class when instantiating
- Instantiating the client creates an execution object in Salesforce, unless you pass it the id of an already existing execution object
- All of your bulk operations will have their errors parsed and error objects created in Salesforce if applicable

```python
from kicksaw_integration_app_client import KicksawSalesforce

step_function_payload = {}
salesforce = KicksawSalesforce(connection_object, integration_name, step_function_payload)

salesforce.bulk.Account.upsert()
```

For code examples, please refer to `tests/test_integrations.py`.
