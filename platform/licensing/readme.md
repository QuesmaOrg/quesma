Quesma Licensing Module
=======================

The diagram below illustrates the interaction between the Quesma Licensing Module and the License Server.

```mermaid
sequenceDiagram
    box  Quesma Instance
participant Q as Quesma<br>(main process)
participant License as License Module
end
    box  License Server
participant Server as License Server
end 
Q->>License: Boot up
  alt License Key not found in configuration
    License-->>License: Generate `Installation ID`
    License-->>Server: Register `Installation ID`
    Server-->>License: Obtain License Key
   activate License
   License -->> Q: (maybe panic)
   deactivate License
end
   License->>Server: Present License Key
  activate Server
    Server-->>Server: Verify License Key
  Server->>License: Return License <br>(granted permissions, expiration date)
  deactivate Server
   activate License
   License -->> Q: (maybe panic)
   deactivate License
License->>License: Validate configuration
   License -->> Q: (maybe panic)
   activate License
deactivate License
License->>License: Trigger runtime checks<br>(Connectors/Processors)
   activate License
   License -->> Q: (maybe panic)
   deactivate License
License->>Q: Done veryfing.
```

## Key assumptions 

* Unless provided explicitly, License Module is going to generate unique `Installation ID` in the form of UUID.
* Aforementioned `Installation ID` is going to be used to identify the Quesma installation, so ideally it has to persist between restarts. \
  We are going to attempt writing it to a file, next to the configuration. This will cover local build use case. 
  If we fail writing it, we are going to regenerate it on each boot (this probably implies cloud deployment situation). 
* License Module is going to use `Installation ID` to **obtain the License Key** from the License Server (unless the License Key is **not** specified in the configuration)
* Quesma License is going to be signed by us and will contain expiration date.
* License Module is going to contact license server and ask for what it's eligible to based on the License Key.
* Based on the that information License Module is going to validate the configuration.
* License Module is going to trigger local checks (usage of allowed processors/connectors).
  Those checks are going to be part of respective components.
