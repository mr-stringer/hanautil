# hanautil

A helper utility for the HANA database written in golang.

hanautil aims to provide helper functions for the monitoring and maintenance
of the SAP HANA databases.

The library provides an abstraction from SQL, meaning that, as a user, you
don't need to understand the SQL required to perform the tasks, preventing
users creating SQL statements that don't behave as intended. However,
some of the functions contained in this library are destructive. Destructive
functions are all documented appropriately and should be used with extreme
caution.
