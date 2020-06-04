This is an example project that shows how to change the `processInstanceId` of process instances in the byte-arrays of the jBPM database.

**DISCLAIMER: This was the output of a coding challenge to see if this can be technically done. This code should NOT be used on production data as it can corrupt your database. I can not, and will not, provide any support on this code, nor will I fix any issue that you encounter in this code.**

Also, this code is not complete. It only allows to alter the `processInstanceId` in 2 places, in the `ProcessInstance` and the `WorkItem` instance. The id is however stored in multiple places, so this code currently only works for simple use-cases.

This project targets a PostgreSQL 10.x database.