# mysql_copy_tablespaces

## Prerequisites

- Make sure the hosts you run `mysql_copy_tablespaces.py` from can SSH using keys to both source and destination.
- Similarly, from the source, the SSH account should be able to connect to the destination using SSH keys.
- If you are not using `root` SSH account (recommended!), make sure you can also execute `sudo`.