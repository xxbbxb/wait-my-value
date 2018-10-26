## MySQL value waiter

### Usage sample

#### Will wait for Seconds_Behind_Master become 0 or Ctrl+C

```
wait-my-value -credential ~/.my.cnf -query "show slave status" -value "0" -field "Seconds_Behind_Master" -source "tcp(remote_addr:3306)/"
```
