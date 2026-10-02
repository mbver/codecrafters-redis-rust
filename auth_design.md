- the auth coming before handling of command. it can reject to go further
- the client has a field called authenticated. it is set if authentication is true
- the client might have a log-in name. initially, it is default because there's no password, any client can access it
- after the password is set for default, other clients without the right password can't access
- how about storing password for other users too? if we set passwords for other users, default will be unaccessible for unauth users? ==> maybe yes
- the auth initial state

- we will have a map for username-password
  - default is put on the same map
  - initially, when we create the client we will check
    - is default having pwd? -> no -> authenticated
      - yes -> need authentication
  - AUTH command is used for authentication
    - AUTH command is handled separately
    - we check if username and password matches
    - if they match, set authenticated on client

- Auth should be protected with Arc<RwLock>, the hashmap is protected with RwLock
