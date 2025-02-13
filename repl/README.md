#   Chotki REPL

Read-Evaluate-Loop for Chotki. Manage databases, network,
objects. Debug and troubleshoot.

All commands have the same syntax: `verb {rdx args}`.
RDX arguments can take different forms (string, maps,
arrays, ids, etc) depending on the verb. Each command
returns an id and/or an error message.

##  Replica lifecycle

 1. `create b0b-0` create a replica with that id
 2. `open b0b-0` open a replica
 3. `close` close the replica
 4. `exit`

##  Networking

  1. `listen "localhost:1234"` tcp listen a port
  2. `connect "remote:1234"` tcp connect

##  Backup/restore

##  Http/swagger
1. `servehttp 8001`
   serve http server on defined port with handlers to manipulate opened chotki instance 
2. `swagger`
   serve swagger on http://127.0.0.1:8000/, you can select ports in the top to work with different servers

##  Classes and objects

 1. `class {_ref:Parent,Name:S,Girl:T}`
     create a (sub)class with the given field types
 2. `name {Child:b0b-9}`
    set a global-scope name for an object/type/field
 3. `new {_ref:Child,Name:"Alice"}`
    create a new object of the specified class (Child)
 4. `edit {_ref:b0b-a,Girl:true}`
    edit the specified object
 5. `cat b0b-a` shallow-print the object in JSON-like RDX
 6. `list b0b-a` deep-print the object in JSON-like RDX

##  Debugging

 1. `dump all, dump objects, dump vv`
    dump the raw key-value store
 2. `tell b0b-a`
    notify of any changes to the object/field
 3. `mute b0b-a` stop notifying on that
 4. `pinc b0b-f-2`, `ponc b0b-f-2`
    ping/pong increments to the counter (normally,
    commands are entered at different replicas)
 5. `sinc {fid: b0b-f-2, ms: 100, count: 1000000}`
    blind-increment the field at that interval that many times
