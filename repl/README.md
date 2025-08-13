# Chotki REPL

Command vocabulary

- Open or close a replica or a snapshot at a given path. 01. `open`, e.g. `open DecSys.db` 02. `shut` 03. `quit`, `exit`
- Sync with another replica over the network 01. `pull`, e.g. `pull replicated.store`,
  `pull replicated.store for Student` -- import changes
  from a remote replica 02. `push` -- send changes to a remote replica 03. `poke` -- handshake with a remote replica to see their
  progress; output the version vector difference 04. `talk` -- start two-way real-time sync 05. `kick` -- disconnect a replica 06. `mute` -- stop listening to a replica, keep sending
- Open, create or delete a snapshot 01. `view` 02. `fork` 03. `drop`
- Object/field direct manipulation 01. `cat`, e.g. `cat petr`, `cat Student live` -- print
  the object(s) out, once or repeatedly/live 02. `let` 03. `new`, e.g. `new student` 04. `set`, e.g. `set petr name="Pyotr" mark=9` -- object
  LWW field change 05. `add`, e.g. `add petr pullreqs+1` -- object counter
  field change 06. `inc`, `exc` e.g. `add passed petr` -- object set
  field change 07. `ins`, `rem` e.g. `ins tasks 03-git 04-formats` --
  object array field change
- Version vectors, closures 01. `join`, `cone`/`\/`, e.g. `join STABLE 2e-804f2`, `\/
tip` -- merge version vectors (simple or using closures) 02. `sect`, `divv`/`X` -- version vector difference,
  either simple or using closures 03. `both`, `over`/`/\` -- version vector intersection,
  either simple or using closures
- JSON data import/export 01. `json`, e.g. `json 2e-804d2`, `json petr`,
  `json Student` -- prints out JSON for the object(s) 02. `save`, e.g. `save petr petr.json` -- save JSON 03. `load`, e.g. `load petr petr.json` -- load JSON
- Dealing with the packet log 01. `tail`, e.g. `tail A` -- print the log to the console
- Database queries 01. `grab`, e.g. `grab Student name="Ivan" score>8`,
  filter objects by their class and fields

The object graph path convention

### The REPL alias system

Each command returns `[]id64` on completion. That might be a
version vector, a list of object ids, single object or field id,
etc. That value will get an alias if the command was prepended
with `name=`, e.g. `STABLE=cone 1e-402`. Later, that alias can
be used in place of an argument in any command. Aliases are
stored, so they survive restarts. But they are not replicated to
other hosts unless through snaphot duplication.

Convention: object aliases are `lowercase`, class aliases are
`CamelCase` and version vector aliases are `UPPERCASE`.

Read-Evaluate-Loop for Chotki. Manage databases, network,
objects. Debug and troubleshoot.

All commands have the same syntax: `verb {rdx args}`.
RDX arguments can take different forms (string, maps,
arrays, ids, etc) depending on the verb. Each command
returns an id and/or an error message.

## Replica lifecycle

1.  `create b0b-0` create a replica with that id
2.  `open b0b-0` open a replica
3.  `close` close the replica
4.  `exit`

## Networking

1. `listen "localhost:1234"` tcp listen a port
2. `connect "remote:1234"` tcp connect

## Backup/restore

## Http/swagger

1. `servehttp 8001`
   serve http server on defined port with handlers to manipulate opened chotki instance
2. `swagger`
   serve swagger on http://127.0.0.1:8000/, you can select ports in the top to work with different servers

## Classes and objects

1.  `class {_ref:Parent,Name:S,Girl:T}`
    create a (sub)class with the given field types
2.  `name {Child:b0b-9}`
    set a global-scope name for an object/type/field
3.  `new {_ref:Child,Name:"Alice"}`
    create a new object of the specified class (Child)
4.  `edit {_ref:b0b-a,Girl:true}`
    edit the specified object
5.  `cat b0b-a` shallow-print the object in JSON-like RDX
6.  `list b0b-a` deep-print the object in JSON-like RDX

## Debugging

1.  `dump all, dump objects, dump vv`
    dump the raw key-value store
2.  `tell b0b-a`
    notify of any changes to the object/field
3.  `mute b0b-a` stop notifying on that
4.  `pinc b0b-f-2`, `ponc b0b-f-2`
    ping/pong increments to the counter (normally,
    commands are entered at different replicas)
5.  `sinc {fid: b0b-f-2, ms: 100, count: 1000000}`
    blind-increment the field at that interval that many times
