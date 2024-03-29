package rdx 

import (
    "fmt"
    "errors"
)

%%{

machine REPL;
include RDX "rdx_parser.rl";

write data;

action cmd { cmd = string(data[mark[0]:p]); }
action dot {
    rdx.Parent = path
    path.Nested = append(path.Nested, *rdx)
    rdx = &RDX{}
}
action path { 
    rdx.Parent = path
    path.Nested = append(path.Nested, *rdx)
    rdx = &RDX{}
}

PATH = token ("." %dot token)* ; 
ws = space;

command = "new" | "quit" | "exit";

main :=
    ws*
    command >b %cmd
    (ws+ PATH %path)? 
    (ws+ RDX)? 
    ws*;

}%%

func ParseREPL(data []byte) (cmd string, path *RDX, rdx *RDX, err error) {

    var mark [RdxMaxNesting]int
    nest, cs, p, pe, eof := 0, 0, 0, len(data), len(data)

    rdx = &RDX{}
    path = &RDX{RdxType:RdxPath}

    %%write init;
    %%write exec;

    if cs < REPL_first_final { 
        err = errors.New(fmt.Sprintf("command parsing failed at pos %d", p))
    }

    return

}
